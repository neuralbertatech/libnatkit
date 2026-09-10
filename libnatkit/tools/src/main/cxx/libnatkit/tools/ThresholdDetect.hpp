#pragma once

// The DATA -> MARKER lane crossing (TEC-NATKIT-109).
//
// Watch one channel; when it crosses a level and stays past it for a dwell time,
// emit a marker. That is muscle-onset detection expressed as a wiring decision
// rather than as a bespoke transform, and it is what lets the graph REACT to an
// onset while a participant is still in the chair instead of discovering it in
// analysis afterwards.
//
// This header is deliberately transport-free: it takes one channel's samples
// plus their time base and returns crossings. ThresholdWorker owns the I/O and
// the MarkerEventV1 construction. Same split as CombineJoin.hpp, for the same
// reason — the interesting logic is decidable from numbers and timestamps, so it
// is testable without a broker, a descriptor or a thread.
//
// ⚠️ TIME COMES FROM device_ts_us AND THE DECLARED SAMPLE RATE, NEVER THE WALL
// CLOCK (docs/STREAM_CONTRACT.md clause 4). The dwell and refractory timers are
// in DATA time, so a recording replayed at 10x produces the identical marker
// set. A timer on the wall clock would pass every real-time test and fail that
// one.

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace nat::tools {

enum class ThresholdDirection {
    Rising,   // v[i-1] < level <= v[i]
    Falling,  // v[i-1] > level >= v[i]
    Either,
};

inline const char* thresholdDirectionName(ThresholdDirection direction)
{
    switch (direction) {
    case ThresholdDirection::Rising:
        return "rising";
    case ThresholdDirection::Falling:
        return "falling";
    case ThresholdDirection::Either:
        return "either";
    }
    return "rising";
}

inline std::optional<ThresholdDirection> parseThresholdDirection(
    const std::string& name)
{
    if (name == "rising") return ThresholdDirection::Rising;
    if (name == "falling") return ThresholdDirection::Falling;
    if (name == "either") return ThresholdDirection::Either;
    return std::nullopt;
}

struct ThresholdConfig {
    double level = 0.0;
    ThresholdDirection direction = ThresholdDirection::Rising;

    // How long the signal must STAY past the level before the crossing counts.
    // Rejects a single noisy sample without rejecting a real onset, because the
    // emitted marker is stamped at the crossing, not at the confirmation.
    uint64_t dwellUs = 0;

    // After a crossing fires, suppress further crossings for this long. This is
    // TEC-NATKIT-105's `debounce` folded in: an envelope hovering at the level
    // would otherwise emit hundreds of markers a second, and requiring a second
    // node to make the common case usable is the wrong default.
    uint64_t refractoryUs = 0;

    // Which channel of the incoming frame to watch.
    size_t channelIndex = 0;
};

struct ThresholdCrossing {
    // ⚠️ THE INTERPOLATED CROSSING TIME, not the frame's timestamp and not the
    // confirming sample's. A frame carries many samples; reporting the frame's
    // device_ts_us quantises every onset to the frame cadence, which at a 100 Hz
    // frame rate is a 10 ms error on a measurement whose whole purpose is
    // timing.
    uint64_t atUs = 0;
    // The level crossed and the sample value that carried it past — recorded so
    // a marker can say how hard the crossing was, not just that it happened.
    double level = 0.0;
    double value = 0.0;
    bool rising = true;
};

class ThresholdDetector {
public:
    explicit ThresholdDetector(ThresholdConfig config) : config_(config) {}

    // Feed one frame's worth of ONE channel. `frame_ts_us` is the time of sample
    // 0 and `sample_rate_hz` gives the spacing, which is the convention the
    // windowing transforms in StreamViewerWebSocket.cpp already use.
    std::vector<ThresholdCrossing> push(
        uint64_t frame_ts_us,
        uint32_t sample_rate_hz,
        const std::vector<float>& samples)
    {
        std::vector<ThresholdCrossing> crossings;
        if (samples.empty()) {
            return crossings;
        }
        const double period_us = sample_rate_hz > 0
            ? 1'000'000.0 / static_cast<double>(sample_rate_hz)
            : 0.0;

        for (size_t index = 0; index < samples.size(); ++index) {
            const double value = static_cast<double>(samples[index]);
            const uint64_t ts = frame_ts_us +
                static_cast<uint64_t>(
                    std::llround(period_us * static_cast<double>(index)));

            // A pending candidate is resolved before a new one is considered:
            // either it has held past the level long enough to fire, or it has
            // fallen back and was noise.
            if (pending_.has_value()) {
                if (stillPast(value, pending_->rising)) {
                    if (ts - pending_->atUs >= config_.dwellUs) {
                        crossings.push_back(*pending_);
                        refractoryUntilUs_ =
                            pending_->atUs + config_.refractoryUs;
                        pending_.reset();
                    }
                } else {
                    pending_.reset();
                    ++candidatesAbandoned_;
                }
            } else if (lastValue_.has_value()) {
                const auto crossing =
                    detectCrossing(lastValue_.value(), lastTsUs_, value, ts);
                if (crossing.has_value()) {
                    if (crossing->atUs < refractoryUntilUs_) {
                        ++suppressedByRefractory_;
                    } else {
                        pending_ = crossing;
                        // Dwell of zero means "fire on the crossing": resolve it
                        // now rather than waiting for the next sample, or a
                        // single-sample frame would never fire at all.
                        if (config_.dwellUs == 0) {
                            crossings.push_back(*pending_);
                            refractoryUntilUs_ =
                                pending_->atUs + config_.refractoryUs;
                            pending_.reset();
                        }
                    }
                }
            }

            lastValue_ = value;
            lastTsUs_ = ts;
        }
        return crossings;
    }

    // Crossings rejected because they landed inside a refractory window. Worth
    // reporting: a rising count means the level is set below the signal's noise
    // floor, which is the difference between a working detector and one that is
    // merely quiet.
    uint64_t suppressedByRefractory() const { return suppressedByRefractory_; }
    // Candidates that crossed but fell back before the dwell elapsed.
    uint64_t candidatesAbandoned() const { return candidatesAbandoned_; }

private:
    bool stillPast(double value, bool rising) const
    {
        return rising ? value >= config_.level : value <= config_.level;
    }

    bool wants(bool rising) const
    {
        switch (config_.direction) {
        case ThresholdDirection::Rising:
            return rising;
        case ThresholdDirection::Falling:
            return !rising;
        case ThresholdDirection::Either:
            return true;
        }
        return false;
    }

    std::optional<ThresholdCrossing> detectCrossing(
        double previous, uint64_t previous_ts, double current, uint64_t ts) const
    {
        const bool rising = previous < config_.level && current >= config_.level;
        const bool falling = previous > config_.level && current <= config_.level;
        if (!rising && !falling) {
            return std::nullopt;
        }
        if (!wants(rising)) {
            return std::nullopt;
        }

        ThresholdCrossing crossing;
        crossing.level = config_.level;
        crossing.value = current;
        crossing.rising = rising;
        // Linear interpolation between the bracketing samples. The crossing did
        // not happen AT a sample — it happened between two, and the sample grid
        // is the limit of our resolution, not of our arithmetic.
        const double span = current - previous;
        double fraction = 0.0;
        if (span != 0.0) {
            fraction = (config_.level - previous) / span;
        }
        if (fraction < 0.0) fraction = 0.0;
        if (fraction > 1.0) fraction = 1.0;
        const double interval =
            static_cast<double>(ts) - static_cast<double>(previous_ts);
        crossing.atUs = previous_ts +
            static_cast<uint64_t>(std::llround(fraction * interval));
        return crossing;
    }

    ThresholdConfig config_{};
    std::optional<double> lastValue_{};
    uint64_t lastTsUs_ = 0;
    std::optional<ThresholdCrossing> pending_{};
    uint64_t refractoryUntilUs_ = 0;
    uint64_t suppressedByRefractory_ = 0;
    uint64_t candidatesAbandoned_ = 0;
};

}  // namespace nat::tools
