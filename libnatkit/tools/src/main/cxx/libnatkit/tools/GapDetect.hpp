#pragma once

// GAP DETECTION on the data lane (TEC-NATKIT-115).
//
// "Consecutive frames are further apart than they should be" is a property of
// the TIMESTAMPS, so this needs no clock, replays identically, and is an
// ordinary data-lane operator. It is how a sensor dropping out mid-trial
// becomes something the graph can react to — wire it into a gate and "capture
// only while the sensor is healthy" is wiring rather than logic — instead of
// something noticed afterwards in analysis, if at all.
//
// ⚠️ THIS IS NOT A LIVENESS TIMEOUT, and the difference is the whole reason
// TEC-NATKIT-110 closed the way it did. "Nothing has arrived for N real
// seconds" is a statement about the TRANSPORT: on replay the data is all there,
// so a wall-clock timeout would not reproduce, and every downstream operator's
// determinism would then depend on when the graph happened to be run. That case
// already lives where it belongs — classifyTransformWorkerStatus reports a
// worker as `stalled` from the wall clock, in the status layer. This file is
// the data-time half only, and it must stay that way.
//
// ⚠️ IT FIRES WHEN DATA RESUMES, NOT WHEN IT STOPS. A gap is the distance
// between two consecutive frames, so it is only measurable once the second one
// arrives: a dropout that NEVER recovers produces no marker at all. That is not
// an oversight, it is the same boundary again -- knowing that a silence is still
// going on requires a clock the data cannot supply -- but it is a sharp edge,
// because "gap detector" sounds like it should fire on the silence itself. A
// dropout in progress shows up as a STALLED node (classifyTransformWorkerStatus)
// and on the marble strips as a lane gone quiet; a dropout that recovered shows
// up here, with its duration and cause. Both surfaces are needed and neither
// replaces the other.
//
// Transport-free like ThresholdDetect.hpp: the worker owns the I/O.

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

namespace nat::tools {

// What the sequence numbers say happened during the gap. The stream contract
// makes gap detection a promise precisely so this is answerable, and the two
// cases want different responses: lost frames are a transport problem, a pause
// is usually the operator stopping the sensor.
enum class GapCause {
    // seq_no jumped: frames were produced and never arrived.
    FramesLost,
    // seq_no is contiguous: the producer itself stopped for a while.
    Paused,
};

inline const char* gapCauseEvent(GapCause cause)
{
    // Carried in the marker's `event`, NOT only in the attributes JSON. A
    // downstream marker_filter or gate can match on label or event and on
    // nothing else, so putting the distinction anywhere else would make it
    // undetectable by the very operators meant to act on it.
    return cause == GapCause::FramesLost ? "gap_lost" : "gap_paused";
}

struct GapConfig {
    // The smallest inter-frame gap that counts as one. Compared against how far
    // the next frame's first sample falls beyond where it was expected, so this
    // is slack ON TOP of the normal frame cadence rather than an absolute
    // spacing — a 100 ms frame cadence does not trip a 50 ms threshold.
    uint64_t gapUs = 250'000;
};

struct GapEvent {
    // ⚠️ THE START OF THE GAP — the last sample time before the silence, not
    // the frame that ended it. The onset is the event; stamping the recovery
    // reports every dropout late by exactly the gap's own duration, which is
    // the same error `threshold` avoids by stamping the crossing rather than
    // the dwell.
    uint64_t atUs = 0;
    // How long the silence actually lasted, measured from where the next frame
    // was expected.
    uint64_t gapUs = 0;
    GapCause cause = GapCause::Paused;
    uint64_t seqBefore = 0;
    uint64_t seqAfter = 0;
    // Frames the sequence numbers say went missing (0 for a pause).
    uint64_t framesLost = 0;
};

// Feed one frame's timing per call; get a gap back when one opened before it.
class GapDetector {
public:
    explicit GapDetector(GapConfig config) : config_(config) {}

    // `frame_ts_us` is the time of sample 0 and `sample_rate_hz` gives the
    // spacing, matching the convention the windowing transforms already use.
    std::optional<GapEvent> push(
        uint64_t frame_ts_us,
        uint32_t sample_rate_hz,
        size_t samples_per_channel,
        uint64_t seq_no)
    {
        const double period_us = sample_rate_hz > 0
            ? 1'000'000.0 / static_cast<double>(sample_rate_hz)
            : 0.0;
        const uint64_t span_us = samples_per_channel > 0
            ? static_cast<uint64_t>(std::llround(
                  period_us * static_cast<double>(samples_per_channel)))
            : 0;

        std::optional<GapEvent> event;
        if (haveprevious_) {
            // A sequence number going BACKWARDS is a producer restart, not a
            // gap. The contract calls that an epoch change (it is counted as
            // seq_restarts in firmware), and reporting a dropout for a reboot
            // would be reporting the wrong event entirely.
            if (seq_no < lastSeq_) {
                ++restarts_;
            } else if (frame_ts_us > expectedNextUs_ &&
                       frame_ts_us - expectedNextUs_ >= config_.gapUs) {
                GapEvent found;
                found.atUs = lastEndUs_;
                found.gapUs = frame_ts_us - expectedNextUs_;
                found.seqBefore = lastSeq_;
                found.seqAfter = seq_no;
                // One frame's worth of advance is expected; anything beyond it
                // is frames that existed and did not arrive.
                found.framesLost = seq_no > lastSeq_ + 1
                    ? seq_no - lastSeq_ - 1
                    : 0;
                found.cause = found.framesLost > 0 ? GapCause::FramesLost
                                                   : GapCause::Paused;
                event = found;
                ++detected_;
            }
        }

        lastSeq_ = seq_no;
        // The last sample's time, which is where the silence starts if one
        // follows. Note this is (count - 1) periods after the frame's stamp,
        // while the NEXT frame is expected a full `count` periods after it.
        lastEndUs_ = span_us > 0 && samples_per_channel > 0
            ? frame_ts_us + static_cast<uint64_t>(std::llround(
                  period_us * static_cast<double>(samples_per_channel - 1)))
            : frame_ts_us;
        expectedNextUs_ = frame_ts_us + span_us;
        haveprevious_ = true;
        return event;
    }

    uint64_t detected() const { return detected_; }
    // Producer restarts seen. Worth reporting separately: a rising count here
    // with no gaps means a device is rebooting, which looks nothing like a
    // dropout but would be indistinguishable if both were counted as gaps.
    uint64_t restarts() const { return restarts_; }

private:
    GapConfig config_{};
    bool haveprevious_ = false;
    uint64_t lastSeq_ = 0;
    uint64_t lastEndUs_ = 0;
    uint64_t expectedNextUs_ = 0;
    uint64_t detected_ = 0;
    uint64_t restarts_ = 0;
};

}  // namespace nat::tools
