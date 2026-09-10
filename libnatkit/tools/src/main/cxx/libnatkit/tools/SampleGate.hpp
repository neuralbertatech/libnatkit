#pragma once

// The MARKER -> DATA lane crossing (TEC-NATKIT-109).
//
// Pass data only between an opening marker and a closing marker. That is how
// "only train on the cue windows" stops being logic buried inside the experiment
// node and becomes a wiring decision — and it is what turns the
// record -> train -> live-classify path into a graph rather than a pipeline with
// a mode flag.
//
// ⚠️ THE DEFAULT SPLITS A FRAME AT THE SAMPLE, and that is not a nicety. A gate
// that rounded to frame boundaries would admit up to a frame of the wrong class
// at each edge of every cue window, silently contaminating every training set it
// produced. That is the same shape as the train/serve mismatch that once put
// gesture accuracy at chance, so the honest default is the exact one.
//
// ⚠️ IT ALSO NEEDS A WATERMARK, for the same reason CombineJoin.hpp does. Data
// and markers arrive on two different channels, so a marker timestamped inside a
// frame can arrive AFTER that frame. Admitting frames as they arrive would make
// the output depend on inter-channel arrival skew, and a live run would disagree
// with a replay of its own recording. A frame is therefore held until the marker
// lane has advanced past its last sample.

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

namespace nat::tools {

// What to do with a frame that straddles an edge of the open window.
enum class GateEdgeMode {
    // Emit exactly the samples inside the window. The default, and the only one
    // that does not contaminate the output.
    SplitAtSample,
    // Emit the whole frame if any sample is inside. Cheaper downstream (frame
    // sizes stay uniform) at the cost of admitting out-of-window samples.
    PassWholeFrame,
    // Emit the frame only if every sample is inside. Loses real data at both
    // edges, but never admits a sample from outside.
    DropPartialFrame,
};

inline const char* gateEdgeModeName(GateEdgeMode mode)
{
    switch (mode) {
    case GateEdgeMode::SplitAtSample:
        return "split_at_sample";
    case GateEdgeMode::PassWholeFrame:
        return "pass_whole_frame";
    case GateEdgeMode::DropPartialFrame:
        return "drop_partial_frame";
    }
    return "split_at_sample";
}

inline std::optional<GateEdgeMode> parseGateEdgeMode(const std::string& name)
{
    if (name == "split_at_sample") return GateEdgeMode::SplitAtSample;
    if (name == "pass_whole_frame") return GateEdgeMode::PassWholeFrame;
    if (name == "drop_partial_frame") return GateEdgeMode::DropPartialFrame;
    return std::nullopt;
}

struct SampleGateConfig {
    GateEdgeMode edgeMode = GateEdgeMode::SplitAtSample;
    // Bound on buffered frames while waiting for the marker lane to advance.
    // Evicts oldest, matching the drop-oldest policy of the channels feeding us.
    size_t maxQueuedFrames = 256;
};

// One admitted run of samples out of a single input frame.
template <typename Frame>
struct GatedSpan {
    Frame frame{};
    size_t firstSample = 0;
    size_t sampleCount = 0;
    // The device_ts_us the emitted frame should carry: the time of
    // `firstSample`, not of the original frame's sample 0.
    uint64_t startTsUs = 0;
};

// Admits the parts of each frame that fall inside an open window.
// `Frame` must expose `deviceTsUs`, `sampleRateHz` and `samplesPerChannel`.
template <typename Frame>
class SampleGate {
public:
    explicit SampleGate(SampleGateConfig config) : config_(config) {}

    // An opening or closing marker, at its own device time. Matching a
    // MarkerEventV1's fields against the node's configured labels happens in the
    // worker; by the time it reaches here the decision is made.
    void pushMarker(uint64_t at_us, bool opens)
    {
        if (at_us > markerHighWaterUs_) {
            markerHighWaterUs_ = at_us;
        }
        markerSeen_ = true;
        // Repeated opens (or closes) are idempotent: a second open while already
        // open does not restart the window, and a close with no open is ignored.
        // Both are things a real protocol emits, and neither is an error.
        if (!events_.empty() && events_.back().opens == opens) {
            return;
        }
        events_.push_back(Event{at_us, opens});
    }

    void pushFrame(Frame frame)
    {
        frames_.push_back(std::move(frame));
        while (frames_.size() > config_.maxQueuedFrames) {
            frames_.pop_front();
            ++framesDroppedOverflow_;
        }
    }

    // Drain in a loop: one frame can yield several spans when a window opens and
    // closes inside it.
    std::optional<GatedSpan<Frame>> tryEmit()
    {
        while (true) {
            if (!pendingSpans_.empty()) {
                auto span = pendingSpans_.front();
                pendingSpans_.pop_front();
                return span;
            }
            if (frames_.empty() || !markerSeen_) {
                return std::nullopt;
            }
            const auto& frame = frames_.front();
            const size_t count = frame.samplesPerChannel;
            if (count == 0) {
                frames_.pop_front();
                continue;
            }
            const double period_us = frame.sampleRateHz > 0
                ? 1'000'000.0 / static_cast<double>(frame.sampleRateHz)
                : 0.0;
            const uint64_t last_sample_ts = sampleTs(frame, count - 1, period_us);
            // The watermark: hold the frame until markers are known to have
            // advanced past its final sample, or a late-arriving marker could
            // have changed the answer we already gave.
            if (last_sample_ts > markerHighWaterUs_) {
                return std::nullopt;
            }
            auto frame_copy = frames_.front();
            frames_.pop_front();
            planSpans(frame_copy, count, period_us);
        }
    }

    // Samples admitted and rejected, in samples rather than frames — a
    // frame-granular count would be meaningless once frames are split.
    uint64_t samplesAdmitted() const { return samplesAdmitted_; }
    uint64_t samplesRejected() const { return samplesRejected_; }
    uint64_t framesDroppedOverflow() const { return framesDroppedOverflow_; }
    bool isOpen() const { return openState_; }

private:
    struct Event {
        uint64_t atUs = 0;
        bool opens = false;
    };

    static uint64_t sampleTs(const Frame& frame, size_t index, double period_us)
    {
        return frame.deviceTsUs +
            static_cast<uint64_t>(
                std::llround(period_us * static_cast<double>(index)));
    }

    // Is the gate open at `ts`? Consumes events up to that time, so the answer
    // walks forward with the data rather than rescanning history.
    bool openAt(uint64_t ts)
    {
        while (!events_.empty() && events_.front().atUs <= ts) {
            openState_ = events_.front().opens;
            events_.pop_front();
        }
        return openState_;
    }

    void planSpans(const Frame& frame, size_t count, double period_us)
    {
        // Per-sample admission, then run-length encoded back into spans. Doing
        // it sample-wise first is what makes a window that opens and closes
        // inside one frame come out as two spans rather than one wrong one.
        std::vector<bool> admitted(count, false);
        size_t admitted_count = 0;
        for (size_t index = 0; index < count; ++index) {
            const uint64_t ts = sampleTs(frame, index, period_us);
            if (openAt(ts)) {
                admitted[index] = true;
                ++admitted_count;
            }
        }

        if (admitted_count == 0) {
            samplesRejected_ += count;
            return;
        }
        if (config_.edgeMode == GateEdgeMode::DropPartialFrame &&
            admitted_count != count) {
            samplesRejected_ += count;
            return;
        }
        if (config_.edgeMode == GateEdgeMode::PassWholeFrame ||
            admitted_count == count) {
            GatedSpan<Frame> span;
            span.frame = frame;
            span.firstSample = 0;
            span.sampleCount = count;
            span.startTsUs = frame.deviceTsUs;
            samplesAdmitted_ += count;
            pendingSpans_.push_back(span);
            return;
        }

        size_t index = 0;
        while (index < count) {
            if (!admitted[index]) {
                ++samplesRejected_;
                ++index;
                continue;
            }
            const size_t first = index;
            while (index < count && admitted[index]) {
                ++index;
            }
            GatedSpan<Frame> span;
            span.frame = frame;
            span.firstSample = first;
            span.sampleCount = index - first;
            span.startTsUs = sampleTs(frame, first, period_us);
            samplesAdmitted_ += span.sampleCount;
            pendingSpans_.push_back(span);
        }
    }

    SampleGateConfig config_{};
    std::deque<Event> events_{};
    std::deque<Frame> frames_{};
    std::deque<GatedSpan<Frame>> pendingSpans_{};
    uint64_t markerHighWaterUs_ = 0;
    bool markerSeen_ = false;
    bool openState_ = false;  // a gate with no markers yet passes nothing
    uint64_t samplesAdmitted_ = 0;
    uint64_t samplesRejected_ = 0;
    uint64_t framesDroppedOverflow_ = 0;
};

}  // namespace nat::tools
