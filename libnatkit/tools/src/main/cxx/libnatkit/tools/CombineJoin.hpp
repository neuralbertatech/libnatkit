#pragma once

// The combine node's FAN-IN ALGEBRA (TEC-NATKIT-103).
//
// A combine node merges N >= 2 upstream channels into one. "Merge" is not one
// operation: there are four genuinely different scientific meanings of combining
// two sensors, and until this file existed all four collapsed into the first one
// with no warning and no way to choose (see CombineJoinPolicy below).
//
// This header is deliberately transport-free and frame-agnostic — it decides
// WHICH frames to emit and WHAT TIMESTAMP the result carries, and nothing else.
// That is what makes the four policies unit-testable side by side without a
// broker, a messenger or a thread; CombineWorker owns the I/O and delegates
// every decision here.
//
// ⚠️ EVERY DECISION IN THIS FILE READS device_ts_us, NEVER ARRIVAL TIME. That is
// the stream contract's clause 4 (docs/STREAM_CONTRACT.md) and it is what makes
// a replay at max speed produce byte-identical output to a replay at 1x. A
// policy that consulted the wall clock would pass every test that ran in real
// time and fail the only one that matters.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>
#include <vector>

namespace nat::tools {

// How a combine node reconciles inputs that do not arrive in lockstep.
//
// The Rx operator each one corresponds to is named because that vocabulary is
// where these semantics come from and it is far more precise than any prose
// summary: someone who knows Rx knows exactly what `CombineLatest` does here.
enum class CombineJoinPolicy {
    // zip: lockstep. One frame per input per output, waiting for laggards.
    // RIGHT FOR: several feature transforms derived from one common
    // sliding_window, which is what combine was originally built for. This is
    // the historical behaviour and remains the default, so every saved graph
    // keeps the semantics it was authored against.
    Zip,
    // combineLatest: emit whenever ANY input produces, reusing the most recent
    // frame of every other input.
    // RIGHT FOR: mixed-rate fusion (1 kHz EMG with 100 Hz IMU), where waiting
    // for the slow input under Zip throws away most of the fast one.
    CombineLatest,
    // withLatestFrom: input 0 is the clock; every other input is ambient state
    // sampled at input 0's cadence. A non-primary input producing emits nothing.
    // RIGHT FOR: classify on the EMG cadence with IMU pose as context.
    WithLatestFrom,
    // sample: a fixed-rate clock drives emission and every input contributes its
    // latest frame.
    // RIGHT FOR: fixed-rate logging of sources with no common cadence.
    Sample,
};

inline const char* combineJoinPolicyName(CombineJoinPolicy policy)
{
    switch (policy) {
    case CombineJoinPolicy::Zip:
        return "zip";
    case CombineJoinPolicy::CombineLatest:
        return "combine_latest";
    case CombineJoinPolicy::WithLatestFrom:
        return "with_latest_from";
    case CombineJoinPolicy::Sample:
        return "sample";
    }
    return "zip";
}

inline std::optional<CombineJoinPolicy> parseCombineJoinPolicy(
    const std::string& name)
{
    if (name == "zip") return CombineJoinPolicy::Zip;
    if (name == "combine_latest") return CombineJoinPolicy::CombineLatest;
    if (name == "with_latest_from") return CombineJoinPolicy::WithLatestFrom;
    if (name == "sample") return CombineJoinPolicy::Sample;
    return std::nullopt;
}

struct CombineJoinConfig {
    CombineJoinPolicy policy = CombineJoinPolicy::Zip;

    // Zip only. Two frames align if their device_ts_us differ by <= this.
    // Was a compile-time constant (50 ms, "~half a typical windowed-feature
    // cadence") — a sensible default for the case it was written for and an
    // arbitrary number for every other, which is why it is now per-node.
    uint64_t alignToleranceUs = 50'000;

    // Sample only. The emission period on the DATA clock. 0 disables emission
    // rather than dividing by zero.
    uint64_t samplePeriodUs = 100'000;  // 10 Hz

    // Per-input queue bound, all policies; the oldest frame is evicted when
    // full, matching the drop-oldest policy of the channels feeding it.
    size_t maxQueuedFramesPerInput = 64;

    // Sample only. If the data clock leaps further ahead than this many periods
    // (a replay seek, or a long sensor gap), the tick phase is resynchronised to
    // the data instead of emitting one stale frame per elapsed period. Without
    // it, seeking a recording forward would flood the output.
    uint64_t maxCatchUpTicks = 64;
};

// What the joiner decided to emit: one frame per input, in input order, plus the
// timestamp the merged output should carry.
template <typename Frame>
struct CombineEmission {
    std::vector<Frame> frames{};
    uint64_t outputTsUs = 0;
};

// Decides when a set of inputs has enough to emit, under one of the four
// policies. `Frame` need only expose a `deviceTsUs` member.
//
// Usage from the worker: push() every frame as it is normalized, then drain
// tryEmit() until it returns nullopt. Draining in a loop is required, not
// optional — CombineLatest can owe several emissions after one polling pass, and
// Sample can owe several ticks after a fast replay advances the data clock.
template <typename Frame>
class CombineJoiner {
public:
    CombineJoiner(size_t input_count, CombineJoinConfig config)
        : config_(config),
          queues_(input_count),
          current_(input_count),
          highWaterUs_(input_count)
    {
    }

    size_t inputCount() const { return current_.size(); }

    void push(size_t input_index, Frame frame)
    {
        if (input_index >= queues_.size()) {
            return;
        }
        auto& queue = queues_[input_index];
        // The input's high-water mark: the newest timestamp it has delivered.
        // Ingress guarantees it never goes backwards, which is exactly what
        // makes it usable as a watermark (see watermark() below).
        const uint64_t ts = frame.deviceTsUs;
        if (!highWaterUs_[input_index].has_value() ||
            ts > highWaterUs_[input_index].value()) {
            highWaterUs_[input_index] = ts;
        }
        queue.push_back(std::move(frame));
        // Bounding every input's queue is what stops a stalled input from
        // growing the others without limit while the join waits for it. Evicting
        // the oldest matches the drop-oldest policy of the channels feeding us.
        while (queue.size() > config_.maxQueuedFramesPerInput) {
            queue.pop_front();
            ++droppedOverflow_;
        }
    }

    std::optional<CombineEmission<Frame>> tryEmit()
    {
        if (config_.policy == CombineJoinPolicy::Zip) {
            return tryEmitZip();
        }
        // Sample can owe several ticks without consuming any further input, so
        // an outstanding tick is emitted before advancing the merge.
        if (config_.policy == CombineJoinPolicy::Sample) {
            if (auto tick = tryEmitSampleTick()) {
                return tick;
            }
        }
        // Walk the time-ordered merge one frame at a time until a policy decides
        // to emit. A step that triggers nothing (an ambient input under
        // withLatestFrom, an incomplete set under combineLatest) is not a
        // failure — it just advances state, so keep stepping.
        while (step()) {
            switch (config_.policy) {
            case CombineJoinPolicy::CombineLatest:
                // Rx: silent until every source has produced once; after that,
                // every arrival from any source produces one output.
                if (allPresent()) {
                    return emitCurrent(/*ts=*/dataClockUs_);
                }
                break;
            case CombineJoinPolicy::WithLatestFrom:
                // Only the primary triggers. A primary frame with no ambient
                // context yet is DROPPED, not buffered — Rx's semantics, and
                // the count makes it visible rather than mysterious.
                if (lastSteppedInput_ == 0) {
                    if (allPresent()) {
                        // The primary IS the clock: stamp with its time, not the
                        // newest constituent. The ambient inputs are older on
                        // purpose.
                        return emitCurrent(/*ts=*/current_[0]->deviceTsUs);
                    }
                    ++droppedUnmatched_;
                }
                break;
            case CombineJoinPolicy::Sample:
                if (auto tick = tryEmitSampleTick()) {
                    return tick;
                }
                break;
            case CombineJoinPolicy::Zip:
                break;
            }
        }
        return std::nullopt;
    }

    // Frames discarded because they could never be matched — an unmatchably-old
    // frame under Zip, or a primary frame with no context under WithLatestFrom.
    uint64_t droppedUnmatched() const { return droppedUnmatched_; }
    // Frames evicted because an input queue was full (Zip only).
    uint64_t droppedOverflow() const { return droppedOverflow_; }
    // Sample ticks skipped by a data-clock leap rather than emitted stale.
    uint64_t ticksResynced() const { return ticksResynced_; }

private:
    bool allPresent() const
    {
        for (const auto& frame : current_) {
            if (!frame.has_value()) {
                return false;
            }
        }
        return !current_.empty();
    }

    // The join watermark: the largest timestamp we can safely process, which is
    // the SMALLEST high-water mark across the inputs.
    //
    // ⚠️ THIS IS WHAT MAKES combineLatest / withLatestFrom / sample
    // DETERMINISTIC, and it is not optional. A frame may only be processed once
    // no input can still deliver something older than it. Because ingress
    // guarantees each input is non-decreasing (docs/STREAM_CONTRACT.md clause 4
    // and the monotonicity rule), input i can never again produce a timestamp
    // below its own high-water mark — so anything at or below the minimum of
    // those marks is genuinely the earliest remaining frame.
    //
    // The first cut of this file kept a single "latest" slot per input and
    // counted triggers instead. That made the output depend on how often the
    // worker happened to poll: several frames arriving between two drains
    // collapsed into the last one, and a 1x replay disagreed with a 10x replay.
    // CombineJoinContract.OutputIsIndependentOfFeedRate is that bug's regression
    // test — it is the single most important test in this file.
    //
    // The cost is latency, never correctness: a fast input is held at the slow
    // input's high-water mark, so combineLatest on 1 kHz + 100 Hz emits in
    // bursts of ten, one burst per slow frame. An input that stops entirely
    // stalls the join — the same behaviour zip has always had, visible as a
    // stalled node with a rising overflow-drop count rather than as silence.
    std::optional<uint64_t> watermark() const
    {
        uint64_t low = 0;
        bool first = true;
        for (const auto& mark : highWaterUs_) {
            if (!mark.has_value()) {
                return std::nullopt;  // an input has produced nothing yet
            }
            if (first || mark.value() < low) {
                low = mark.value();
                first = false;
            }
        }
        return first ? std::nullopt : std::optional<uint64_t>(low);
    }

    // Advance the merge by one frame: pop the earliest queued frame that the
    // watermark allows, and make it that input's current value. Ties are broken
    // by input index so the order is total and reproducible.
    bool step()
    {
        const auto limit = watermark();
        if (!limit.has_value()) {
            return false;
        }
        size_t earliest = 0;
        bool found = false;
        for (size_t index = 0; index < queues_.size(); ++index) {
            if (queues_[index].empty()) {
                continue;
            }
            const uint64_t ts = queues_[index].front().deviceTsUs;
            if (ts > limit.value()) {
                continue;  // may not be the earliest yet; hold it back
            }
            if (!found || ts < queues_[earliest].front().deviceTsUs) {
                earliest = index;
                found = true;
            }
        }
        if (!found) {
            return false;
        }
        current_[earliest] = queues_[earliest].front();
        queues_[earliest].pop_front();
        const uint64_t ts = current_[earliest]->deviceTsUs;
        if (ts > dataClockUs_) {
            dataClockUs_ = ts;
        }
        lastSteppedInput_ = earliest;
        return true;
    }

    std::optional<CombineEmission<Frame>> emitCurrent(uint64_t output_ts_us)
    {
        CombineEmission<Frame> emission;
        emission.frames.reserve(current_.size());
        for (const auto& frame : current_) {
            emission.frames.push_back(frame.value());
        }
        emission.outputTsUs = output_ts_us;
        return emission;
    }

    std::optional<CombineEmission<Frame>> tryEmitZip()
    {
        if (queues_.empty()) {
            return std::nullopt;
        }
        for (const auto& queue : queues_) {
            if (queue.empty()) {
                return std::nullopt;
            }
        }

        // target = the newest of the per-input fronts. Every input must have
        // reached at least this time for a group to align here. Each queue is
        // time-ordered, so its front is the oldest unconsumed frame.
        uint64_t target_ts = 0;
        for (const auto& queue : queues_) {
            if (queue.front().deviceTsUs > target_ts) {
                target_ts = queue.front().deviceTsUs;
            }
        }

        // Discard unmatchably-old frames — a faster input's frames with no
        // counterpart anywhere near target. Keep at least one so an input never
        // empties itself out of the join.
        for (auto& queue : queues_) {
            while (queue.size() > 1 &&
                   queue.front().deviceTsUs + config_.alignToleranceUs <
                       target_ts) {
                queue.pop_front();
                ++droppedUnmatched_;
            }
        }

        for (const auto& queue : queues_) {
            const uint64_t ts = queue.front().deviceTsUs;
            const uint64_t diff =
                ts > target_ts ? ts - target_ts : target_ts - ts;
            if (diff > config_.alignToleranceUs) {
                return std::nullopt;  // wait for a lagging input to catch up
            }
        }

        CombineEmission<Frame> emission;
        emission.frames.reserve(queues_.size());
        for (auto& queue : queues_) {
            emission.frames.push_back(queue.front());
            queue.pop_front();
        }
        // Input 0's timestamp, NOT the group's max. Every frame in the group is
        // within tolerance of every other, so the choice is cosmetic — but it
        // is the choice the pre-policy implementation made, and keeping it means
        // an existing saved graph's output is byte-identical after this change.
        emission.outputTsUs = emission.frames.front().deviceTsUs;
        return emission;
    }

    std::optional<CombineEmission<Frame>> tryEmitSampleTick()
    {
        if (!allPresent() || config_.samplePeriodUs == 0) {
            return std::nullopt;
        }
        // The first tick is placed where the inputs first became complete, so
        // the grid's phase is a property of the data rather than of when the
        // worker started.
        if (nextEmitTsUs_ == 0) {
            nextEmitTsUs_ = dataClockUs_;
        }
        if (dataClockUs_ < nextEmitTsUs_) {
            return std::nullopt;
        }
        // A replay seek or a long sensor gap can leave the data clock far past
        // the next tick. Emitting one stale frame per elapsed period would flood
        // the output with duplicates, so resynchronise the phase instead.
        if (dataClockUs_ - nextEmitTsUs_ >
            config_.samplePeriodUs * config_.maxCatchUpTicks) {
            nextEmitTsUs_ = dataClockUs_;
            ++ticksResynced_;
        }
        // The TICK's time, not the newest input's: the point of sample is a
        // regular output grid, and a grid whose stamps drift with input jitter
        // is not a grid.
        auto emission = emitCurrent(nextEmitTsUs_);
        nextEmitTsUs_ += config_.samplePeriodUs;
        return emission;
    }

    CombineJoinConfig config_{};
    // Every policy queues: zip pairs across the queues, and the other three walk
    // their time-ordered merge. Only zip reads them as groups.
    std::vector<std::deque<Frame>> queues_{};
    // The most recent frame the merge has stepped past, per input — the "latest"
    // that combineLatest and withLatestFrom reuse.
    std::vector<std::optional<Frame>> current_{};
    // Per-input high-water mark, the input to watermark(). nullopt until that
    // input has delivered its first frame.
    std::vector<std::optional<uint64_t>> highWaterUs_{};
    size_t lastSteppedInput_ = 0;
    uint64_t dataClockUs_ = 0;
    uint64_t nextEmitTsUs_ = 0;
    uint64_t droppedUnmatched_ = 0;
    uint64_t droppedOverflow_ = 0;
    uint64_t ticksResynced_ = 0;
};

}  // namespace nat::tools
