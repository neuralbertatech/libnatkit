#pragma once

// The k-way TIME-ORDERED MERGE with a watermark, extracted because three
// separate operators now need exactly this and a third copy would drift.
//
// The problem it solves, in one sentence: when several channels feed one node,
// an event may only be processed once no channel can still deliver something
// older than it — otherwise the output depends on inter-channel arrival skew,
// and a live run disagrees with a replay of its own recording.
//
// ⚠️ THE WATERMARK IS SOUND ONLY BECAUSE INGRESS GUARANTEES MONOTONICITY.
// docs/STREAM_CONTRACT.md clause 4 and its monotonicity rule mean a channel can
// never again produce a timestamp below its own high-water mark, so the minimum
// of those marks is a safe horizon. Without that guarantee this is wrong, not
// merely conservative — which is why the contract had to be written down first
// (TEC-NATKIT-104) before any of these operators could be built.
//
// The cost is latency, never correctness: a fast channel is held at the slowest
// channel's high-water mark, so a 1 kHz lane against a 100 Hz lane emits in
// bursts of ten. A channel that stops entirely stalls the merge, which presents
// as a stalled node with a rising drop count rather than as silence.
//
// CombineJoiner (CombineJoin.hpp) implements this same mechanism inline and
// predates the extraction; unifying it is a follow-up, not a silent rewrite of
// shipped behaviour.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <utility>
#include <vector>

namespace nat::tools {

template <typename Event>
class TimeOrderedMerge {
public:
    explicit TimeOrderedMerge(size_t lane_count, size_t max_queued_per_lane = 256)
        : lanes_(lane_count),
          highWaterUs_(lane_count),
          maxQueuedPerLane_(max_queued_per_lane)
    {
    }

    size_t laneCount() const { return lanes_.size(); }

    // The timestamp is passed explicitly rather than read off the event, so one
    // implementation serves markers (emitted_at_us) and frames (device_ts_us)
    // without either type having to agree on a member name.
    void push(size_t lane, Event event, uint64_t at_us)
    {
        if (lane >= lanes_.size()) {
            return;
        }
        if (!highWaterUs_[lane].has_value() || at_us > highWaterUs_[lane].value()) {
            highWaterUs_[lane] = at_us;
        }
        lanes_[lane].push_back(Entry{std::move(event), at_us});
        // Bounding each lane is what stops a stalled lane from growing the
        // others without limit while the merge waits for it. Evicting the oldest
        // matches the drop-oldest policy of the channels feeding us.
        while (lanes_[lane].size() > maxQueuedPerLane_) {
            lanes_[lane].pop_front();
            ++droppedOverflow_;
        }
    }

    // The horizon: the smallest high-water mark across lanes, or nullopt while
    // any lane has produced nothing at all (nothing can be ordered against a
    // lane that has never spoken).
    std::optional<uint64_t> watermark() const
    {
        if (lanes_.empty()) {
            return std::nullopt;
        }
        uint64_t low = 0;
        bool first = true;
        for (const auto& mark : highWaterUs_) {
            if (!mark.has_value()) {
                return std::nullopt;
            }
            if (first || mark.value() < low) {
                low = mark.value();
                first = false;
            }
        }
        return first ? std::nullopt : std::optional<uint64_t>(low);
    }

    // The next globally-earliest event the watermark permits, with the lane it
    // came from. Ties break by lane index, so the order is total and repeatable.
    std::optional<std::pair<size_t, Event>> next()
    {
        const auto limit = watermark();
        if (!limit.has_value()) {
            return std::nullopt;
        }
        size_t earliest = 0;
        bool found = false;
        for (size_t lane = 0; lane < lanes_.size(); ++lane) {
            if (lanes_[lane].empty()) {
                continue;
            }
            const uint64_t at = lanes_[lane].front().atUs;
            if (at > limit.value()) {
                continue;  // may not be the earliest yet; hold it back
            }
            if (!found || at < lanes_[earliest].front().atUs) {
                earliest = lane;
                found = true;
            }
        }
        if (!found) {
            return std::nullopt;
        }
        Entry entry = std::move(lanes_[earliest].front());
        lanes_[earliest].pop_front();
        if (entry.atUs > lastEmittedUs_) {
            lastEmittedUs_ = entry.atUs;
        }
        return std::make_pair(earliest, std::move(entry.event));
    }

    // The timestamp of the most recently released event — the merge's own clock.
    uint64_t lastEmittedUs() const { return lastEmittedUs_; }
    uint64_t droppedOverflow() const { return droppedOverflow_; }

private:
    struct Entry {
        Event event{};
        uint64_t atUs = 0;
    };

    std::vector<std::deque<Entry>> lanes_{};
    std::vector<std::optional<uint64_t>> highWaterUs_{};
    size_t maxQueuedPerLane_ = 256;
    uint64_t lastEmittedUs_ = 0;
    uint64_t droppedOverflow_ = 0;
};

}  // namespace nat::tools
