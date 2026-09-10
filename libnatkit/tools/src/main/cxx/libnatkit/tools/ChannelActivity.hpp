#pragma once

// PER-CHANNEL ACTIVITY, for the marble strips on node cards (TEC-NATKIT-106).
//
// Rx's real contribution to a *visual* dataflow language is not its operator
// list — it is the marble diagram, a per-operator ground truth you can look at.
// Drawn from real frames instead of an illustration, it makes the failure modes
// this whole epic is about visible for the first time:
//
//   * a combine silently misaligning shows as two input rows whose marbles do
//     not line up above the output row
//   * a starved input shows as a nearly empty row beside a dense one
//   * a drop-oldest edge shows as a gap
//   * an operator with a state bug shows as an output pattern that does not
//     follow from its inputs
//
// None of those is visible anywhere today. You find out from the shape of the
// result, later, if at all.
//
// ⚠️ THIS RECORDS device_ts_us, NOT ARRIVAL TIME, for the same reason every
// other temporal decision in the graph does (docs/STREAM_CONTRACT.md clause 4).
// A strip drawn on arrival time would show the network's behaviour rather than
// the data's, and would disagree with itself between a live run and a replay of
// that run.
//
// COST: this must stay off the data path. It records ONE uint64 per frame into
// fixed storage — no allocation, no locking beyond a single mutex, nothing
// proportional to sample count. The strip needs timestamps, not frames, which
// is what makes it affordable at all.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

namespace nat::tools {

// How much history a strip shows. Fixed DURATION rather than a fixed count,
// because the whole point is comparing rows against each other: two ports at
// different rates must share one time axis or their marbles cannot be read as
// aligned or misaligned. Four seconds is long enough to see a cadence and short
// enough that a 100 Hz stream stays legible.
constexpr uint64_t kActivityWindowUs = 4'000'000;

// Bucket width for the density histogram. 160 buckets across the window, which
// is comfortably under the ~200 px a node card can give a strip.
constexpr uint64_t kActivityBucketUs = 25'000;
constexpr size_t kActivityBucketCount =
    static_cast<size_t>(kActivityWindowUs / kActivityBucketUs);

// Above this many events in the window, exact timestamps stop being renderable:
// the strip is ~200 px wide, so more than one marble per ~2 px is a filled
// rectangle whatever we do. Past it the snapshot carries per-bucket COUNTS
// instead, which is both smaller on the wire and more honest on screen — a
// density shading says "a lot, evenly" where a solid bar says nothing.
constexpr size_t kActivityExactCap = 120;

enum class ActivityMode {
    // Few enough events to place individually. Sub-bucket precision preserved,
    // which is what makes misalignment visible at all: a combine's 50 ms
    // tolerance cannot be judged against a 25 ms bucket.
    Exact,
    // Too many to place. Per-bucket counts.
    Density,
};

struct ActivitySnapshot {
    ActivityMode mode = ActivityMode::Exact;
    uint64_t windowUs = kActivityWindowUs;
    uint64_t bucketUs = kActivityBucketUs;
    // The newest event's time; offsets below are measured back from it, so the
    // wire carries small deltas rather than 19-digit absolute microseconds.
    uint64_t baseUs = 0;
    // Exact mode: microseconds BEFORE baseUs, newest first.
    std::vector<uint64_t> offsetsUs{};
    // Density mode: counts per bucket, oldest bucket first.
    std::vector<uint32_t> buckets{};
    // Events in the window, exact in both modes.
    uint64_t total = 0;
};

// Records event times and answers "what happened in the last N seconds".
//
// A circular bucket histogram gives exact counts at any rate in O(1) per event
// and fixed memory, so a 1 kHz channel is as cheap as a 1 Hz one. A separate
// small ring keeps the newest few exactly, for when there are few enough to
// draw individually. Storing every timestamp instead would either allocate per
// frame or silently undercount once a cap was hit — and a strip that undercounts
// is worse than no strip, because it invites the wrong conclusion.
class ChannelActivity {
public:
    void record(uint64_t at_us)
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        // A timestamp older than the window is ignored rather than folded in:
        // ingress guarantees monotonicity, so this only happens on a clock
        // epoch change, and admitting it would corrupt every bucket index.
        if (newestUs_ > at_us && newestUs_ - at_us > kActivityWindowUs) {
            return;
        }
        const uint64_t index = at_us / kActivityBucketUs;
        auto& slot = buckets_[index % kActivityBucketCount];
        if (slot.epoch != index) {
            slot.epoch = index;
            slot.count = 0;
        }
        ++slot.count;

        recent_[recentHead_] = at_us;
        recentHead_ = (recentHead_ + 1) % kActivityExactCap;
        if (recentCount_ < kActivityExactCap) {
            ++recentCount_;
        }
        if (at_us > newestUs_) {
            newestUs_ = at_us;
        }
    }

    // `now_us` is the time the strip is being drawn AT — on the data clock, not
    // the wall clock. Callers pass the newest timestamp the graph has seen, so a
    // paused replay shows a still strip rather than one that empties itself.
    ActivitySnapshot snapshot(uint64_t now_us) const
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        ActivitySnapshot out;
        out.baseUs = newestUs_;
        if (newestUs_ == 0) {
            return out;  // nothing has ever been recorded
        }
        const uint64_t reference = std::max(now_us, newestUs_);
        const uint64_t oldest =
            reference > kActivityWindowUs ? reference - kActivityWindowUs : 0;
        const uint64_t newest_index = reference / kActivityBucketUs;

        uint64_t total = 0;
        for (const auto& slot : buckets_) {
            if (slot.count == 0) continue;
            if (slot.epoch > newest_index) continue;
            if (newest_index - slot.epoch >= kActivityBucketCount) continue;
            total += slot.count;
        }
        out.total = total;

        if (total <= kActivityExactCap) {
            out.mode = ActivityMode::Exact;
            // Newest first, so a renderer can stop early and a truncated list
            // still shows the most recent activity rather than the oldest.
            for (size_t step = 0; step < recentCount_; ++step) {
                const size_t index =
                    (recentHead_ + kActivityExactCap - 1 - step) %
                    kActivityExactCap;
                const uint64_t at = recent_[index];
                if (at < oldest) break;
                out.offsetsUs.push_back(newestUs_ - at);
            }
            return out;
        }

        out.mode = ActivityMode::Density;
        out.buckets.assign(kActivityBucketCount, 0);
        for (const auto& slot : buckets_) {
            if (slot.count == 0) continue;
            if (slot.epoch > newest_index) continue;
            const uint64_t age = newest_index - slot.epoch;
            if (age >= kActivityBucketCount) continue;
            // Oldest bucket first, so the array reads left-to-right as time.
            out.buckets[kActivityBucketCount - 1 - age] = slot.count;
        }
        return out;
    }

    uint64_t newestUs() const
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        return newestUs_;
    }

private:
    struct Bucket {
        uint64_t epoch = 0;   // which absolute bucket index this slot holds
        uint32_t count = 0;
    };

    mutable std::mutex mutex_{};
    std::array<Bucket, kActivityBucketCount> buckets_{};
    std::array<uint64_t, kActivityExactCap> recent_{};
    size_t recentHead_ = 0;
    size_t recentCount_ = 0;
    uint64_t newestUs_ = 0;
};

}  // namespace nat::tools
