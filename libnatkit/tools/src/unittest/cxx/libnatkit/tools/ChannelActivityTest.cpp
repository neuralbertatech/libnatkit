#include <gtest/gtest.h>

#include <cstdint>
#include <numeric>

#include <libnatkit/tools/ChannelActivity.hpp>

namespace {

using nat::tools::ActivityMode;
using nat::tools::ChannelActivity;
using nat::tools::kActivityBucketCount;
using nat::tools::kActivityBucketUs;
using nat::tools::kActivityExactCap;
using nat::tools::kActivityWindowUs;

TEST(ChannelActivityTest, AnUntouchedChannelReportsNothingRatherThanZeroActivity)
{
    ChannelActivity activity;
    const auto snapshot = activity.snapshot(1'000'000);
    EXPECT_EQ(snapshot.total, 0U);
    EXPECT_EQ(snapshot.baseUs, 0U);
    EXPECT_TRUE(snapshot.offsetsUs.empty());
    EXPECT_TRUE(snapshot.buckets.empty());
}

// Few events: exact placement, because sub-bucket precision is the whole point.
// A combine's 50 ms alignment tolerance cannot be judged against a 25 ms bucket,
// so misalignment would be invisible in density mode.
TEST(ChannelActivityTest, FewEventsAreReportedExactlyAndNewestFirst)
{
    ChannelActivity activity;
    activity.record(1'000'000);
    activity.record(1'100'000);
    activity.record(1'250'000);

    const auto snapshot = activity.snapshot(1'250'000);
    EXPECT_EQ(snapshot.mode, ActivityMode::Exact);
    EXPECT_EQ(snapshot.total, 3U);
    EXPECT_EQ(snapshot.baseUs, 1'250'000U);
    // Offsets are measured BACK from the newest event, so the wire carries
    // small deltas instead of 19-digit absolute microseconds.
    ASSERT_EQ(snapshot.offsetsUs.size(), 3U);
    EXPECT_EQ(snapshot.offsetsUs[0], 0U);
    EXPECT_EQ(snapshot.offsetsUs[1], 150'000U);
    EXPECT_EQ(snapshot.offsetsUs[2], 250'000U);
}

TEST(ChannelActivityTest, EventsOlderThanTheWindowFallOutOfTheCount)
{
    ChannelActivity activity;
    activity.record(1'000'000);
    // Well beyond the 4 s window from the first event.
    activity.record(10'000'000);

    const auto snapshot = activity.snapshot(10'000'000);
    EXPECT_EQ(snapshot.total, 1U);
    ASSERT_EQ(snapshot.offsetsUs.size(), 1U);
    EXPECT_EQ(snapshot.offsetsUs[0], 0U);
}

// ⚠️ THE THRESHOLD DECISION. Above the exact cap the strip is ~200 px against
// more than one marble per 2 px, so individual placement is a filled rectangle
// whatever the renderer does. Per-bucket counts are both smaller on the wire and
// more honest on screen.
TEST(ChannelActivityTest, ManyEventsSwitchToDensityAndTheCountStaysExact)
{
    ChannelActivity activity;
    const uint64_t start = 100'000'000;
    // 400 events at 100 Hz across the whole 4 s window.
    for (int index = 0; index < 400; ++index) {
        activity.record(start + static_cast<uint64_t>(index) * 10'000);
    }
    const uint64_t newest = start + 399 * 10'000;

    const auto snapshot = activity.snapshot(newest);
    EXPECT_EQ(snapshot.mode, ActivityMode::Density);
    // The count is exact regardless of rate — a strip that undercounts is worse
    // than no strip, because it invites the wrong conclusion.
    EXPECT_EQ(snapshot.total, 400U);
    EXPECT_TRUE(snapshot.offsetsUs.empty());
    ASSERT_EQ(snapshot.buckets.size(), kActivityBucketCount);
    // The buckets must account for every event, not a sampled subset.
    const uint64_t summed = std::accumulate(
        snapshot.buckets.begin(), snapshot.buckets.end(), uint64_t{0});
    EXPECT_EQ(summed, 400U);
}

// The rate that makes a naive implementation either allocate per frame or
// silently cap. Neither is acceptable, so this pins that a very fast channel is
// still counted exactly.
TEST(ChannelActivityTest, AKilohertzChannelIsCountedExactlyAndCheaply)
{
    ChannelActivity activity;
    const uint64_t start = 500'000'000;
    for (int index = 0; index < 4'000; ++index) {
        activity.record(start + static_cast<uint64_t>(index) * 1'000);
    }
    const uint64_t newest = start + 3'999 * 1'000;

    const auto snapshot = activity.snapshot(newest);
    EXPECT_EQ(snapshot.mode, ActivityMode::Density);
    EXPECT_EQ(snapshot.total, 4'000U);
    const uint64_t summed = std::accumulate(
        snapshot.buckets.begin(), snapshot.buckets.end(), uint64_t{0});
    EXPECT_EQ(summed, 4'000U);
}

// A gap is one of the four things the strip exists to show — a drop-oldest edge
// under load looks exactly like this.
TEST(ChannelActivityTest, AGapInTheMiddleShowsAsEmptyBuckets)
{
    ChannelActivity activity;
    const uint64_t start = 100'000'000;
    // Dense for 1 s, silent for 1 s, dense again for 1 s.
    for (int index = 0; index < 200; ++index) {
        activity.record(start + static_cast<uint64_t>(index) * 5'000);
    }
    for (int index = 0; index < 200; ++index) {
        activity.record(start + 2'000'000 + static_cast<uint64_t>(index) * 5'000);
    }
    const uint64_t newest = start + 2'000'000 + 199 * 5'000;

    const auto snapshot = activity.snapshot(newest);
    ASSERT_EQ(snapshot.mode, ActivityMode::Density);
    EXPECT_EQ(snapshot.total, 400U);

    size_t empty_buckets = 0;
    for (const auto count : snapshot.buckets) {
        if (count == 0) ++empty_buckets;
    }
    // Roughly the 1 s silence, in 25 ms buckets — the exact figure depends on
    // where the window's edge falls, so this asserts the shape, not a number
    // that would break on an off-by-one in the harness rather than in the code.
    EXPECT_GT(empty_buckets, 20U);
    EXPECT_LT(empty_buckets, 100U);
}

// Buckets read left-to-right as time, so a renderer can draw them in order
// without knowing anything about the ring underneath.
TEST(ChannelActivityTest, DensityBucketsAreOrderedOldestFirst)
{
    ChannelActivity activity;
    const uint64_t start = 100'000'000;
    // Sparse early, dense late: the weight must land at the END of the array.
    for (int index = 0; index < 20; ++index) {
        activity.record(start + static_cast<uint64_t>(index) * 20'000);
    }
    for (int index = 0; index < 300; ++index) {
        activity.record(start + 3'000'000 + static_cast<uint64_t>(index) * 3'000);
    }
    const uint64_t newest = start + 3'000'000 + 299 * 3'000;

    const auto snapshot = activity.snapshot(newest);
    ASSERT_EQ(snapshot.mode, ActivityMode::Density);
    const size_t half = snapshot.buckets.size() / 2;
    const uint64_t first_half = std::accumulate(
        snapshot.buckets.begin(), snapshot.buckets.begin() + half, uint64_t{0});
    const uint64_t second_half = std::accumulate(
        snapshot.buckets.begin() + half, snapshot.buckets.end(), uint64_t{0});
    EXPECT_GT(second_half, first_half);
}

// A paused replay must show a still strip, not one that drains itself. The
// caller passes the newest time the GRAPH has seen, so `now` never runs ahead of
// the data on its own.
TEST(ChannelActivityTest, SnapshotUsesTheDataClockSoAPausedReplayHoldsStill)
{
    ChannelActivity activity;
    activity.record(1'000'000);
    activity.record(1'500'000);

    const auto held = activity.snapshot(1'500'000);
    EXPECT_EQ(held.total, 2U);

    // Asking again with the same data clock gives the same answer, however much
    // wall-clock time has passed in between.
    const auto again = activity.snapshot(1'500'000);
    EXPECT_EQ(again.total, held.total);
    EXPECT_EQ(again.offsetsUs, held.offsetsUs);

    // Only advancing the data clock past the window empties it.
    const auto later = activity.snapshot(1'500'000 + kActivityWindowUs + 1);
    EXPECT_EQ(later.total, 0U);
}

// Clock epochs happen (a leaf reboot). Folding a wildly old timestamp in would
// corrupt every bucket index, so it is ignored rather than admitted.
TEST(ChannelActivityTest, ATimestampFarBeforeTheWindowIsIgnoredNotFoldedIn)
{
    ChannelActivity activity;
    activity.record(100'000'000);
    activity.record(50);  // a pre-epoch stamp from a restarted device

    const auto snapshot = activity.snapshot(100'000'000);
    EXPECT_EQ(snapshot.total, 1U);
    EXPECT_EQ(snapshot.baseUs, 100'000'000U);
}

TEST(ChannelActivityTest, TheExactCapIsTheModeBoundary)
{
    ChannelActivity at_cap;
    const uint64_t start = 100'000'000;
    for (size_t index = 0; index < kActivityExactCap; ++index) {
        at_cap.record(start + index * 10'000);
    }
    EXPECT_EQ(
        at_cap.snapshot(start + (kActivityExactCap - 1) * 10'000).mode,
        ActivityMode::Exact);

    ChannelActivity over_cap;
    for (size_t index = 0; index < kActivityExactCap + 1; ++index) {
        over_cap.record(start + index * 10'000);
    }
    EXPECT_EQ(
        over_cap.snapshot(start + kActivityExactCap * 10'000).mode,
        ActivityMode::Density);
}

}  // namespace
