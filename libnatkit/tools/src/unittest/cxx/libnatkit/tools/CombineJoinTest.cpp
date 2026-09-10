#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include <libnatkit/tools/CombineJoin.hpp>

namespace {

using nat::tools::CombineJoinConfig;
using nat::tools::CombineJoinPolicy;
using nat::tools::CombineJoiner;
using nat::tools::parseCombineJoinPolicy;

// The joiner needs exactly one thing from a frame: its device_ts_us. Using a
// stand-in rather than NormalizedNumericChannelFrame is the point of the
// extraction — the four policies are decidable from timestamps alone, so they
// are testable without a broker, a descriptor or a thread.
struct Frame {
    uint64_t deviceTsUs = 0;
    int tag = 0;  // identifies WHICH frame came out, not just how many
};

CombineJoinConfig withPolicy(CombineJoinPolicy policy)
{
    CombineJoinConfig config;
    config.policy = policy;
    return config;
}

// Drains the joiner, as the worker's run loop does.
std::vector<nat::tools::CombineEmission<Frame>> drain(
    CombineJoiner<Frame>& joiner)
{
    std::vector<nat::tools::CombineEmission<Frame>> emissions;
    while (const auto emission = joiner.tryEmit()) {
        emissions.push_back(emission.value());
    }
    return emissions;
}

TEST(CombineJoinPolicyNames, RoundTripsAndRejectsUnknown)
{
    for (const auto policy :
         {CombineJoinPolicy::Zip, CombineJoinPolicy::CombineLatest,
          CombineJoinPolicy::WithLatestFrom, CombineJoinPolicy::Sample}) {
        const auto name = nat::tools::combineJoinPolicyName(policy);
        const auto parsed = parseCombineJoinPolicy(name);
        ASSERT_TRUE(parsed.has_value()) << name;
        EXPECT_EQ(parsed.value(), policy);
    }
    EXPECT_FALSE(parseCombineJoinPolicy("").has_value());
    EXPECT_FALSE(parseCombineJoinPolicy("combineLatest").has_value());
    EXPECT_FALSE(parseCombineJoinPolicy("ZIP").has_value());
}

// The default must remain zip: every graph saved before join policies existed
// has no join_policy in its config, and must keep behaving exactly as it did.
TEST(CombineJoinDefaults, DefaultPolicyIsZip)
{
    EXPECT_EQ(CombineJoinConfig{}.policy, CombineJoinPolicy::Zip);
    EXPECT_EQ(CombineJoinConfig{}.alignToleranceUs, 50'000U);
}

// --- zip ------------------------------------------------------------------

TEST(CombineJoinZip, EmitsOnePerInputAndWaitsForLaggards)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::Zip));

    joiner.push(0, Frame{1'000, 1});
    // One input alone is never enough under zip, however long it waits.
    EXPECT_TRUE(drain(joiner).empty());

    joiner.push(1, Frame{1'000, 2});
    const auto emissions = drain(joiner);
    ASSERT_EQ(emissions.size(), 1U);
    ASSERT_EQ(emissions[0].frames.size(), 2U);
    EXPECT_EQ(emissions[0].frames[0].tag, 1);
    EXPECT_EQ(emissions[0].frames[1].tag, 2);
    EXPECT_EQ(emissions[0].outputTsUs, 1'000U);
}

TEST(CombineJoinZip, PairsByTimestampNotArrivalOrder)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::Zip));

    // Spaced BEYOND the 50 ms default tolerance on purpose: 1 ms apart, these
    // frames would legitimately pair, and the test would prove nothing.
    // Input 1's frames arrive in a burst, out of step with input 0.
    joiner.push(1, Frame{100'000, 20});
    joiner.push(1, Frame{200'000, 21});
    joiner.push(0, Frame{200'000, 10});

    const auto emissions = drain(joiner);
    ASSERT_EQ(emissions.size(), 1U);
    // The 200 ms frame is paired with the 200 ms frame — NOT with input 1's
    // arrival-order front (tag 20 at t=100 ms), which is what the pre-Phase-5
    // FIFO would have done.
    EXPECT_EQ(emissions[0].frames[0].tag, 10);
    EXPECT_EQ(emissions[0].frames[1].tag, 21);
    EXPECT_EQ(joiner.droppedUnmatched(), 1U);  // tag 20 was unmatchable
}

TEST(CombineJoinZip, ToleranceIsConfigurableAndBounds)
{
    CombineJoinConfig tight = withPolicy(CombineJoinPolicy::Zip);
    tight.alignToleranceUs = 1'000;
    CombineJoiner<Frame> strict(2, tight);
    strict.push(0, Frame{0, 1});
    strict.push(1, Frame{5'000, 2});
    // 5 ms apart with a 1 ms tolerance: not a pair. Under the old hardcoded
    // 50 ms these would have been silently merged.
    EXPECT_TRUE(drain(strict).empty());

    CombineJoinConfig loose = withPolicy(CombineJoinPolicy::Zip);
    loose.alignToleranceUs = 10'000;
    CombineJoiner<Frame> lenient(2, loose);
    lenient.push(0, Frame{0, 1});
    lenient.push(1, Frame{5'000, 2});
    EXPECT_EQ(drain(lenient).size(), 1U);
}

TEST(CombineJoinZip, OverflowEvictsOldestAndIsCounted)
{
    CombineJoinConfig config = withPolicy(CombineJoinPolicy::Zip);
    config.maxQueuedFramesPerInput = 4;
    CombineJoiner<Frame> joiner(2, config);

    for (int i = 0; i < 10; ++i) {
        joiner.push(0, Frame{static_cast<uint64_t>(i) * 1'000, i});
    }
    EXPECT_EQ(joiner.droppedOverflow(), 6U);
}

// --- the join watermark ---------------------------------------------------

// The property the other three policies are built on, tested on its own because
// everything below depends on it: a frame is processed only once no input can
// still deliver something older. The limit is the SMALLEST high-water mark
// across the inputs, which is sound because ingress guarantees each input is
// non-decreasing (docs/STREAM_CONTRACT.md).
TEST(CombineJoinWatermark, NothingIsProcessedUntilEveryInputHasSpokenOnce)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::CombineLatest));
    for (int i = 1; i <= 20; ++i) {
        joiner.push(0, Frame{static_cast<uint64_t>(i) * 1'000, i});
    }
    // Twenty frames on one input and none on the other: the joiner cannot know
    // whether input 1 is about to deliver something timestamped before all of
    // them, so it processes nothing at all.
    EXPECT_TRUE(drain(joiner).empty());
}

TEST(CombineJoinWatermark, AFastInputIsHeldAtTheSlowInputsHighWaterMark)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::CombineLatest));
    joiner.push(0, Frame{1'000, 1});
    joiner.push(1, Frame{1'000, 20});
    ASSERT_EQ(drain(joiner).size(), 1U);

    // The fast input runs ahead. Its frames are all newer than the slow input's
    // high-water mark, so none may be processed yet — holding them is the whole
    // mechanism, not a bug.
    for (int i = 2; i <= 10; ++i) {
        joiner.push(0, Frame{static_cast<uint64_t>(i) * 1'000, i});
    }
    EXPECT_TRUE(drain(joiner).empty());

    // One slow frame lifts the watermark and the whole burst becomes
    // processable, in timestamp order.
    joiner.push(1, Frame{10'000, 21});
    const auto emissions = drain(joiner);
    EXPECT_EQ(emissions.size(), 10U);
    EXPECT_EQ(emissions.front().outputTsUs, 2'000U);
    EXPECT_EQ(emissions.back().outputTsUs, 10'000U);
}

// --- combineLatest --------------------------------------------------------

TEST(CombineJoinCombineLatest, EmitsNothingUntilEveryInputHasProduced)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::CombineLatest));

    joiner.push(0, Frame{1'000, 1});
    joiner.push(0, Frame{2'000, 2});
    // Rx: combineLatest is silent until all sources have emitted once.
    EXPECT_TRUE(drain(joiner).empty());

    joiner.push(1, Frame{2'100, 20});
    joiner.push(0, Frame{3'000, 3});
    const auto emissions = drain(joiner);
    ASSERT_EQ(emissions.size(), 1U);
    // Input 0's two earlier frames were stepped past while input 1 had no value,
    // producing nothing — they are consumed, not replayed. The first emission
    // therefore carries input 0's LATEST at that point, not its first.
    EXPECT_EQ(emissions[0].frames[0].tag, 2);
    EXPECT_EQ(emissions[0].frames[1].tag, 20);
    EXPECT_EQ(emissions[0].outputTsUs, 2'100U);  // newest constituent
}

TEST(CombineJoinCombineLatest, MixedRateEmitsPerFastFrameReusingTheSlowOne)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::CombineLatest));

    // The slow input brackets the burst; the fast input produces nine frames in
    // between. This is exactly the case zip throws away and combineLatest is for.
    joiner.push(1, Frame{0, 200});
    joiner.push(1, Frame{10'000, 201});
    for (int i = 1; i <= 9; ++i) {
        joiner.push(0, Frame{static_cast<uint64_t>(i) * 1'000, 100 + i});
    }

    const auto emissions = drain(joiner);
    ASSERT_EQ(emissions.size(), 9U);
    for (const auto& emission : emissions) {
        EXPECT_EQ(emission.frames[1].tag, 200);  // slow input's value reused
    }
    EXPECT_EQ(emissions.front().frames[0].tag, 101);
    EXPECT_EQ(emissions.back().frames[0].tag, 109);
    EXPECT_EQ(emissions.back().outputTsUs, 9'000U);
    EXPECT_EQ(joiner.droppedUnmatched(), 0U);  // nothing is discarded here
}

// --- withLatestFrom -------------------------------------------------------

TEST(CombineJoinWithLatestFrom, OnlyThePrimaryTriggers)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::WithLatestFrom));

    // Ambient input 1 ticks four times; primary input 0 twice, in between.
    joiner.push(1, Frame{1'000, 21});
    joiner.push(1, Frame{2'000, 22});
    joiner.push(1, Frame{3'000, 23});
    joiner.push(1, Frame{4'000, 24});
    joiner.push(0, Frame{2'500, 1});
    joiner.push(0, Frame{4'500, 2});

    const auto emissions = drain(joiner);
    // FOUR ambient frames produced ZERO outputs. One primary frame produced one.
    // (The primary's 4'500 frame is beyond the ambient high-water mark of 4'000,
    // so it is correctly still held.)
    ASSERT_EQ(emissions.size(), 1U);
    EXPECT_EQ(emissions[0].frames[0].tag, 1);
    // The ambient value is the most recent one AT OR BEFORE the primary's time —
    // 22 at t=2'000, not 24 at t=4'000. Reading the newest available frame
    // instead would be reading the future, which is what makes a live pipeline
    // and a replay disagree.
    EXPECT_EQ(emissions[0].frames[1].tag, 22);
    // The primary is the clock, so the primary's timestamp stamps the output.
    EXPECT_EQ(emissions[0].outputTsUs, 2'500U);
}

TEST(CombineJoinWithLatestFrom, PrimaryFramesWithNoContextAreDroppedAndCounted)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::WithLatestFrom));
    joiner.push(0, Frame{1'000, 1});
    joiner.push(0, Frame{2'000, 2});
    joiner.push(0, Frame{3'000, 3});
    joiner.push(1, Frame{5'000, 20});  // lifts the watermark past all three

    EXPECT_TRUE(drain(joiner).empty());
    // Rx drops a primary emission with no counterpart yet. Three were dropped,
    // and the count is what makes that visible instead of mysterious.
    EXPECT_EQ(joiner.droppedUnmatched(), 3U);
}

// --- sample ---------------------------------------------------------------

TEST(CombineJoinSample, EmitsOnADataClockGridNotAnArrivalGrid)
{
    CombineJoinConfig config = withPolicy(CombineJoinPolicy::Sample);
    config.samplePeriodUs = 1'000;  // 1 kHz
    CombineJoiner<Frame> joiner(2, config);

    joiner.push(0, Frame{10'000, 1});
    joiner.push(1, Frame{10'000, 2});
    // The first tick lands where the inputs became complete, so the grid's
    // phase is a property of the data rather than of when the worker started.
    const auto first = drain(joiner);
    ASSERT_EQ(first.size(), 1U);
    EXPECT_EQ(first[0].outputTsUs, 10'000U);

    // Advancing the data clock by 5 periods owes 5 ticks, stamped on the grid
    // rather than on the inputs' own timestamps.
    joiner.push(0, Frame{15'000, 3});
    joiner.push(1, Frame{15'000, 4});
    const auto ticks = drain(joiner);
    ASSERT_EQ(ticks.size(), 5U);
    EXPECT_EQ(ticks[0].outputTsUs, 11'000U);
    EXPECT_EQ(ticks[4].outputTsUs, 15'000U);
    // Every tick carries the latest of each input — the grid stays regular even
    // though the inputs are not.
    EXPECT_EQ(ticks[4].frames[0].tag, 3);
    EXPECT_EQ(ticks[4].frames[1].tag, 2);
}

TEST(CombineJoinSample, ALargeDataClockLeapResyncsInsteadOfFlooding)
{
    CombineJoinConfig config = withPolicy(CombineJoinPolicy::Sample);
    config.samplePeriodUs = 1'000;
    config.maxCatchUpTicks = 4;
    CombineJoiner<Frame> joiner(2, config);

    joiner.push(0, Frame{0, 1});
    joiner.push(1, Frame{0, 2});
    ASSERT_EQ(drain(joiner).size(), 1U);

    // A replay seek jumps the data clock a million periods forward. Emitting one
    // stale frame per elapsed period would produce a million duplicates.
    joiner.push(0, Frame{1'000'000'000, 3});
    joiner.push(1, Frame{1'000'000'000, 4});
    const auto emissions = drain(joiner);
    EXPECT_EQ(emissions.size(), 1U);
    EXPECT_EQ(joiner.ticksResynced(), 1U);
    EXPECT_EQ(emissions[0].outputTsUs, 1'000'000'000U);
}

TEST(CombineJoinSample, AZeroPeriodEmitsNothingRatherThanDividingByZero)
{
    CombineJoinConfig config = withPolicy(CombineJoinPolicy::Sample);
    config.samplePeriodUs = 0;
    CombineJoiner<Frame> joiner(2, config);
    joiner.push(0, Frame{1'000, 1});
    joiner.push(1, Frame{1'000, 2});
    EXPECT_TRUE(drain(joiner).empty());
}

// --- the contract's duplicate-timestamp rule -------------------------------

// docs/STREAM_CONTRACT.md: ingress clamps a backwards step forward, which makes
// two frames share a device_ts_us. A join must not match the same frame twice
// or emit twice for one pair.
TEST(CombineJoinContract, ClampedDuplicateTimestampsAreDistinctFrames)
{
    CombineJoiner<Frame> joiner(2, withPolicy(CombineJoinPolicy::Zip));

    joiner.push(0, Frame{7'000, 1});
    joiner.push(0, Frame{7'000, 2});  // the clamped one
    joiner.push(1, Frame{7'000, 20});
    joiner.push(1, Frame{7'000, 21});

    const auto emissions = drain(joiner);
    ASSERT_EQ(emissions.size(), 2U);
    // Two pairs out of four frames — each consumed exactly once, in order.
    EXPECT_EQ(emissions[0].frames[0].tag, 1);
    EXPECT_EQ(emissions[0].frames[1].tag, 20);
    EXPECT_EQ(emissions[1].frames[0].tag, 2);
    EXPECT_EQ(emissions[1].frames[1].tag, 21);
}

// ⚠️ THE MOST IMPORTANT TEST IN THIS FILE.
//
// Determinism is the epic's headline invariant: the same input sequence must
// produce the same output regardless of how fast it is fed. Here "slow" drains
// after every push and "fast" pushes everything first — which is the difference
// between a live 1x run and a max-speed replay of the same recording.
//
// This test FAILED on the first implementation, which kept one "latest" slot
// per input and counted triggers. That version collapsed several arrivals into
// the last one whenever they landed between two drains, so combine_latest
// produced {1000, 2000, 2000, 3000, 3000} fed slowly and {3000, 3000, 3000,
// 3000, 3000} fed quickly. The watermark exists to make these equal.
TEST(CombineJoinContract, OutputIsIndependentOfFeedRate)
{
    const std::vector<std::pair<size_t, Frame>> script{
        {0, Frame{1'000, 1}},  {1, Frame{1'000, 20}}, {0, Frame{2'000, 2}},
        {1, Frame{2'000, 21}}, {0, Frame{3'000, 3}},  {1, Frame{3'000, 22}},
    };

    for (const auto policy :
         {CombineJoinPolicy::Zip, CombineJoinPolicy::CombineLatest,
          CombineJoinPolicy::WithLatestFrom, CombineJoinPolicy::Sample}) {
        CombineJoinConfig config = withPolicy(policy);
        config.samplePeriodUs = 1'000;  // only read by Sample

        std::vector<uint64_t> slow_ts;
        CombineJoiner<Frame> slow(2, config);
        for (const auto& entry : script) {
            slow.push(entry.first, entry.second);
            for (const auto& emission : drain(slow)) {
                slow_ts.push_back(emission.outputTsUs);
            }
        }

        std::vector<uint64_t> fast_ts;
        CombineJoiner<Frame> fast(2, config);
        for (const auto& entry : script) {
            fast.push(entry.first, entry.second);
        }
        for (const auto& emission : drain(fast)) {
            fast_ts.push_back(emission.outputTsUs);
        }

        EXPECT_EQ(slow_ts, fast_ts)
            << "policy " << nat::tools::combineJoinPolicyName(policy);
        EXPECT_FALSE(slow_ts.empty())
            << "policy " << nat::tools::combineJoinPolicyName(policy)
            << " emitted nothing, so the comparison proves nothing";
    }
}

}  // namespace
