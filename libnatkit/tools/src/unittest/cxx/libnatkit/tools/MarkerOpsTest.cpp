#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include <libnatkit/tools/MarkerOps.hpp>

namespace {

using nat::tools::MarkerDebounce;
using nat::tools::MarkerEventView;
using nat::tools::MarkerFilter;
using nat::tools::MarkerFilterConfig;
using nat::tools::MarkerMatchField;
using nat::tools::MarkerMerge;
using nat::tools::MarkerTakeUntil;
using nat::tools::parseMarkerMatchField;

MarkerEventView cue(uint64_t at_us, std::string label, std::string event = "onset")
{
    MarkerEventView marker;
    marker.atUs = at_us;
    marker.label = std::move(label);
    marker.event = std::move(event);
    marker.markerType = "experiment";
    return marker;
}

TEST(MarkerMatchFieldNames, RoundTripsAndRejectsUnknown)
{
    for (const auto field :
         {MarkerMatchField::Label, MarkerMatchField::Event,
          MarkerMatchField::MarkerType}) {
        const auto name = nat::tools::markerMatchFieldName(field);
        const auto parsed = parseMarkerMatchField(name);
        ASSERT_TRUE(parsed.has_value()) << name;
        EXPECT_EQ(parsed.value(), field);
    }
    EXPECT_FALSE(parseMarkerMatchField("session_id").has_value());
    EXPECT_FALSE(parseMarkerMatchField("").has_value());
}

// --- filter ---------------------------------------------------------------

TEST(MarkerFilterTest, MatchesTheChosenFieldWithSeveralValuesOred)
{
    MarkerFilterConfig config;
    config.field = MarkerMatchField::Label;
    config.values = {"fist", "pinch"};
    MarkerFilter filter(config);

    EXPECT_TRUE(filter.passes(cue(0, "fist")));
    EXPECT_TRUE(filter.passes(cue(0, "pinch")));
    EXPECT_FALSE(filter.passes(cue(0, "rest")));
}

TEST(MarkerFilterTest, ExcludeInvertsTheMatch)
{
    MarkerFilterConfig config;
    config.field = MarkerMatchField::Label;
    config.values = {"rest"};
    config.exclude = true;
    MarkerFilter filter(config);

    // "everything except the rest periods" — the other direction the immediate
    // use case needs.
    EXPECT_FALSE(filter.passes(cue(0, "rest")));
    EXPECT_TRUE(filter.passes(cue(0, "fist")));
}

TEST(MarkerFilterTest, CanMatchOnEventOrMarkerTypeRatherThanLabel)
{
    MarkerFilterConfig by_event;
    by_event.field = MarkerMatchField::Event;
    by_event.values = {"rising"};
    // A threshold's crossings carry the direction in `event`, not in `label`,
    // so filtering by event is what makes them selectable at all.
    EXPECT_TRUE(MarkerFilter(by_event).passes(cue(0, "emg-onset", "rising")));
    EXPECT_FALSE(MarkerFilter(by_event).passes(cue(0, "emg-onset", "falling")));

    MarkerFilterConfig by_type;
    by_type.field = MarkerMatchField::MarkerType;
    by_type.values = {"threshold"};
    EXPECT_FALSE(MarkerFilter(by_type).passes(cue(0, "fist")));
}

// ⚠️ An unconfigured filter PASSES everything. Blocking instead would look
// exactly like a dead upstream, which on this lane is the failure mode hardest
// to distinguish from a real one.
TEST(MarkerFilterTest, AnEmptyValueListIsAPassThroughNotABlock)
{
    MarkerFilterConfig config;
    config.field = MarkerMatchField::Label;
    EXPECT_TRUE(MarkerFilter(config).passes(cue(0, "anything")));

    config.exclude = true;
    EXPECT_TRUE(MarkerFilter(config).passes(cue(0, "anything")));
}

// --- debounce -------------------------------------------------------------

TEST(MarkerDebounceTest, SuppressesMarkersInsideTheWindowAndCountsThem)
{
    MarkerDebounce debounce(1'000);  // 1 ms

    EXPECT_TRUE(debounce.admit(0));
    EXPECT_FALSE(debounce.admit(200));
    EXPECT_FALSE(debounce.admit(900));
    EXPECT_TRUE(debounce.admit(1'000));  // exactly at the boundary: admitted
    EXPECT_FALSE(debounce.admit(1'500));
    EXPECT_TRUE(debounce.admit(2'000));
    EXPECT_EQ(debounce.suppressed(), 3U);
}

// ⚠️ The window runs from the last marker PASSED, not the last one SEEN.
// Measuring from the last seen marker would let a dense burst extend the
// suppression indefinitely, and the operator would pass exactly one marker and
// then go silent for ever — a bug that looks like a dead upstream.
TEST(MarkerDebounceTest, ADenseBurstDoesNotExtendSuppressionForEver)
{
    MarkerDebounce debounce(1'000);
    ASSERT_TRUE(debounce.admit(0));

    // A hundred markers 100 us apart: all suppressed, but the window is still
    // measured from t=0, so t=1'000 is admitted on schedule.
    for (uint64_t at = 100; at < 1'000; at += 100) {
        EXPECT_FALSE(debounce.admit(at)) << "at " << at;
    }
    EXPECT_TRUE(debounce.admit(1'000));
}

TEST(MarkerDebounceTest, AZeroWindowIsAPassThrough)
{
    MarkerDebounce debounce(0);
    EXPECT_TRUE(debounce.admit(0));
    EXPECT_TRUE(debounce.admit(0));
    EXPECT_TRUE(debounce.admit(1));
    EXPECT_EQ(debounce.suppressed(), 0U);
}

// --- merge ----------------------------------------------------------------

std::vector<uint64_t> drainMerge(MarkerMerge<MarkerEventView>& merge)
{
    std::vector<uint64_t> out;
    while (const auto marker = merge.tryEmit()) {
        out.push_back(marker->atUs);
    }
    return out;
}

// combine's marker lane interleaves by ARRIVAL; this orders by the markers' own
// emitted_at_us, which is the difference between a timeline you can reason about
// and one that reflects network scheduling.
TEST(MarkerMergeTest, OrdersByEmittedTimeNotByArrival)
{
    MarkerMerge<MarkerEventView> merge(2);

    // Lane 1 arrives first but is timestamped later.
    merge.push(1, cue(3'000, "b1"), 3'000);
    merge.push(1, cue(5'000, "b2"), 5'000);
    merge.push(0, cue(1'000, "a1"), 1'000);
    merge.push(0, cue(4'000, "a2"), 4'000);

    EXPECT_EQ(drainMerge(merge), (std::vector<uint64_t>{1'000, 3'000, 4'000}));
    // 5'000 is beyond lane 0's high-water mark of 4'000 and is correctly held:
    // lane 0 could still deliver something at 4'500.
}

TEST(MarkerMergeTest, HoldsEverythingUntilEveryLaneHasSpokenOnce)
{
    MarkerMerge<MarkerEventView> merge(2);
    for (uint64_t at = 1'000; at <= 10'000; at += 1'000) {
        merge.push(0, cue(at, "a"), at);
    }
    // Nothing can be ordered against a lane that has never produced.
    EXPECT_TRUE(drainMerge(merge).empty());

    merge.push(1, cue(6'000, "b"), 6'000);
    // Everything up to lane 1's high-water mark, and note SEVEN events for two
    // lanes: 6'000 appears twice because this is a merge, so lane 1's own marker
    // is forwarded alongside lane 0's. Ties break by lane index.
    EXPECT_EQ(
        drainMerge(merge),
        (std::vector<uint64_t>{1'000, 2'000, 3'000, 4'000, 5'000, 6'000, 6'000}));
}

// --- take-until -----------------------------------------------------------

std::vector<std::string> drainTakeUntil(
    MarkerTakeUntil<MarkerEventView>& op)
{
    std::vector<std::string> out;
    while (const auto marker = op.tryEmit()) {
        out.push_back(marker->label);
    }
    return out;
}

TEST(MarkerTakeUntilTest, PassesUntilTheStopMarkerThenStopsForGood)
{
    MarkerTakeUntil<MarkerEventView> op;

    op.pushPrimary(cue(1'000, "a"), 1'000);
    op.pushPrimary(cue(2'000, "b"), 2'000);
    op.pushPrimary(cue(4'000, "c"), 4'000);
    op.pushPrimary(cue(5'000, "d"), 5'000);
    op.pushStop(cue(3'000, "stop"), 3'000);
    // Lift the primary lane's mark so everything up to 5'000 is processable.
    op.pushStop(cue(9'000, "stop-again"), 9'000);

    EXPECT_EQ(drainTakeUntil(op), (std::vector<std::string>{"a", "b"}));
    EXPECT_TRUE(op.stopped());
    // c and d are timestamped after the stop and are dropped, loudly.
    EXPECT_EQ(op.droppedAfterStop(), 2U);
}

// ⚠️ THE QUESTION THE TICKET LEFT OPEN, and the reason the merge is underneath
// this operator: "until" means BY TIMESTAMP, not by arrival. A marker
// timestamped before the stop passes even though it arrived after it. Deciding
// on arrival would make the cut depend on network scheduling, so replaying the
// same recording would cut somewhere else.
TEST(MarkerTakeUntilTest, AMarkerTimestampedBeforeTheStopPassesEvenIfItArrivesAfter)
{
    MarkerTakeUntil<MarkerEventView> op;

    op.pushStop(cue(3'000, "stop"), 3'000);
    // Arrives after the stop marker, but happened before it.
    op.pushPrimary(cue(2'500, "late-arrival-early-event"), 2'500);
    op.pushPrimary(cue(8'000, "genuinely-after"), 8'000);

    EXPECT_EQ(
        drainTakeUntil(op),
        (std::vector<std::string>{"late-arrival-early-event"}));
    EXPECT_TRUE(op.stopped());

    // The 8'000 marker is NOT dropped yet — it is still held. The stop lane's
    // high-water mark is only 3'000, so another stop marker at 4'000 could
    // still arrive, and dropping now would be deciding on information the
    // operator does not have.
    EXPECT_EQ(op.droppedAfterStop(), 0U);

    // Advancing the stop lane past it resolves the hold, and only then is it
    // dropped.
    op.pushStop(cue(9'000, "stop-again"), 9'000);
    EXPECT_TRUE(drainTakeUntil(op).empty());
    EXPECT_EQ(op.droppedAfterStop(), 1U);
}

TEST(MarkerTakeUntilTest, TheStopMarkerItselfIsNotForwarded)
{
    MarkerTakeUntil<MarkerEventView> op;
    op.pushPrimary(cue(1'000, "a"), 1'000);
    op.pushStop(cue(2'000, "stop"), 2'000);
    op.pushStop(cue(3'000, "stop2"), 3'000);

    const auto passed = drainTakeUntil(op);
    EXPECT_EQ(passed, (std::vector<std::string>{"a"}));
    for (const auto& label : passed) {
        EXPECT_NE(label, "stop");
    }
}

// --- the contract ---------------------------------------------------------

// The marker-lane counterpart of CombineJoinContract.OutputIsIndependentOfFeedRate.
// Same input events, two different interleavings of push and drain: the output
// must be identical, or a live run and a replay of its own recording disagree.
TEST(MarkerOpsContract, MergeOutputIsIndependentOfFeedInterleaving)
{
    const std::vector<std::pair<size_t, uint64_t>> script{
        {0, 1'000}, {1, 1'500}, {0, 2'000}, {1, 2'500}, {0, 3'000}, {1, 3'500}};

    MarkerMerge<MarkerEventView> all_at_once(2);
    for (const auto& entry : script) {
        all_at_once.push(entry.first, cue(entry.second, "x"), entry.second);
    }
    const auto batched = drainMerge(all_at_once);

    MarkerMerge<MarkerEventView> one_at_a_time(2);
    std::vector<uint64_t> streamed;
    for (const auto& entry : script) {
        one_at_a_time.push(entry.first, cue(entry.second, "x"), entry.second);
        for (const auto at : drainMerge(one_at_a_time)) {
            streamed.push_back(at);
        }
    }

    EXPECT_EQ(batched, streamed);
    EXPECT_FALSE(batched.empty());
}

TEST(MarkerOpsContract, DebounceReadsMarkerTimeSoReplaySpeedCannotChangeIt)
{
    // The same marker times fed with wildly different real-world spacing must
    // produce the same admissions. A wall-clock debounce would pass all of these
    // when fed slowly and suppress most when fed fast.
    const std::vector<uint64_t> times{0, 500, 1'000, 1'200, 2'500, 2'600, 5'000};

    std::vector<bool> first;
    MarkerDebounce a(1'000);
    for (const auto at : times) {
        first.push_back(a.admit(at));
    }

    std::vector<bool> second;
    MarkerDebounce b(1'000);
    for (const auto at : times) {
        second.push_back(b.admit(at));
    }

    EXPECT_EQ(first, second);
    EXPECT_EQ(first, (std::vector<bool>{true, false, true, false, true, false, true}));
}

// --- the match-value list -------------------------------------------------

TEST(MarkerMatchValuesTest, SplitsOnCommasAndTrimsWhatAPersonActuallyTypes)
{
    using nat::tools::parseMarkerMatchValues;

    EXPECT_EQ(
        parseMarkerMatchValues("fist,pinch"),
        (std::vector<std::string>{"fist", "pinch"}));
    // The spacing a person types. Treating " pinch" as a distinct label would
    // read as a bug, not as strictness.
    EXPECT_EQ(
        parseMarkerMatchValues("fist, pinch ,  open_hand"),
        (std::vector<std::string>{"fist", "pinch", "open_hand"}));
    // Empty entries are discarded rather than becoming a value that matches a
    // marker with an empty label.
    EXPECT_EQ(
        parseMarkerMatchValues("fist,,pinch,"),
        (std::vector<std::string>{"fist", "pinch"}));
    EXPECT_TRUE(parseMarkerMatchValues("").empty());
    EXPECT_TRUE(parseMarkerMatchValues("   ").empty());
    EXPECT_TRUE(parseMarkerMatchValues(",,,").empty());
    // A single value with internal spaces is one value; only commas separate.
    EXPECT_EQ(
        parseMarkerMatchValues("  open hand  "),
        (std::vector<std::string>{"open hand"}));
}

}  // namespace
