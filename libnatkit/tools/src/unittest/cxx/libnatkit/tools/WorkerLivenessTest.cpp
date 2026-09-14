#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include <libnatkit/tools/WorkerLiveness.hpp>

namespace {

using nat::tools::classifyGraphNodeStatusAt;
using nat::tools::kWorkerStallUs;
using nat::tools::silenceIsNormalKind;

// An arbitrary "now", far enough from zero that subtracting a few seconds stays
// positive. Every timestamp below is expressed relative to it, so the tests read
// as "N seconds ago" rather than as absolute microseconds.
constexpr uint64_t kNow = 1'000'000'000'000ULL;

constexpr uint64_t agoUs(uint64_t us)
{
    return kNow - us;
}

constexpr uint64_t kRecently = 500'000;          // well inside the stall window
constexpr uint64_t kLongAgo = kWorkerStallUs * 3;  // well outside it

// A node that has been up long enough that the `starting` grace has expired.
constexpr uint64_t kStartedLongAgo = kWorkerStallUs * 10;

std::string classify(
    uint64_t last_output_ago,
    uint64_t last_input_ago,
    bool silence_is_normal,
    bool has_input_signal = true,
    uint64_t started_ago = kStartedLongAgo)
{
    return classifyGraphNodeStatusAt(
        kNow,
        last_output_ago == 0 ? 0 : agoUs(last_output_ago),
        last_input_ago == 0 ? 0 : agoUs(last_input_ago),
        agoUs(started_ago),
        silence_is_normal,
        has_input_signal);
}

TEST(SilenceIsNormalKind, CoversEveryEventDrivenOperator)
{
    // Each of these stamps its heartbeat on the emit path only, so each one
    // reads stalled on a healthy stream given the right config.
    EXPECT_TRUE(silenceIsNormalKind("gap_detect"));
    EXPECT_TRUE(silenceIsNormalKind("threshold"));
    EXPECT_TRUE(silenceIsNormalKind("gate"));
    EXPECT_TRUE(silenceIsNormalKind("marker_merge"));
    EXPECT_TRUE(silenceIsNormalKind("marker_filter"));
    EXPECT_TRUE(silenceIsNormalKind("marker_debounce"));
    EXPECT_TRUE(silenceIsNormalKind("marker_take_until"));
}

TEST(SilenceIsNormalKind, ExcludesOperatorsThatOweOutputPerInput)
{
    // ⚠️ These two must NEVER be excused. A transform that stops emitting while
    // being fed is a wedged worker, and a combine that stops is the starved-input
    // case the marble strips exist to show. Excusing either would silence the
    // faults this whole mechanism is for.
    EXPECT_FALSE(silenceIsNormalKind("transform"));
    EXPECT_FALSE(silenceIsNormalKind("combine"));
    EXPECT_FALSE(silenceIsNormalKind("stream_source"));
    EXPECT_FALSE(silenceIsNormalKind("viewer"));
    EXPECT_FALSE(silenceIsNormalKind(""));
}

// ---------------------------------------------------------------------------
// The bug this exists to prevent, in both directions.
// ---------------------------------------------------------------------------

TEST(GraphNodeStatus, SilentDetectorOnALiveStreamIsLive)
{
    // The reported case: a gap detector watching a healthy 400 Hz feed. It has
    // emitted nothing for minutes, correctly, and used to read `stalled` — which
    // then made the WHOLE board read stalled.
    EXPECT_EQ(classify(kLongAgo, kRecently, true), "live");
}

TEST(GraphNodeStatus, SilentDetectorWhoseUpstreamDiedIsStalled)
{
    // ⚠️ THE OTHER HALF, and the reason a bare per-kind exemption is wrong.
    // Excusing the kind outright would report `live` here forever — a node at
    // its most confident when it is most wrong (TEC-NATKIT-123).
    EXPECT_EQ(classify(kLongAgo, kLongAgo, true), "stalled");
}

TEST(GraphNodeStatus, SilenceIsNormalNodeThatJustEmittedIsLive)
{
    // A marker operator draining a burst can out-live its input by a moment.
    EXPECT_EQ(classify(kRecently, kLongAgo, true), "live");
}

// ---------------------------------------------------------------------------
// Kinds that owe an output keep the old, stricter rule.
// ---------------------------------------------------------------------------

TEST(GraphNodeStatus, FedButSilentTransformIsStillStalled)
{
    // The wedged-worker case. Input arriving, nothing coming out: a real fault,
    // and it must survive this change untouched.
    EXPECT_EQ(classify(kLongAgo, kRecently, false), "stalled");
}

TEST(GraphNodeStatus, EmittingTransformIsLive)
{
    EXPECT_EQ(classify(kRecently, kRecently, false), "live");
}

TEST(GraphNodeStatus, NodeWithNoInputLanesIsJudgedOnItsOutputAlone)
{
    // A plain transform reports no per-input activity, and so did every worker
    // before TEC-NATKIT-119. Without an input signal there is nothing to excuse
    // silence with, so the pre-existing behaviour stands — even for a kind whose
    // silence would otherwise be normal.
    EXPECT_EQ(classify(kLongAgo, 0, true, /*has_input_signal=*/false), "stalled");
    EXPECT_EQ(classify(kRecently, 0, true, /*has_input_signal=*/false), "live");
}

// ---------------------------------------------------------------------------
// Startup.
// ---------------------------------------------------------------------------

TEST(GraphNodeStatus, FreshlyStartedNodeIsStartingRatherThanStalled)
{
    // Without the grace every node reads `stalled` for the first three seconds
    // of every run, which is how a state earns being ignored.
    EXPECT_EQ(
        classify(0, 0, false, /*has_input_signal=*/false, /*started_ago=*/10'000),
        "starting");
    EXPECT_EQ(
        classify(0, 0, true, /*has_input_signal=*/true, /*started_ago=*/10'000),
        "starting");
}

TEST(GraphNodeStatus, TheGraceExpires)
{
    // Past the window with still nothing to show, it is a stall like any other.
    EXPECT_EQ(
        classify(0, 0, false, /*has_input_signal=*/false,
                 /*started_ago=*/kWorkerStallUs + 1),
        "stalled");
}

TEST(GraphNodeStatus, ANodeAlreadyBeingFedIsNotMerelyStarting)
{
    // Input is arriving, so there is something better to say than "starting".
    EXPECT_EQ(
        classify(0, kRecently, true, /*has_input_signal=*/true,
                 /*started_ago=*/10'000),
        "live");
}

// ---------------------------------------------------------------------------
// Boundary.
// ---------------------------------------------------------------------------

TEST(GraphNodeStatus, TheStallBoundaryIsInclusive)
{
    EXPECT_EQ(classify(kWorkerStallUs - 1, kLongAgo, false), "live");
    EXPECT_EQ(classify(kWorkerStallUs, kLongAgo, false), "stalled");

    EXPECT_EQ(classify(kLongAgo, kWorkerStallUs - 1, true), "live");
    EXPECT_EQ(classify(kLongAgo, kWorkerStallUs, true), "stalled");
}

TEST(GraphNodeStatus, AClockThatRunsBackwardsIsNotAStall)
{
    // A stamp from the future (a clock step, or a stamp taken microseconds after
    // `now` was read) must not underflow into an enormous age.
    EXPECT_EQ(
        classifyGraphNodeStatusAt(
            kNow, kNow + 5'000, kNow + 5'000, agoUs(kStartedLongAgo), false, true),
        "live");
}

}  // namespace
