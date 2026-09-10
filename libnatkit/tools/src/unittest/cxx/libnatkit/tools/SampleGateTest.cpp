#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include <libnatkit/tools/SampleGate.hpp>

namespace {

using nat::tools::GateEdgeMode;
using nat::tools::parseGateEdgeMode;
using nat::tools::SampleGate;
using nat::tools::SampleGateConfig;

// The gate needs three things from a frame; a stand-in supplies exactly those,
// which is what lets the windowing logic be tested without a broker.
struct Frame {
    uint64_t deviceTsUs = 0;
    uint32_t sampleRateHz = 1'000;  // 1 000 us between samples
    uint32_t samplesPerChannel = 0;
    int tag = 0;
};

Frame frameAt(uint64_t ts, uint32_t count, int tag = 0)
{
    return Frame{ts, 1'000, count, tag};
}

SampleGateConfig withMode(GateEdgeMode mode)
{
    SampleGateConfig config;
    config.edgeMode = mode;
    return config;
}

struct Span {
    size_t firstSample;
    size_t sampleCount;
    uint64_t startTsUs;

    bool operator==(const Span& other) const
    {
        return firstSample == other.firstSample &&
            sampleCount == other.sampleCount && startTsUs == other.startTsUs;
    }
};

std::vector<Span> drain(SampleGate<Frame>& gate)
{
    std::vector<Span> spans;
    while (const auto span = gate.tryEmit()) {
        spans.push_back(
            Span{span->firstSample, span->sampleCount, span->startTsUs});
    }
    return spans;
}

TEST(GateEdgeModeNames, RoundTripsAndRejectsUnknown)
{
    for (const auto mode :
         {GateEdgeMode::SplitAtSample, GateEdgeMode::PassWholeFrame,
          GateEdgeMode::DropPartialFrame}) {
        const auto name = nat::tools::gateEdgeModeName(mode);
        const auto parsed = parseGateEdgeMode(name);
        ASSERT_TRUE(parsed.has_value()) << name;
        EXPECT_EQ(parsed.value(), mode);
    }
    EXPECT_FALSE(parseGateEdgeMode("split").has_value());
    EXPECT_FALSE(parseGateEdgeMode("").has_value());
}

TEST(SampleGateDefaults, DefaultsToSplittingAtTheSample)
{
    EXPECT_EQ(SampleGateConfig{}.edgeMode, GateEdgeMode::SplitAtSample);
}

// A gate with no markers yet passes NOTHING. Defaulting to open would silently
// admit everything recorded before the first cue, which is the opposite of what
// "only train on the cue windows" asks for.
TEST(SampleGate, PassesNothingBeforeAnyMarkerArrives)
{
    SampleGate<Frame> gate(SampleGateConfig{});
    gate.pushFrame(frameAt(0, 10));
    EXPECT_TRUE(drain(gate).empty());
    EXPECT_FALSE(gate.isOpen());
}

// ⚠️ THE POINT OF THE NODE. The window opens at 3 ms and closes at 7 ms, inside
// a single 10 ms frame. Rounding to the frame boundary would admit all ten
// samples — six of them from outside the cue — and every training set built
// from it would carry that contamination invisibly.
TEST(SampleGate, SplitsAFrameAtTheSampleRatherThanTheFrameBoundary)
{
    SampleGate<Frame> gate(SampleGateConfig{});
    gate.pushMarker(3'000, /*opens=*/true);
    gate.pushMarker(7'000, /*opens=*/false);
    gate.pushMarker(20'000, /*opens=*/true);  // lifts the watermark past the frame
    gate.pushFrame(frameAt(0, 10));

    const auto spans = drain(gate);
    ASSERT_EQ(spans.size(), 1U);
    // Samples at 3'000, 4'000, 5'000 and 6'000 us. The sample at 7'000 is the
    // close instant itself and is excluded — the window is half-open.
    EXPECT_EQ(spans[0], (Span{3, 4, 3'000}));
    EXPECT_EQ(gate.samplesAdmitted(), 4U);
    EXPECT_EQ(gate.samplesRejected(), 6U);
}

// Two cues inside one frame must come out as two spans. Anything that assumed
// one contiguous run per frame would silently merge the gap between them into
// the output.
TEST(SampleGate, AWindowThatOpensAndClosesTwiceInOneFrameYieldsTwoSpans)
{
    SampleGate<Frame> gate(SampleGateConfig{});
    gate.pushMarker(1'000, true);
    gate.pushMarker(3'000, false);
    gate.pushMarker(5'000, true);
    gate.pushMarker(7'000, false);
    gate.pushMarker(20'000, true);
    gate.pushFrame(frameAt(0, 10));

    const auto spans = drain(gate);
    ASSERT_EQ(spans.size(), 2U);
    EXPECT_EQ(spans[0], (Span{1, 2, 1'000}));
    EXPECT_EQ(spans[1], (Span{5, 2, 5'000}));
    EXPECT_EQ(gate.samplesAdmitted(), 4U);
}

TEST(SampleGate, RepeatedOpensAndAStrayCloseAreBothHarmless)
{
    SampleGate<Frame> gate(SampleGateConfig{});
    gate.pushMarker(500, false);  // a close with nothing open: ignored
    gate.pushMarker(2'000, true);
    gate.pushMarker(3'000, true);  // already open: must not restart the window
    gate.pushMarker(20'000, false);
    gate.pushFrame(frameAt(0, 10));

    const auto spans = drain(gate);
    ASSERT_EQ(spans.size(), 1U);
    EXPECT_EQ(spans[0], (Span{2, 8, 2'000}));
}

// --- the watermark --------------------------------------------------------

// ⚠️ Data and markers are two channels, so a marker timestamped inside a frame
// can ARRIVE after it. Answering immediately would make the output depend on
// inter-channel arrival skew — a live run would disagree with a replay of its
// own recording.
TEST(SampleGateWatermark, HoldsAFrameUntilMarkersHaveAdvancedPastIt)
{
    SampleGate<Frame> gate(SampleGateConfig{});
    gate.pushMarker(3'000, true);
    gate.pushFrame(frameAt(0, 10));  // last sample at 9'000, marker only at 3'000

    // Held, not answered: a close marker at 5'000 could still be in flight.
    EXPECT_TRUE(drain(gate).empty());

    gate.pushMarker(5'000, false);
    // Still held: 5'000 is under the frame's last sample time.
    EXPECT_TRUE(drain(gate).empty());

    gate.pushMarker(9'000, true);
    const auto spans = drain(gate);
    ASSERT_EQ(spans.size(), 2U);
    // The late close DID change the answer, which is exactly why waiting was
    // necessary: samples 3-4 are inside the first window, and sample 9 reopens.
    EXPECT_EQ(spans[0], (Span{3, 2, 3'000}));
    EXPECT_EQ(spans[1], (Span{9, 1, 9'000}));
}

// The analogue of CombineJoinContract.OutputIsIndependentOfFeedRate: the gate's
// answer must be a function of the timestamps, not of the interleaving.
TEST(SampleGateWatermark, OutputIsIndependentOfMarkerAndFrameInterleaving)
{
    const std::vector<std::pair<uint64_t, bool>> markers{
        {2'500, true}, {6'500, false}, {12'000, true}, {30'000, false}};
    const std::vector<Frame> frames{
        frameAt(0, 10, 1), frameAt(10'000, 10, 2), frameAt(20'000, 10, 3)};

    // Markers first, then every frame.
    SampleGate<Frame> markers_first(SampleGateConfig{});
    for (const auto& marker : markers) {
        markers_first.pushMarker(marker.first, marker.second);
    }
    for (const auto& frame : frames) {
        markers_first.pushFrame(frame);
    }
    const auto a = drain(markers_first);

    // Frames first, draining as we go, then the markers.
    SampleGate<Frame> frames_first(SampleGateConfig{});
    std::vector<Span> b;
    for (const auto& frame : frames) {
        frames_first.pushFrame(frame);
        for (const auto& span : drain(frames_first)) {
            b.push_back(span);
        }
    }
    for (const auto& marker : markers) {
        frames_first.pushMarker(marker.first, marker.second);
        for (const auto& span : drain(frames_first)) {
            b.push_back(span);
        }
    }

    EXPECT_EQ(a, b);
    EXPECT_FALSE(a.empty());
}

// --- edge modes -----------------------------------------------------------

TEST(SampleGateEdgeModes, PassWholeFrameAdmitsOutOfWindowSamplesOnPurpose)
{
    SampleGate<Frame> gate(withMode(GateEdgeMode::PassWholeFrame));
    gate.pushMarker(3'000, true);
    gate.pushMarker(7'000, false);
    gate.pushMarker(20'000, true);
    gate.pushFrame(frameAt(0, 10));

    const auto spans = drain(gate);
    ASSERT_EQ(spans.size(), 1U);
    EXPECT_EQ(spans[0], (Span{0, 10, 0}));
    // Ten admitted where only four were inside the cue. That is the documented
    // trade — uniform frame sizes downstream, contamination at the edges.
    EXPECT_EQ(gate.samplesAdmitted(), 10U);
}

TEST(SampleGateEdgeModes, DropPartialFrameLosesAStraddlingFrameEntirely)
{
    SampleGate<Frame> gate(withMode(GateEdgeMode::DropPartialFrame));
    gate.pushMarker(3'000, true);
    gate.pushMarker(7'000, false);
    gate.pushMarker(20'000, true);
    gate.pushFrame(frameAt(0, 10));

    EXPECT_TRUE(drain(gate).empty());
    EXPECT_EQ(gate.samplesAdmitted(), 0U);
    EXPECT_EQ(gate.samplesRejected(), 10U);
}

TEST(SampleGateEdgeModes, DropPartialFrameKeepsAFullyEnclosedFrame)
{
    SampleGate<Frame> gate(withMode(GateEdgeMode::DropPartialFrame));
    gate.pushMarker(0, true);
    gate.pushMarker(30'000, false);
    gate.pushFrame(frameAt(0, 10));

    const auto spans = drain(gate);
    ASSERT_EQ(spans.size(), 1U);
    EXPECT_EQ(spans[0], (Span{0, 10, 0}));
}

// Every mode must agree on the two unambiguous cases, or the mode is choosing
// more than it claims to.
TEST(SampleGateEdgeModes, AllModesAgreeWhenNothingStraddles)
{
    for (const auto mode :
         {GateEdgeMode::SplitAtSample, GateEdgeMode::PassWholeFrame,
          GateEdgeMode::DropPartialFrame}) {
        SampleGate<Frame> fully_open(withMode(mode));
        fully_open.pushMarker(0, true);
        fully_open.pushMarker(30'000, false);
        fully_open.pushFrame(frameAt(0, 10));
        EXPECT_EQ(drain(fully_open).size(), 1U)
            << "mode " << nat::tools::gateEdgeModeName(mode);

        SampleGate<Frame> fully_closed(withMode(mode));
        fully_closed.pushMarker(50'000, true);
        fully_closed.pushFrame(frameAt(0, 10));
        EXPECT_TRUE(drain(fully_closed).empty())
            << "mode " << nat::tools::gateEdgeModeName(mode);
    }
}

TEST(SampleGate, OverflowEvictsOldestFramesAndCountsThem)
{
    SampleGateConfig config;
    config.maxQueuedFrames = 4;
    SampleGate<Frame> gate(config);

    // No marker has arrived, so nothing can be answered and frames pile up.
    for (int index = 0; index < 10; ++index) {
        gate.pushFrame(frameAt(static_cast<uint64_t>(index) * 10'000, 10, index));
    }
    EXPECT_EQ(gate.framesDroppedOverflow(), 6U);
}

}  // namespace
