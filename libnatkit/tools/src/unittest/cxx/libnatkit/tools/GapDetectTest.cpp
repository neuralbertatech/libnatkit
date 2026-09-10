#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include <libnatkit/tools/GapDetect.hpp>

namespace {

using nat::tools::GapCause;
using nat::tools::GapConfig;
using nat::tools::GapDetector;

// 1 kHz, 10 samples per frame -> a frame every 10 ms, spanning 9 ms of samples.
constexpr uint32_t kRate = 1'000;
constexpr size_t kPerFrame = 10;
constexpr uint64_t kFramePeriodUs = 10'000;

GapConfig withGap(uint64_t gap_us)
{
    GapConfig config;
    config.gapUs = gap_us;
    return config;
}

// Feeds `count` back-to-back frames starting at `first_ts`, returning any gaps.
std::vector<nat::tools::GapEvent> feed(
    GapDetector& detector, uint64_t first_ts, uint64_t first_seq, int count)
{
    std::vector<nat::tools::GapEvent> events;
    for (int index = 0; index < count; ++index) {
        auto event = detector.push(
            first_ts + static_cast<uint64_t>(index) * kFramePeriodUs,
            kRate, kPerFrame, first_seq + static_cast<uint64_t>(index));
        if (event.has_value()) events.push_back(event.value());
    }
    return events;
}

TEST(GapDetectTest, AContiguousStreamReportsNothing)
{
    GapDetector detector(withGap(50'000));
    EXPECT_TRUE(feed(detector, 1'000'000, 0, 40).empty());
    EXPECT_EQ(detector.detected(), 0U);
}

// ⚠️ The threshold is slack ON TOP of the frame cadence, not an absolute
// spacing. A 10 ms frame cadence must not trip a 50 ms threshold, or every
// stream would read as one continuous dropout.
TEST(GapDetectTest, TheNormalFrameCadenceIsNotAGap)
{
    GapDetector detector(withGap(1'000));  // 1 ms of slack, cadence is 10 ms
    EXPECT_TRUE(feed(detector, 0, 0, 20).empty());
}

// ⚠️ THE STAMP IS THE ONSET, not the recovery. Stamping the frame that ended
// the silence reports every dropout late by exactly the gap's own duration.
TEST(GapDetectTest, TheMarkerIsStampedWhereTheDataSTOPPED)
{
    GapDetector detector(withGap(50'000));
    feed(detector, 0, 0, 3);  // frames at 0, 10'000, 20'000; last sample 29'000

    // Next frame arrives 500 ms late, with contiguous sequence numbers.
    const auto event = detector.push(530'000, kRate, kPerFrame, 3);
    ASSERT_TRUE(event.has_value());
    // Silence began at the last sample of frame 2, not at 530'000.
    EXPECT_EQ(event->atUs, 29'000U);
    // Expected at 30'000, arrived at 530'000.
    EXPECT_EQ(event->gapUs, 500'000U);
}

TEST(GapDetectTest, AGapBelowTheThresholdIsIgnored)
{
    GapDetector detector(withGap(50'000));
    feed(detector, 0, 0, 3);
    // 20 ms late — real jitter, not a dropout.
    EXPECT_FALSE(detector.push(50'000, kRate, kPerFrame, 3).has_value());
    EXPECT_EQ(detector.detected(), 0U);
}

// --- what the sequence numbers say ---------------------------------------

// The distinction the stream contract's seq_no promise exists to provide, and
// the two cases want different responses: lost frames are a transport problem,
// a pause is usually somebody stopping the sensor.
TEST(GapDetectTest, AJumpInSeqNoMeansFramesWereLOST)
{
    GapDetector detector(withGap(50'000));
    feed(detector, 0, 0, 3);  // seq 0,1,2

    // 500 ms of silence, and seq has advanced by 51 — so 50 frames existed and
    // never arrived.
    const auto event = detector.push(530'000, kRate, kPerFrame, 53);
    ASSERT_TRUE(event.has_value());
    EXPECT_EQ(event->cause, GapCause::FramesLost);
    EXPECT_EQ(event->framesLost, 50U);
    EXPECT_EQ(event->seqBefore, 2U);
    EXPECT_EQ(event->seqAfter, 53U);
    EXPECT_STREQ(nat::tools::gapCauseEvent(event->cause), "gap_lost");
}

TEST(GapDetectTest, AContiguousSeqNoMeansTheProducerPAUSED)
{
    GapDetector detector(withGap(50'000));
    feed(detector, 0, 0, 3);

    const auto event = detector.push(530'000, kRate, kPerFrame, 3);
    ASSERT_TRUE(event.has_value());
    EXPECT_EQ(event->cause, GapCause::Paused);
    EXPECT_EQ(event->framesLost, 0U);
    EXPECT_STREQ(nat::tools::gapCauseEvent(event->cause), "gap_paused");
}

// ⚠️ A REBOOT IS NOT A DROPOUT. seq_no going backwards is an epoch change --
// firmware counts it separately as seq_restarts -- and reporting a gap for it
// would be reporting the wrong event entirely, on a stream that is in fact
// healthy again.
TEST(GapDetectTest, ASequenceRESTARTIsNotReportedAsAGap)
{
    GapDetector detector(withGap(50'000));
    feed(detector, 0, 100, 3);  // seq 100,101,102

    // The device reboots: a long silence AND seq back to 0.
    const auto event = detector.push(900'000, kRate, kPerFrame, 0);
    EXPECT_FALSE(event.has_value());
    EXPECT_EQ(detector.detected(), 0U);
    EXPECT_EQ(detector.restarts(), 1U);
}

TEST(GapDetectTest, TheFirstFrameCannotBeAGap)
{
    GapDetector detector(withGap(1));
    EXPECT_FALSE(detector.push(999'999'999, kRate, kPerFrame, 7).has_value());
}

TEST(GapDetectTest, SeveralGapsAreEachReportedOnce)
{
    GapDetector detector(withGap(50'000));
    feed(detector, 0, 0, 3);
    ASSERT_TRUE(detector.push(500'000, kRate, kPerFrame, 3).has_value());
    feed(detector, 510'000, 4, 3);
    ASSERT_TRUE(detector.push(1'000'000, kRate, kPerFrame, 7).has_value());
    EXPECT_EQ(detector.detected(), 2U);
}

// --- the contract --------------------------------------------------------

// docs/STREAM_CONTRACT.md clause 4: everything temporal reads device_ts_us. A
// gap is decided entirely from the timestamps, so the same frames produce the
// same gaps however fast they are fed -- which is what a wall-clock timeout
// could never manage, and the reason TEC-NATKIT-110 closed as it did.
TEST(GapDetectContract, FeedRateCannotChangeTheGapSet)
{
    struct Frame { uint64_t ts; uint64_t seq; };
    const std::vector<Frame> script{
        {0, 0}, {10'000, 1}, {20'000, 2},
        {600'000, 3},                       // a pause
        {610'000, 4}, {620'000, 5},
        {1'200'000, 90},                    // lost frames
    };

    auto run = [&](GapDetector& detector) {
        std::vector<std::pair<uint64_t, uint64_t>> out;
        for (const auto& f : script) {
            if (auto e = detector.push(f.ts, kRate, kPerFrame, f.seq)) {
                out.emplace_back(e->atUs, e->gapUs);
            }
        }
        return out;
    };

    GapDetector a(withGap(50'000));
    GapDetector b(withGap(50'000));
    const auto first = run(a);
    const auto second = run(b);

    EXPECT_EQ(first, second);
    ASSERT_EQ(first.size(), 2U);
    EXPECT_EQ(first[0].first, 29'000U);   // stamped at the onset
    EXPECT_EQ(first[1].first, 629'000U);
}

}  // namespace
