#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include <libnatkit/tools/ThresholdDetect.hpp>

namespace {

using nat::tools::parseThresholdDirection;
using nat::tools::ThresholdConfig;
using nat::tools::ThresholdDetector;
using nat::tools::ThresholdDirection;

constexpr uint32_t kKilohertz = 1'000;  // 1 000 us between samples

ThresholdConfig rising(double level)
{
    ThresholdConfig config;
    config.level = level;
    config.direction = ThresholdDirection::Rising;
    return config;
}

TEST(ThresholdDirectionNames, RoundTripsAndRejectsUnknown)
{
    for (const auto direction :
         {ThresholdDirection::Rising, ThresholdDirection::Falling,
          ThresholdDirection::Either}) {
        const auto name = nat::tools::thresholdDirectionName(direction);
        const auto parsed = parseThresholdDirection(name);
        ASSERT_TRUE(parsed.has_value()) << name;
        EXPECT_EQ(parsed.value(), direction);
    }
    EXPECT_FALSE(parseThresholdDirection("up").has_value());
    EXPECT_FALSE(parseThresholdDirection("").has_value());
}

// ⚠️ THE REASON THIS NODE INTERPOLATES.
//
// The signal goes 0 -> 2 across one sample interval with the level at 1. The
// crossing did not happen at either sample: it happened halfway between them.
// Reporting the frame's timestamp (0) or the confirming sample's (1'000) would
// each be a 500 us error on a measurement whose entire purpose is timing.
TEST(ThresholdDetect, ReportsTheInterpolatedCrossingTimeNotASampleTime)
{
    ThresholdDetector detector(rising(1.0));
    const auto crossings = detector.push(0, kKilohertz, {0.0f, 2.0f});

    ASSERT_EQ(crossings.size(), 1U);
    EXPECT_EQ(crossings[0].atUs, 500U);
    EXPECT_TRUE(crossings[0].rising);
    EXPECT_DOUBLE_EQ(crossings[0].level, 1.0);
    EXPECT_DOUBLE_EQ(crossings[0].value, 2.0);
}

// The quantisation error the ticket is about, at the scale it actually occurs:
// a 100-sample frame at 1 kHz is one 100 ms frame, so stamping an onset with the
// frame's device_ts_us can be wrong by the better part of a tenth of a second.
TEST(ThresholdDetect, AnOnsetLateInAFrameIsNotStampedWithTheFramesTime)
{
    ThresholdDetector detector(rising(1.0));
    std::vector<float> samples(100, 0.0f);
    for (size_t index = 90; index < samples.size(); ++index) {
        samples[index] = 5.0f;
    }

    const auto crossings = detector.push(0, kKilohertz, samples);
    ASSERT_EQ(crossings.size(), 1U);
    // Between sample 89 (0.0) and sample 90 (5.0): 89'000 + 0.2 * 1'000.
    EXPECT_EQ(crossings[0].atUs, 89'200U);
}

TEST(ThresholdDetect, ASignalAlreadyPastTheLevelIsNotACrossing)
{
    ThresholdDetector detector(rising(1.0));
    EXPECT_TRUE(detector.push(0, kKilohertz, {5.0f, 5.0f, 5.0f}).empty());
}

// State survives the frame boundary, so an onset that straddles two frames is
// still one onset. Detecting per frame in isolation would miss every crossing
// that happens to land on a frame edge.
TEST(ThresholdDetect, DetectsACrossingThatStraddlesTwoFrames)
{
    ThresholdDetector detector(rising(1.0));
    EXPECT_TRUE(detector.push(0, kKilohertz, {0.0f}).empty());

    const auto crossings = detector.push(1'000, kKilohertz, {5.0f});
    ASSERT_EQ(crossings.size(), 1U);
    EXPECT_EQ(crossings[0].atUs, 200U);
}

// --- dwell ----------------------------------------------------------------

TEST(ThresholdDetectDwell, ASingleNoisySampleDoesNotFire)
{
    ThresholdConfig config = rising(1.0);
    config.dwellUs = 3'000;
    ThresholdDetector detector(config);

    // One sample spikes to 5 and the signal drops straight back.
    const auto crossings =
        detector.push(0, kKilohertz, {0.0f, 5.0f, 0.0f, 0.0f, 0.0f});
    EXPECT_TRUE(crossings.empty());
    EXPECT_EQ(detector.candidatesAbandoned(), 1U);
}

// ⚠️ The marker is stamped at the ONSET, not at the moment the dwell elapsed.
// Stamping the confirmation would report every onset 3 ms late here, and by
// whatever the dwell happens to be set to in general — which would make the
// dwell a systematic bias on the measurement instead of a noise filter.
TEST(ThresholdDetectDwell, ASustainedOnsetFiresStampedAtTheCrossing)
{
    ThresholdConfig config = rising(1.0);
    config.dwellUs = 3'000;
    ThresholdDetector detector(config);

    const auto crossings =
        detector.push(0, kKilohertz, {0.0f, 5.0f, 5.0f, 5.0f, 5.0f});
    ASSERT_EQ(crossings.size(), 1U);
    EXPECT_EQ(crossings[0].atUs, 200U);  // the crossing, not 4'000
    EXPECT_EQ(detector.candidatesAbandoned(), 0U);
}

// --- refractory -----------------------------------------------------------

TEST(ThresholdDetectRefractory, SuppressesRepeatFiringAndCountsWhatItSuppressed)
{
    ThresholdConfig config = rising(1.0);
    config.refractoryUs = 5'000;
    ThresholdDetector detector(config);

    // A signal chattering across the level once per two samples: five rising
    // crossings, at 200, 2'200, 4'200, 6'200 and 8'200 us.
    const auto crossings = detector.push(
        0, kKilohertz,
        {0.0f, 5.0f, 0.0f, 5.0f, 0.0f, 5.0f, 0.0f, 5.0f, 0.0f, 5.0f});

    // Fires at 200, then suppresses until 5'200; the next admissible crossing is
    // 6'200, which then suppresses until 11'200.
    ASSERT_EQ(crossings.size(), 2U);
    EXPECT_EQ(crossings[0].atUs, 200U);
    EXPECT_EQ(crossings[1].atUs, 6'200U);
    EXPECT_EQ(detector.suppressedByRefractory(), 3U);
}

// Without a refractory period this is what the same signal produces — which is
// why the guard is folded into this node rather than left to a separate
// debounce node the common case would always need.
TEST(ThresholdDetectRefractory, WithoutItEveryChatterCrossingFires)
{
    ThresholdDetector detector(rising(1.0));
    const auto crossings = detector.push(
        0, kKilohertz,
        {0.0f, 5.0f, 0.0f, 5.0f, 0.0f, 5.0f, 0.0f, 5.0f, 0.0f, 5.0f});
    EXPECT_EQ(crossings.size(), 5U);
    EXPECT_EQ(detector.suppressedByRefractory(), 0U);
}

// --- direction ------------------------------------------------------------

TEST(ThresholdDetectDirection, FallingIgnoresRisingCrossings)
{
    ThresholdConfig config = rising(1.0);
    config.direction = ThresholdDirection::Falling;
    ThresholdDetector detector(config);

    const auto crossings =
        detector.push(0, kKilohertz, {5.0f, 0.0f, 5.0f, 0.0f});
    ASSERT_EQ(crossings.size(), 2U);
    EXPECT_EQ(crossings[0].atUs, 800U);
    EXPECT_EQ(crossings[1].atUs, 2'800U);
    EXPECT_FALSE(crossings[0].rising);
    EXPECT_FALSE(crossings[1].rising);
}

TEST(ThresholdDetectDirection, EitherReportsBothAndSaysWhichWasWhich)
{
    ThresholdConfig config = rising(1.0);
    config.direction = ThresholdDirection::Either;
    ThresholdDetector detector(config);

    const auto crossings = detector.push(0, kKilohertz, {0.0f, 5.0f, 0.0f});
    ASSERT_EQ(crossings.size(), 2U);
    EXPECT_TRUE(crossings[0].rising);
    EXPECT_FALSE(crossings[1].rising);
}

// --- the contract ---------------------------------------------------------

// docs/STREAM_CONTRACT.md clause 4: everything temporal reads device_ts_us. The
// dwell and refractory timers are the tempting place to reach for a wall clock,
// and doing so would pass in real time and fail here. Feeding the identical
// samples with timestamps scaled as a 10x replay would must produce the same
// marker set, scaled — not a different one.
TEST(ThresholdDetectContract, ReplaySpeedCannotChangeTheMarkerSet)
{
    const std::vector<float> samples{0.0f, 5.0f, 5.0f, 5.0f, 0.0f,
                                     0.0f, 5.0f, 5.0f, 5.0f, 0.0f};
    ThresholdConfig config = rising(1.0);
    config.dwellUs = 2'000;
    config.refractoryUs = 3'000;

    // Same data, same declared rate, fed as one frame or as ten. The wall-clock
    // gap between pushes differs by orders of magnitude; the answer must not.
    ThresholdDetector single(config);
    const auto in_one_go = single.push(0, kKilohertz, samples);

    ThresholdDetector split(config);
    std::vector<uint64_t> piecemeal;
    for (size_t index = 0; index < samples.size(); ++index) {
        for (const auto& crossing : split.push(
                 static_cast<uint64_t>(index) * 1'000, kKilohertz,
                 {samples[index]})) {
            piecemeal.push_back(crossing.atUs);
        }
    }

    std::vector<uint64_t> whole;
    for (const auto& crossing : in_one_go) {
        whole.push_back(crossing.atUs);
    }
    EXPECT_EQ(whole, piecemeal);
    EXPECT_FALSE(whole.empty());
}

}  // namespace
