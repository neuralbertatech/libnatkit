// Tests for the part of device health that can lie without failing.
//
// Tailing a Kafka topic either works or does not. Differencing counters is where
// a wrong answer looks right: a rebooted leaf reported as the busiest device on
// the rig, a silent leaf reported as healthy because its last frame said so, or a
// rate of zero shown for a device we simply have no second sample for. Each of
// those is a case below.

#include <gtest/gtest.h>

#include <libnatkit/tools/DeviceHealth.hpp>

namespace {

using nat::tools::DeviceHealthTracker;
using nat::tools::RateStatus;

// A leaf frame with the fields the tracker reads. Everything else stays 0: this
// is about the arithmetic, and the LAYOUT is pinned by libnatkit-core's fixture
// test against a real capture, which is the right place for it.
nat::core::NatKitNodeStatusV1Schema leaf(const uint64_t deviceId,
                                         const uint64_t lastSeenUs,
                                         const uint32_t dataFrames,
                                         const uint32_t sendFailures = 0) {
    nat::core::NatKitNodeStatusV1Schema record;
    record.deviceId = deviceId;
    record.lastSeenUs = lastSeenUs;
    record.dataFrames = dataFrames;
    record.leafSendFailures = sendFailures;
    return record;
}

nat::core::NatKitPrimaryStatusV1Schema hub(const uint64_t deviceId,
                                           const uint64_t uptimeUs,
                                           const uint32_t framesSent) {
    nat::core::NatKitPrimaryStatusV1Schema record;
    record.deviceId = deviceId;
    record.uptimeUs = uptimeUs;
    record.framesSent = framesSent;
    return record;
}

const nat::tools::DeviceHealth &find(const std::vector<nat::tools::DeviceHealth> &all,
                                     const uint64_t deviceId) {
    for (const auto &entry : all) {
        if (entry.deviceId == deviceId) {
            return entry;
        }
    }
    throw std::runtime_error("device not in snapshot");
}

}  // namespace

// One sample cannot make a rate, and the panel must be told that rather than
// shown a zero.
TEST(DeviceHealthTest, FirstSampleHasNoRates) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);

    const auto snapshot = tracker.snapshot(5000);
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kFirstSample);
    EXPECT_TRUE(snapshot[0].rates.empty());
    EXPECT_TRUE(snapshot[0].toJson(3000)["rates"].is_null());
}

TEST(DeviceHealthTest, RateIsPerSecondOverTheDeviceClock) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    // Six seconds later on the device's clock, 270 more frames: 45/s.
    tracker.observeNode(leaf(1, 7000000, 370), 5500);

    const auto snapshot = tracker.snapshot(5500);
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kAvailable);
    EXPECT_DOUBLE_EQ(snapshot[0].rates.at("data_frames"), 45.0);
    EXPECT_EQ(snapshot[0].rateIntervalUs, 6000000u);
}

// The behaviour the rate window buys, and the reason it exists: a leaf's
// frames_built reaches the hub on the LEAF's heartbeat, out of step with the
// hub's 1 Hz status publish. Differenced tick by tick it reads 0/s then 20/s for
// a counter that is really 10/s -- observed on the live rig. Until a window's
// worth of history exists, there is no rate to report.
TEST(DeviceHealthTest, ATickIsNotAWindow) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    tracker.observeNode(leaf(1, 2000000, 110), 6000);

    auto snapshot = tracker.snapshot(6000);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kWindowTooShort);
    EXPECT_TRUE(snapshot[0].toJson(3000)["rates"].is_null());
    // ... and the figures themselves are still current throughout.
    EXPECT_EQ(snapshot[0].fields.at("data_frames").get<uint64_t>(), 110u);

    // Once the window is spanned, the rate appears -- and it is the average over
    // the whole window, which is what makes it immune to the aliasing.
    tracker.observeNode(leaf(1, 4000000, 130), 8000);
    tracker.observeNode(leaf(1, 6100000, 151), 10000);
    snapshot = tracker.snapshot(10000);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kAvailable);
    EXPECT_GE(snapshot[0].rateIntervalUs, 5000000u);
    EXPECT_DOUBLE_EQ(snapshot[0].rates.at("data_frames"), 10.0);
}

// ⚠️ The wall clock between the two frames was 500 ms; the device clock says six
// seconds. Using the wall clock would report twelve times the truth -- and it
// would be wrong in exactly the situation that produces a backlog.
TEST(DeviceHealthTest, WallClockJitterDoesNotSkewTheRate) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    tracker.observeNode(leaf(1, 7000000, 370), 5500);

    EXPECT_DOUBLE_EQ(tracker.snapshot(5500)[0].rates.at("data_frames"), 45.0);
}

// The case that would otherwise make a rebooting board look like the healthiest
// thing on the rig.
TEST(DeviceHealthTest, RebootSuppressesRatesInsteadOfInventingAHugeOne) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 900000000, 500000), 5000);
    // Power-cycled: the clock restarted, and so did every counter.
    tracker.observeNode(leaf(1, 1000000, 40), 6000);

    const auto snapshot = tracker.snapshot(6000);
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kRebooted);
    EXPECT_TRUE(snapshot[0].rates.empty());
    EXPECT_TRUE(snapshot[0].toJson(3000)["rates"].is_null());
    // The fields must still be the NEW ones: a rebooted device is not an
    // unknown device, and its post-reboot counters are the current truth.
    EXPECT_EQ(snapshot[0].fields.at("data_frames").get<uint64_t>(), 40u);
}

// And it recovers. The frame that REVEALED the reboot becomes the new baseline,
// so recovery costs a window and not a window plus a frame -- post-reboot is
// exactly when somebody is watching to see whether the board came back healthy.
TEST(DeviceHealthTest, RatesResumeAfterAReboot) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 900000000, 500000), 5000);
    tracker.observeNode(leaf(1, 1000000, 40), 6000);
    EXPECT_EQ(tracker.snapshot(6000)[0].rateStatus, RateStatus::kRebooted);

    // A second post-reboot frame, but not yet a window's worth.
    tracker.observeNode(leaf(1, 2000000, 85), 7000);
    EXPECT_EQ(tracker.snapshot(7000)[0].rateStatus, RateStatus::kWindowTooShort);

    // Six seconds after the reboot baseline: 270 frames on from 40, so 45/s --
    // and differenced against the post-reboot baseline, not the pre-reboot one.
    tracker.observeNode(leaf(1, 7000000, 310), 11000);
    const auto snapshot = tracker.snapshot(11000);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kAvailable);
    EXPECT_DOUBLE_EQ(snapshot[0].rates.at("data_frames"), 45.0);
}

// The same frame twice is not an interval. Dividing by zero here would give inf,
// which JSON cannot even represent.
TEST(DeviceHealthTest, RepeatedFrameIsNotAnInterval) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    tracker.observeNode(leaf(1, 1000000, 100), 5100);

    const auto snapshot = tracker.snapshot(5100);
    EXPECT_EQ(snapshot[0].rateStatus, RateStatus::kClockNotAdvanced);
    EXPECT_TRUE(snapshot[0].toJson(3000)["rates"].is_null());
}

// The failure the whole panel exists to make visible: a leaf that stopped
// sending. Its last frame is healthy and stays healthy forever, so the only
// signal is the absence of new frames.
TEST(DeviceHealthTest, ASilentDeviceGoesQuietAndStaysInTheSnapshot) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    tracker.observeNode(leaf(1, 7000000, 190), 6000);

    // One second on: still fresh.
    auto snapshot = tracker.snapshot(7000);
    EXPECT_EQ(snapshot[0].ageMs, 1000u);
    EXPECT_FALSE(snapshot[0].toJson(3000)["quiet"].get<bool>());

    // Thirty seconds on, with no new frame: quiet, still listed, and still
    // carrying its last-known figures.
    snapshot = tracker.snapshot(36000);
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].ageMs, 30000u);
    EXPECT_TRUE(snapshot[0].toJson(3000)["quiet"].get<bool>());
    EXPECT_EQ(snapshot[0].fields.at("data_frames").get<uint64_t>(), 190u);
}

// A system clock that steps backwards must not make a device look ancient.
TEST(DeviceHealthTest, AgeSaturatesRatherThanWrapping) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 10000);
    EXPECT_EQ(tracker.snapshot(9000)[0].ageMs, 0u);
}

TEST(DeviceHealthTest, HubSortsFirstAndLeavesSortByDeviceId) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(300, 1000000, 10), 5000);
    tracker.observeNode(leaf(100, 1000000, 10), 5000);
    tracker.observeHub(hub(999, 1000000, 10), 5000);
    tracker.observeNode(leaf(200, 1000000, 10), 5000);

    const auto snapshot = tracker.snapshot(5000);
    ASSERT_EQ(snapshot.size(), 4u);
    EXPECT_TRUE(snapshot[0].isHub);
    EXPECT_EQ(snapshot[0].deviceId, 999u);
    EXPECT_EQ(snapshot[1].deviceId, 100u);
    EXPECT_EQ(snapshot[2].deviceId, 200u);
    EXPECT_EQ(snapshot[3].deviceId, 300u);
}

// Each device is differenced against its own history, not against whichever
// frame arrived last.
TEST(DeviceHealthTest, DevicesDoNotShareHistory) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    tracker.observeNode(leaf(2, 1000000, 700000), 5000);
    tracker.observeNode(leaf(1, 7000000, 370), 6000);
    tracker.observeNode(leaf(2, 7000000, 700060), 6000);

    const auto snapshot = tracker.snapshot(6000);
    EXPECT_DOUBLE_EQ(find(snapshot, 1).rates.at("data_frames"), 45.0);
    EXPECT_DOUBLE_EQ(find(snapshot, 2).rates.at("data_frames"), 10.0);
}

// The counter a diagnosis usually turns on: a leaf whose radio is failing.
TEST(DeviceHealthTest, SendFailuresGetARate) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100, 0), 5000);
    tracker.observeNode(leaf(1, 7000000, 106, 12000), 6000);

    const auto snapshot = tracker.snapshot(6000);
    EXPECT_DOUBLE_EQ(snapshot[0].rates.at("leaf_send_failures"), 2000.0);
    // One frame a second while 2000 sends fail: the shape of TEC-NATKIT-50.
    EXPECT_DOUBLE_EQ(snapshot[0].rates.at("data_frames"), 1.0);
}

// ⚠️ frames_queued must NOT get a rate: it is racy and reads below frames_sent
// (TEC-NATKIT-75), so a rate from it would be authoritative-looking nonsense.
TEST(DeviceHealthTest, TheRacyHubCounterIsNotGivenARate) {
    DeviceHealthTracker tracker;
    auto first = hub(999, 1000000, 100);
    first.framesQueued = 100;
    auto second = hub(999, 7000000, 370);
    second.framesQueued = 330;  // undercounting, as the live rig does
    tracker.observeHub(first, 5000);
    tracker.observeHub(second, 6000);

    const auto snapshot = tracker.snapshot(6000);
    EXPECT_DOUBLE_EQ(snapshot[0].rates.at("frames_sent"), 45.0);
    EXPECT_EQ(snapshot[0].rates.count("frames_queued"), 0u);
    // It is still REPORTED -- suppressing the field would hide the defect -- just
    // never turned into a rate.
    EXPECT_EQ(snapshot[0].fields.at("frames_queued").get<uint64_t>(), 330u);
}

TEST(DeviceHealthTest, ForgettingDropsOnlyLongGoneDevices) {
    DeviceHealthTracker tracker;
    tracker.observeNode(leaf(1, 1000000, 100), 5000);
    tracker.observeNode(leaf(2, 1000000, 100), 100000);

    tracker.forgetOlderThan(100000, 60000);
    const auto snapshot = tracker.snapshot(100000);
    ASSERT_EQ(snapshot.size(), 1u);
    EXPECT_EQ(snapshot[0].deviceId, 2u);
}

// --- what a recording carries (TEC-NATKIT-77) ------------------------------
//
// These matter more than the live panel's arithmetic, because they are the only
// record that survives. The status frames age out of Kafka retention and the
// recording keeps no copy, so anything this gets wrong is wrong permanently and
// nobody can go back and check.

namespace {

nat::tools::DeviceHealth healthOf(const uint64_t deviceId, const nlohmann::json& fields,
                                  const uint64_t ageMs = 0) {
    nat::tools::DeviceHealth health;
    health.deviceId = deviceId;
    health.ageMs = ageMs;
    health.fields = fields;
    return health;
}

nlohmann::json syncFields(const int beaconsMissed, const int epoch,
                          const bool valid = true) {
    return nlohmann::json{
        {"data_frames", 1000},
        {"sync",
         {{"valid", valid},
          {"quality", 2},
          {"epoch", epoch},
          {"residual_rms_ns", 45767},
          {"peak_residual_ns", 138746},
          {"skew_ppb", 43265},
          {"beacons_missed", beaconsMissed}}}};
}

nlohmann::json fitsJson(const std::vector<nat::tools::RecordedClockFit>& fits) {
    nlohmann::json out = nlohmann::json::array();
    for (const auto& fit : fits) {
        out.push_back(fit.toJson());
    }
    return out;
}

}  // namespace

// ⚠️ The distinction the whole record hangs on. A device that never sent a status
// frame must not be written down as a valid fit full of zeroes.
TEST(RecordedClockFitTest, ADeviceThatNeverReportedSaysSo) {
    const auto fits = nat::tools::snapshotClockFits({}, {"13793649670644"}, /*watching=*/true);
    ASSERT_EQ(fits.size(), 1u);
    EXPECT_EQ(fits[0].status, "no_status_frames");
    EXPECT_TRUE(fits[0].fields.is_null());
    // null in the JSON too: {} would read as "we looked and it was all zero".
    EXPECT_TRUE(fits[0].toJson()["fields"].is_null());
}

TEST(RecordedClockFitTest, AReportingDeviceCarriesItsFields) {
    const std::vector<nat::tools::DeviceHealth> devices = {
        healthOf(13793649670644ULL, syncFields(100, 7))};
    const auto fits = nat::tools::snapshotClockFits(devices, {"13793649670644"}, /*watching=*/true);
    ASSERT_EQ(fits.size(), 1u);
    EXPECT_EQ(fits[0].status, "reported");
    EXPECT_EQ(fits[0].fields["sync"]["quality"].get<int>(), 2);
}

// A device that was reporting and has gone silent is neither of the other two.
TEST(RecordedClockFitTest, ASilentDeviceIsDistinctFromOneThatNeverReported) {
    const std::vector<nat::tools::DeviceHealth> devices = {
        healthOf(1ULL, syncFields(100, 7), /*ageMs=*/40000)};
    const auto fits = nat::tools::snapshotClockFits(devices, {"1"}, /*watching=*/true);
    EXPECT_EQ(fits[0].status, "went_quiet");
    // Its last-known fields are still recorded: they are the evidence.
    EXPECT_FALSE(fits[0].fields.is_null());
}

TEST(RecordedClockFitTest, EveryRequestedStreamGetsARow) {
    const std::vector<nat::tools::DeviceHealth> devices = {
        healthOf(1ULL, syncFields(100, 7))};
    const auto fits = nat::tools::snapshotClockFits(devices, {"1", "2", "3"}, /*watching=*/true);
    ASSERT_EQ(fits.size(), 3u);
    EXPECT_EQ(fits[0].status, "reported");
    EXPECT_EQ(fits[1].status, "no_status_frames");
    EXPECT_EQ(fits[2].status, "no_status_frames");
}

// ⚠️ A RATE over the run, not the since-boot total. Quoting the total would say
// "this run missed 3662 beacons" about a device that missed them yesterday.
TEST(ClockQualityTest, BeaconLossIsDifferencedOverTheRun) {
    const auto start = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(3600, 7))}, {"1"}, /*watching=*/true));
    const auto finish = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(3662, 7))}, {"1"}, /*watching=*/true));

    const auto quality = nat::tools::buildClockQuality(start, finish, /*runSeconds=*/124.0);
    ASSERT_EQ(quality["devices"].size(), 1u);
    const auto& device = quality["devices"][0];
    EXPECT_EQ(device["beacons_missed_in_run"].get<double>(), 62.0);
    EXPECT_NEAR(device["beacons_missed_per_s"].get<double>(), 0.5, 1e-9);
    EXPECT_EQ(quality["run_seconds"].get<double>(), 124.0);
}

// ⚠️ The single most important thing this record can say, and it is invisible in
// either endpoint alone: the fit was REBUILT mid-run, so timestamps before and
// after it sit on different fits.
TEST(ClockQualityTest, AnEpochChangeMidRunIsRecorded) {
    const auto start = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(10, 7))}, {"1"}, /*watching=*/true));
    const auto same = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(12, 7))}, {"1"}, /*watching=*/true));
    const auto changed = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(12, 8))}, {"1"}, /*watching=*/true));

    EXPECT_FALSE(nat::tools::buildClockQuality(start, same, 60.0)["devices"][0]
                     ["epoch_changed_during_run"]
                         .get<bool>());
    EXPECT_TRUE(nat::tools::buildClockQuality(start, changed, 60.0)["devices"][0]
                    ["epoch_changed_during_run"]
                        .get<bool>());
}

// A counter that went BACKWARDS means the device rebooted mid-run. No rate is
// reported rather than a negative or a wrapped one, and the row still exists.
TEST(ClockQualityTest, ARebootMidRunYieldsNoRateRatherThanNonsense) {
    const auto start = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(50000, 7))}, {"1"}, /*watching=*/true));
    const auto finish = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(12, 9))}, {"1"}, /*watching=*/true));

    const auto quality = nat::tools::buildClockQuality(start, finish, 60.0);
    const auto& device = quality["devices"][0];
    EXPECT_EQ(device.count("beacons_missed_per_s"), 0u);
    EXPECT_TRUE(device["epoch_changed_during_run"].get<bool>());
    EXPECT_EQ(device["device_id"].get<std::string>(), "1");
}

TEST(ClockQualityTest, AZeroLengthRunReportsNoRate) {
    const auto start = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(10, 7))}, {"1"}, /*watching=*/true));
    const auto finish = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(10, 7))}, {"1"}, /*watching=*/true));
    const auto quality = nat::tools::buildClockQuality(start, finish, 0.0);
    EXPECT_EQ(quality["devices"][0].count("beacons_missed_per_s"), 0u);
}

// A device with no status frames at either end still gets a row saying so — the
// row is the record that it was part of the run and could not be vouched for.
TEST(ClockQualityTest, ASilentDeviceStillGetsARow) {
    const auto start = fitsJson(nat::tools::snapshotClockFits({}, {"7"}, /*watching=*/true));
    const auto finish = fitsJson(nat::tools::snapshotClockFits({}, {"7"}, /*watching=*/true));
    const auto quality = nat::tools::buildClockQuality(start, finish, 60.0);
    ASSERT_EQ(quality["devices"].size(), 1u);
    EXPECT_EQ(quality["devices"][0]["status"].get<std::string>(), "no_status_frames");
    EXPECT_EQ(quality["devices"][0].count("valid"), 0u);
}

TEST(ClockQualityTest, AnInvalidFitIsRecordedAsInvalid) {
    const auto start = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(10, 7, /*valid=*/false))}, {"1"}, /*watching=*/true));
    const auto finish = fitsJson(nat::tools::snapshotClockFits({healthOf(1ULL, syncFields(20, 7, /*valid=*/false))}, {"1"}, /*watching=*/true));
    const auto quality = nat::tools::buildClockQuality(start, finish, 60.0);
    EXPECT_FALSE(quality["devices"][0]["valid"].get<bool>());
}

// ⚠️ The distinction that keeps the record honest about WHOSE fault an absence is.
// Lazily starting the tailer meant the first recording after a restart snapshotted
// an empty tracker, and every device in it was sealed as "no_status_frames" —
// permanently accusing the hardware of our own cold start.
TEST(RecordedClockFitTest, NotWatchingIsNotTheSameAsTheDeviceNotReporting) {
    const auto watching =
        nat::tools::snapshotClockFits({}, {"1"}, /*watching=*/true);
    const auto blind =
        nat::tools::snapshotClockFits({}, {"1"}, /*watching=*/false);
    EXPECT_EQ(watching[0].status, "no_status_frames");
    EXPECT_EQ(blind[0].status, "not_watching");
    // Neither invents a fit.
    EXPECT_TRUE(watching[0].fields.is_null());
    EXPECT_TRUE(blind[0].fields.is_null());
}

// And it survives into the sealed record rather than being smoothed away.
TEST(ClockQualityTest, NotWatchingSurvivesIntoTheRecord) {
    const auto blind = fitsJson(
        nat::tools::snapshotClockFits({}, {"1"}, /*watching=*/false));
    const auto quality = nat::tools::buildClockQuality(blind, blind, 60.0);
    EXPECT_EQ(quality["devices"][0]["status"].get<std::string>(), "not_watching");
}
