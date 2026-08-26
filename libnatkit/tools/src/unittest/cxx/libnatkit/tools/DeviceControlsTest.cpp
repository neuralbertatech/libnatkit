// The command gate (TEC-NATKIT-10).
//
// ⚠️ THE MIGRATION PATH IS THE DANGEROUS PART, not the happy path. Three leaves,
// the primary and the gateway on this rig run firmware that predates the control
// channel and will never advertise. If the gate refused them, it would silently
// remove every control that works today — the calibration node's buttons
// included — and it would look like the hardware had broken. So the default must
// let an unadvertised device through, and it must COUNT that, because the count
// is the evidence for when turning strict on is safe.

#include <gtest/gtest.h>

#include <libnatkit/tools/DeviceControls.hpp>

namespace {

using nat::tools::CommandGate;
using nat::tools::DeviceControlsService;

TEST(DeviceControlsGate, UnadvertisedDeviceIsAllowedAndCountedByDefault) {
    auto &service = DeviceControlsService::instance();
    service.setStrictMode(false);

    const auto before = service.unadvertisedAllowed();
    // A device id nothing has ever advertised for.
    EXPECT_EQ(service.check(999000111, "identify"), CommandGate::kAllowed)
        << "an un-flashed device must keep working during the migration";
    EXPECT_GT(service.unadvertisedAllowed(), before)
        << "and every such pass must be counted, or the decision to turn strict "
           "on is a guess";
}

TEST(DeviceControlsGate, StrictModeRefusesAnUnadvertisedDevice) {
    auto &service = DeviceControlsService::instance();
    service.setStrictMode(true);
    EXPECT_EQ(service.check(999000222, "identify"), CommandGate::kNoAdvertisement);
    // ⚠️ Left as we found it. This is a process-wide singleton, and a test that
    // leaves strict mode on would refuse commands for every test after it.
    service.setStrictMode(false);
}

TEST(DeviceControlsGate, AnUnknownDeviceHasAnEmptyButWellFormedSnapshot) {
    auto &service = DeviceControlsService::instance();
    const auto snap = service.forDevice(424242);
    EXPECT_EQ(snap.deviceId, 424242u) << "the snapshot names the device asked for";
    EXPECT_FALSE(snap.hasAdvertisement);
    EXPECT_FALSE(snap.reachable);
    EXPECT_TRUE(snap.controls.empty());
    // ⚠️ Zero, not a huge number. "Never heard" and "heard a very long time ago"
    // are different, and presenting the first as the second would put an absurd
    // age on screen for a device that simply has not spoken.
    EXPECT_EQ(snap.sinceHeartbeatMs, 0u);
}

TEST(DeviceControlsGate, EveryRefusalHasItsOwnName) {
    // The reasons send you to opposite ends of the rig — "not advertised" is
    // firmware, "unreachable" is power or radio — so they must not collapse into
    // one message.
    EXPECT_STRNE(nat::tools::toString(CommandGate::kNotAdvertised),
                 nat::tools::toString(CommandGate::kUnreachable));
    EXPECT_STRNE(nat::tools::toString(CommandGate::kNoAdvertisement),
                 nat::tools::toString(CommandGate::kNotAdvertised));
    EXPECT_STREQ(nat::tools::toString(CommandGate::kAllowed), "allowed");
}

}  // namespace
