#pragma once

// Turning the rig's status frames into something a person can read.
//
// The hub publishes its own health and every leaf's once a second. The frames
// carry almost nothing but COUNTERS CUMULATIVE SINCE BOOT, and that is the whole
// problem this file exists for: a panel that prints them raw shows a wall of
// seven-digit numbers that all go up, in which a leaf that stopped sending an
// hour ago looks exactly like a healthy one. The reading a person wants is
//
//   * a RATE -- 45 frames/s, 0 frames/s -- which needs two samples differenced;
//   * whether the device is still talking AT ALL, which the frame cannot say,
//     because a device that has gone silent simply stops producing frames and
//     the last one it sent looks perfectly healthy forever;
//   * and, when it rebooted, to be told rather than shown a rate of minus four
//     million.
//
// So the tracker below holds the previous sample per device and answers those
// three questions. It is separated from the Kafka tailing on purpose: every way
// this can be WRONG lives here (a wrapped counter, a reboot, a stale sample
// quoted as current) and none of it needs a broker to test.

#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <vector>

#include <libnatkit-core.hpp>
#include <nlohmann/json.hpp>

namespace nat {
namespace tools {

// Why a device's rates are not being reported. Anything other than kAvailable
// means the panel must show a dash rather than a number: a rate we cannot
// compute is not a rate of zero, and the difference matters most in exactly the
// case somebody is staring at the panel to diagnose something.
enum class RateStatus {
    kAvailable,
    kFirstSample,      // nothing to difference against yet
    kRebooted,         // the device's clock went backwards; counters restarted
    kClockNotAdvanced, // two samples, same device clock: a repeat, not an interval
    kWindowTooShort,   // not enough history yet to span the rate window
};

// How long a rate must span.
//
// ⚠️ NOT the tick interval, and the difference is load-bearing. Some of these
// counters do not originate on the device that publishes them: a leaf's
// frames_built/frames_dropped reach the hub on the LEAF'S HEARTBEAT, roughly
// once a second and not in step with the hub's status publish. Differenced over
// a single 1 s tick they alias -- the live rig showed leaf_frames_built flipping
// between 0.0/s and 20.0/s for a counter that cannot be below 10/s -- and a panel
// that reports 0/s for a leaf that is in fact delivering is worse than one that
// reports nothing. Five seconds is long enough to swallow the aliasing and short
// enough that a board going bad is visible while somebody is watching.
constexpr uint64_t kRateWindowUs = 5000000;
// History kept per device, so the window can always be filled.
constexpr uint64_t kRateHistoryUs = 12000000;

const char *toString(RateStatus status);

// One device's health as the panel should show it.
struct DeviceHealth {
    uint64_t deviceId = 0;
    bool isHub = false;

    // Milliseconds since the BACKEND last received a frame from this device,
    // measured on the backend's wall clock. ⚠️ This is the only field that can
    // say "gone quiet", and it cannot come from the frame: it is about the
    // absence of frames.
    uint64_t ageMs = 0;

    // The most recent frame, as the schema's own JSON (snake_case keys matching
    // the descriptor's paths, so the frontend can label fields from the
    // descriptor rather than from a second hard-coded list).
    nlohmann::json fields;

    RateStatus rateStatus = RateStatus::kFirstSample;
    // Per-second rates for the counters worth a rate. Empty unless
    // rateStatus == kAvailable.
    std::map<std::string, double> rates;
    // The device-clock interval the rates were computed over, so a panel can say
    // how much to trust them.
    uint64_t rateIntervalUs = 0;

    nlohmann::json toJson(uint64_t quietAfterMs) const;
};

// Holds the previous sample per device so rates can be differenced.
//
// Not thread-safe: it is owned by the one thread that tails the topics.
class DeviceHealthTracker {
public:
    // Feed a decoded leaf frame. `wallMs` is the backend's wall clock at receipt.
    void observeNode(const nat::core::NatKitNodeStatusV1Schema &record, uint64_t wallMs);
    // Feed a decoded hub frame.
    void observeHub(const nat::core::NatKitPrimaryStatusV1Schema &record, uint64_t wallMs);

    // Every device seen so far, hub first, then leaves by device id -- including
    // ones that have gone quiet, which is the point: dropping a device that
    // stopped reporting would make the failure invisible in the one view meant
    // to show it.
    std::vector<DeviceHealth> snapshot(uint64_t wallMs) const;

    // Forget devices that have not been heard from in this long. Only for a
    // long-running backend that would otherwise accumulate rows for hardware
    // that left the building; a panel wants to SEE a recently-quiet device.
    void forgetOlderThan(uint64_t wallMs, uint64_t maxAgeMs);

    size_t size() const { return entries_.size(); }

private:
    struct Entry {
        bool isHub = false;
        uint64_t lastWallMs = 0;
        // The device's own monotonic clock at the last sample: the primary's
        // uptime_us, or (for a leaf) last_seen_us, which is the hub's clock.
        // Rates are computed over THIS rather than over the backend's wall clock,
        // so a poll that jitters or a frame that queued does not skew them.
        uint64_t lastDeviceUs = 0;
        std::map<std::string, uint64_t> counters;
        // Recent samples, oldest first, trimmed to kRateHistoryUs. A rate is the
        // difference against the OLDEST retained sample, so it always spans a
        // real interval rather than whatever happened to arrive last tick.
        struct Sample {
            uint64_t deviceUs = 0;
            std::map<std::string, uint64_t> counters;
        };
        std::deque<Sample> history;
        nlohmann::json fields;
        RateStatus rateStatus = RateStatus::kFirstSample;
    };

    void observe(uint64_t deviceId, bool isHub, uint64_t deviceUs,
                 std::map<std::string, uint64_t> counters, nlohmann::json fields,
                 uint64_t wallMs);

    std::map<uint64_t, Entry> entries_;
};

// The counters worth a rate, pulled out so the test and the tracker cannot
// disagree about which fields these are.
std::map<std::string, uint64_t> nodeCounters(
    const nat::core::NatKitNodeStatusV1Schema &record);
std::map<std::string, uint64_t> hubCounters(
    const nat::core::NatKitPrimaryStatusV1Schema &record);

}  // namespace tools
}  // namespace nat
