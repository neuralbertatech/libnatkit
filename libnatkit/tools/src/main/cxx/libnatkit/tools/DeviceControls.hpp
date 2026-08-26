#pragma once

// What each device says it can do, and whether it is reachable (TEC-NATKIT-10).
//
// Two channels, two questions, and keeping them apart is the whole design:
//
//   Configuration  what can this device DO?      retained, slow-changing
//   Heartbeat      is it REACHABLE right now?    never retained, periodic
//
// ⚠️ A COMMAND NEEDS BOTH. Permitting on the advertisement alone would let the
// server send to a board that is unplugged -- and because the advertisement is
// RETAINED, it survives the board, so "it is advertised" is not evidence that
// anything is listening. Permitting on reachability alone would let any command
// through to a device that never claimed to support it. The intersection is the
// only honest gate.
//
// ⚠️ AND IT PAYS FOR ITSELF IMMEDIATELY. On 2026-08-26 an `identify` sent to a
// leaf running older firmware burned the full 15-second timeout before failing.
// With this gate the refusal is instant and says which half failed.

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include <libnatkit-core.hpp>
#include <libnatkit-kafka.hpp>

namespace nat {
namespace tools {

/** Why a command was refused, so the caller can say something useful. */
enum class CommandGate {
    /** Advertised and reachable. */
    kAllowed,
    /** The device has never advertised. See the migration note below. */
    kNoAdvertisement,
    /** It advertised, but not this command. */
    kNotAdvertised,
    /** It advertised this command, but is not currently reachable. */
    kUnreachable,
};

const char *toString(CommandGate gate);

struct DeviceControlsSnapshot {
    uint64_t deviceId = 0;
    /** Empty when the device has never advertised. */
    std::vector<nat::core::DeviceControlAdvertisement> controls;
    /** Who published the advertisement; differs for a bridged device. */
    uint64_t advertiserDeviceId = 0;
    bool hasAdvertisement = false;
    /** Whether a heartbeat has arrived inside the reachability window. */
    bool reachable = false;
    /** How long since the last heartbeat, or 0 if none has ever arrived. */
    uint64_t sinceHeartbeatMs = 0;

    nlohmann::json toJson() const;
};

class DeviceControlsService {
public:
    static DeviceControlsService &instance();

    /** Idempotent, like DeviceHealthService: the first caller starts it. */
    void start(const std::shared_ptr<nat::kafka::BrokerManager> &brokerManager);
    void stop();

    std::vector<DeviceControlsSnapshot> snapshot() const;
    DeviceControlsSnapshot forDevice(uint64_t deviceId) const;

    /**
     * The gate.
     *
     * ⚠️ MIGRATION, AND IT IS NOT OPTIONAL. Devices running firmware older than
     * TEC-NATKIT-10 never advertise, and refusing them outright would silently
     * remove every control that works today -- the calibration node's buttons
     * included. So kNoAdvertisement is COUNTED AND ALLOWED while
     * `strictMode` is false, and the count is the evidence for when it is safe
     * to turn strict on. It is not a permanent exemption.
     */
    CommandGate check(uint64_t deviceId, const std::string &command) const;

    void setStrictMode(bool strict);
    bool strictMode() const;

    /** How many commands were let through only because nothing was advertised. */
    uint64_t unadvertisedAllowed() const;

    bool running() const { return running_; }
    size_t topicsTailed() const;

private:
    DeviceControlsService() = default;
    ~DeviceControlsService();
    DeviceControlsService(const DeviceControlsService &) = delete;
    DeviceControlsService &operator=(const DeviceControlsService &) = delete;

    void tailLoop();

    struct Entry {
        std::vector<nat::core::DeviceControlAdvertisement> controls;
        std::vector<std::string> allowedCommands;
        uint64_t advertiserDeviceId = 0;
        bool hasAdvertisement = false;
        uint64_t lastHeartbeatWallMs = 0;
    };

    std::shared_ptr<nat::kafka::BrokerManager> brokerManager_;
    std::atomic<bool> running_{false};
    std::atomic<bool> strict_{false};
    mutable std::atomic<uint64_t> unadvertisedAllowed_{0};
    std::thread thread_;
    mutable std::mutex mutex_;
    std::map<uint64_t, Entry> entries_;
    size_t topicsTailed_ = 0;
};

}  // namespace tools
}  // namespace nat
