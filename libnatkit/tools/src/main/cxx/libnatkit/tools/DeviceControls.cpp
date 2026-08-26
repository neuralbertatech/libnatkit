#include "DeviceControls.hpp"

#include <algorithm>
#include <chrono>

#include <nlohmann/json.hpp>

namespace nat {
namespace tools {
namespace {

uint64_t wallMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

// ⚠️ Ten seconds against a hub that emits one per status interval (~1.5 s), so
// six or more consecutive beats must be missed before a device is called
// unreachable. Generous on purpose: a momentary broker backlog is not evidence
// that a board has gone, and a gate that flickers teaches an operator to ignore
// it -- the same reasoning that put TEC-NATKIT-81's presence window at 30 s.
constexpr uint64_t kReachableWindowMs = 10000;

// Forget a device nothing has been heard from for an hour, so a long-lived
// backend does not accumulate boards that were unplugged in the morning.
constexpr uint64_t kForgetAfterMs = 3600000;

constexpr char kControlsSchemaName[] = "NatKitDeviceControlsV1";

}  // namespace

const char *toString(const CommandGate gate) {
    switch (gate) {
    case CommandGate::kAllowed:
        return "allowed";
    case CommandGate::kNoAdvertisement:
        return "no_advertisement";
    case CommandGate::kNotAdvertised:
        return "not_advertised";
    case CommandGate::kUnreachable:
        return "unreachable";
    }
    return "unknown";
}

DeviceControlsService &DeviceControlsService::instance() {
    static DeviceControlsService service;
    return service;
}

DeviceControlsService::~DeviceControlsService() { stop(); }

void DeviceControlsService::start(
    const std::shared_ptr<nat::kafka::BrokerManager> &brokerManager) {
    if (!brokerManager) {
        return;
    }
    // exchange, not check-then-set: two clients connecting at once both reach
    // here, and two tailers would each see half the frames.
    if (running_.exchange(true)) {
        return;
    }
    brokerManager_ = brokerManager;
    thread_ = std::thread(&DeviceControlsService::tailLoop, this);
}

void DeviceControlsService::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

void DeviceControlsService::setStrictMode(const bool strict) { strict_ = strict; }
bool DeviceControlsService::strictMode() const { return strict_; }
uint64_t DeviceControlsService::unadvertisedAllowed() const {
    return unadvertisedAllowed_;
}

size_t DeviceControlsService::topicsTailed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return topicsTailed_;
}

std::vector<DeviceControlsSnapshot> DeviceControlsService::snapshot() const {
    const uint64_t now = wallMs();
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<DeviceControlsSnapshot> out;
    out.reserve(entries_.size());
    for (const auto &pair : entries_) {
        DeviceControlsSnapshot snap;
        snap.deviceId = pair.first;
        snap.controls = pair.second.controls;
        snap.advertiserDeviceId = pair.second.advertiserDeviceId;
        snap.hasAdvertisement = pair.second.hasAdvertisement;
        const auto beat = pair.second.lastHeartbeatWallMs;
        // Saturating: a system clock that stepped back must not present a device
        // as heard in the future, nor as billions of ms stale.
        snap.sinceHeartbeatMs = (beat != 0 && now > beat) ? now - beat : 0;
        snap.reachable = beat != 0 && now >= beat &&
                         (now - beat) <= kReachableWindowMs;
        out.push_back(std::move(snap));
    }
    return out;
}

DeviceControlsSnapshot DeviceControlsService::forDevice(
    const uint64_t deviceId) const {
    for (auto &snap : snapshot()) {
        if (snap.deviceId == deviceId) {
            return snap;
        }
    }
    DeviceControlsSnapshot empty;
    empty.deviceId = deviceId;
    return empty;
}

CommandGate DeviceControlsService::check(const uint64_t deviceId,
                                         const std::string &command) const {
    const auto snap = forDevice(deviceId);

    if (!snap.hasAdvertisement) {
        // ⚠️ THE MIGRATION PATH. Firmware older than TEC-NATKIT-10 never
        // advertises, and refusing it would silently remove every control that
        // works today. Counted so the decision to turn strict on is evidence-led
        // rather than a guess; not a permanent exemption.
        if (strict_) {
            return CommandGate::kNoAdvertisement;
        }
        ++unadvertisedAllowed_;
        return CommandGate::kAllowed;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = entries_.find(deviceId);
    if (found == entries_.end()) {
        return CommandGate::kNoAdvertisement;
    }
    const auto &allowed = found->second.allowedCommands;
    if (std::find(allowed.begin(), allowed.end(), command) == allowed.end()) {
        return CommandGate::kNotAdvertised;
    }
    // ⚠️ Reachability is checked LAST, so the more specific answer wins: a
    // command this device never supported should say so even while it is
    // offline, rather than sending somebody to check the power first.
    if (!snap.reachable) {
        return CommandGate::kUnreachable;
    }
    return CommandGate::kAllowed;
}

void DeviceControlsService::tailLoop() {
    std::map<std::string, std::unique_ptr<nat::core::TopicMessenger>> messengers;
    uint64_t lastScanMs = 0;

    while (running_) {
        const uint64_t tickMs = wallMs();

        // Rescan: a board that boots mid-session creates its topics then, so the
        // set cannot be captured once at startup.
        if (tickMs - lastScanMs >= 5000) {
            lastScanMs = tickMs;
            for (const auto &topic : brokerManager_->getAllTopics()) {
                if (!topic) {
                    continue;
                }
                const bool wanted =
                    topic->type == nat::core::StreamType::HARDWARE_CONFIGURATION ||
                    topic->type == nat::core::StreamType::LOGGING_HEARTBEAT;
                if (!wanted) {
                    continue;
                }
                const std::string name = topic->toTopicString();
                if (messengers.count(name) > 0) {
                    continue;
                }
                auto messenger = brokerManager_->createMessenger(
                    std::make_shared<nat::core::BasicTopicInformation>(*topic));
                if (messenger) {
                    messengers[name] = std::move(messenger);
                }
            }
            std::lock_guard<std::mutex> lock(mutex_);
            topicsTailed_ = messengers.size();
        }

        for (auto &entry : messengers) {
            const bool isControls =
                entry.first.find(kControlsSchemaName) != std::string::npos;
            for (int drained = 0; drained < 256; ++drained) {
                auto next = entry.second->tryGetNextRawMessage();
                if (!next.has_value()) {
                    break;
                }
                const auto &bytes = *next.value();
                const uint64_t receivedMs = wallMs();

                if (isControls) {
                    auto decoded =
                        nat::core::NatKitDeviceControlsV1Schema::decodeJson(bytes);
                    if (!decoded.has_value()) {
                        continue;
                    }
                    const auto &record = decoded.value();
                    std::lock_guard<std::mutex> lock(mutex_);
                    auto &slot = entries_[record.deviceId];
                    slot.controls = record.controls;
                    // ⚠️ Derived HERE, once, from the advertisement itself. The
                    // server must never assemble an allowlist from its own idea
                    // of what a device supports -- that is the hardcoding this
                    // whole feature exists to delete.
                    slot.allowedCommands = record.allowedCommands();
                    slot.advertiserDeviceId = record.advertiserDeviceId;
                    slot.hasAdvertisement = true;
                } else {
                    // A heartbeat. Only the device id and the arrival time
                    // matter -- the body is diagnostic.
                    const std::string text(bytes.begin(), bytes.end());
                    const auto doc = nlohmann::json::parse(text, nullptr, false);
                    if (doc.is_discarded() || !doc.is_object()) {
                        continue;
                    }
                    const auto deviceId =
                        doc.value("device_id", static_cast<uint64_t>(0));
                    if (deviceId == 0) {
                        continue;
                    }
                    std::lock_guard<std::mutex> lock(mutex_);
                    entries_[deviceId].lastHeartbeatWallMs = receivedMs;
                }
            }
        }

        {
            const uint64_t now = wallMs();
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto it = entries_.begin(); it != entries_.end();) {
                const auto beat = it->second.lastHeartbeatWallMs;
                // ⚠️ Only forget a device with no advertisement AND no recent
                // heartbeat. A retained advertisement is worth keeping: "this is
                // what that board offers" stays true while it is unplugged, and
                // dropping it would make an unreachable device indistinguishable
                // from one that never existed.
                const bool stale = beat == 0 || (now > beat && now - beat > kForgetAfterMs);
                if (stale && !it->second.hasAdvertisement) {
                    it = entries_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
}

}  // namespace tools
}  // namespace nat
