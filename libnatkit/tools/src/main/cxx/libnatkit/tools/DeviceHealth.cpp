#include "DeviceHealth.hpp"

#include <algorithm>

namespace nat {
namespace tools {

const char *toString(const RateStatus status) {
    switch (status) {
    case RateStatus::kAvailable:
        return "available";
    case RateStatus::kFirstSample:
        return "first_sample";
    case RateStatus::kRebooted:
        return "rebooted";
    case RateStatus::kClockNotAdvanced:
        return "clock_not_advanced";
    case RateStatus::kWindowTooShort:
        return "window_too_short";
    }
    return "unknown";
}

// Which counters get a rate.
//
// Not all of them. `frames_queued` is deliberately absent: it is racy and reads
// below frames_sent (TEC-NATKIT-75), and a rate computed from it would look
// authoritative. Gauges are absent too -- nodes_known is a count of what exists,
// and "4 nodes per second" is not a sentence.
std::map<std::string, uint64_t> nodeCounters(
    const nat::core::NatKitNodeStatusV1Schema &record) {
    return {
        {"data_frames", record.dataFrames},
        {"seq_gaps", record.seqGaps},
        {"seq_duplicates", record.seqDuplicates},
        {"seq_restarts", record.seqRestarts},
        {"heartbeats", record.heartbeats},
        {"sync.beacons_seen", record.beaconsSeen},
        {"sync.beacons_missed", record.beaconsMissed},
        {"sync.pairs_used", record.pairsUsed},
        {"sync.pairs_orphaned", record.pairsOrphaned},
        {"sync.outliers_rejected", record.outliersRejected},
        {"leaf_frames_built", record.leafFramesBuilt},
        {"leaf_frames_dropped", record.leafFramesDropped},
        {"leaf_send_failures", record.leafSendFailures},
        {"leaf_channel_hops", record.leafChannelHops},
        {"publish_no_sync", record.publishNoSync},
        {"publish_no_shift", record.publishNoShift},
    };
}

std::map<std::string, uint64_t> hubCounters(
    const nat::core::NatKitPrimaryStatusV1Schema &record) {
    return {
        {"nodes_rejected", record.nodesRejected},
        {"unknown_packets", record.unknownPackets},
        {"frames_sent", record.framesSent},
        {"frames_dropped", record.framesDropped},
        {"write_timeouts", record.writeTimeouts},
        {"bytes_sent", record.bytesSent},
        {"commands_received", record.commandsReceived},
        {"commands_relayed", record.commandsRelayed},
        {"commands_delivered", record.commandsDelivered},
        {"command_retransmits", record.commandRetransmits},
        {"commands_undelivered", record.commandsUndelivered},
    };
}

namespace {

// The device clock a leaf's rates are measured on. last_seen_us is when the HUB
// last heard this leaf, so it is on the hub's clock -- the same axis as the hub's
// own uptime_us, which is what makes a leaf's rate and the hub's comparable.
uint64_t deviceClockUs(const nat::core::NatKitNodeStatusV1Schema &record) {
    return record.lastSeenUs;
}

uint64_t deviceClockUs(const nat::core::NatKitPrimaryStatusV1Schema &record) {
    return record.uptimeUs;
}

nlohmann::json parseFields(const std::string &json) {
    // The schemas produce their own JSON, which is where the field names come
    // from. If that ever failed to parse we would rather show nothing for the
    // device than a half-populated row that reads as fact.
    try {
        return nlohmann::json::parse(json);
    } catch (const std::exception &) {
        return nlohmann::json::object();
    }
}

}  // namespace

void DeviceHealthTracker::observe(const uint64_t deviceId, const bool isHub,
                                  const uint64_t deviceUs,
                                  std::map<std::string, uint64_t> counters,
                                  nlohmann::json fields, const uint64_t wallMs) {
    Entry &entry = entries_[deviceId];
    const bool hadSample = entry.lastWallMs != 0;

    // ⚠️ A device clock that went BACKWARDS means the board restarted, and every
    // counter restarted with it. Differencing across that boundary produces a
    // hugely negative delta, which as an unsigned subtraction becomes a hugely
    // POSITIVE rate -- a rebooting leaf would show as the busiest thing on the
    // rig. So the history is thrown away and rebuilt from here.
    if (hadSample && deviceUs < entry.lastDeviceUs) {
        // Throw the pre-reboot history away, but KEEP THIS SAMPLE as the new
        // baseline. Dropping it too would cost an extra frame before rates could
        // resume -- and post-reboot is exactly when somebody is watching to see
        // whether the board came back healthy.
        entry.history.clear();
        entry.history.push_back(Entry::Sample{deviceUs, counters});
        entry.rateStatus = RateStatus::kRebooted;
    } else if (hadSample && deviceUs == entry.lastDeviceUs) {
        // The same frame twice, or a status frame built before the clock
        // advanced. Not an interval, and not a reboot: the history stays, so the
        // next frame that does advance can still be differenced.
        entry.rateStatus = RateStatus::kClockNotAdvanced;
    } else {
        entry.history.push_back(Entry::Sample{deviceUs, counters});
        // Drop samples older than the history bound, but never the last one we
        // would need: keep the oldest that is still within kRateHistoryUs, plus
        // enough to span the window.
        while (entry.history.size() > 1 &&
               deviceUs - entry.history.front().deviceUs > kRateHistoryUs) {
            entry.history.pop_front();
        }
        if (entry.history.size() < 2) {
            entry.rateStatus = RateStatus::kFirstSample;
        } else if (deviceUs - entry.history.front().deviceUs < kRateWindowUs) {
            // Two samples but not yet a window's worth. Reporting a rate here is
            // exactly the aliasing this window exists to avoid, so it waits.
            entry.rateStatus = RateStatus::kWindowTooShort;
        } else {
            entry.rateStatus = RateStatus::kAvailable;
        }
    }

    entry.isHub = isHub;
    entry.lastWallMs = wallMs;
    entry.lastDeviceUs = deviceUs;
    entry.counters = std::move(counters);
    entry.fields = std::move(fields);
}

void DeviceHealthTracker::observeNode(
    const nat::core::NatKitNodeStatusV1Schema &record, const uint64_t wallMs) {
    observe(record.deviceId, /*isHub=*/false, deviceClockUs(record),
            nodeCounters(record), parseFields(record.toJson()), wallMs);
}

void DeviceHealthTracker::observeHub(
    const nat::core::NatKitPrimaryStatusV1Schema &record, const uint64_t wallMs) {
    observe(record.deviceId, /*isHub=*/true, deviceClockUs(record),
            hubCounters(record), parseFields(record.toJson()), wallMs);
}

std::vector<DeviceHealth> DeviceHealthTracker::snapshot(const uint64_t wallMs) const {
    std::vector<DeviceHealth> out;
    out.reserve(entries_.size());
    for (const auto &pair : entries_) {
        const Entry &entry = pair.second;
        DeviceHealth health;
        health.deviceId = pair.first;
        health.isHub = entry.isHub;
        // Saturate rather than wrap: a wallMs behind lastWallMs would mean the
        // system clock stepped back, and a device reported as 4e18 ms stale is
        // less useful than one reported as fresh.
        health.ageMs = wallMs > entry.lastWallMs ? wallMs - entry.lastWallMs : 0;
        health.fields = entry.fields;
        health.rateStatus = entry.rateStatus;

        if (entry.rateStatus == RateStatus::kAvailable && entry.history.size() >= 2) {
            const auto &baseline = entry.history.front();
            const uint64_t intervalUs = entry.lastDeviceUs - baseline.deviceUs;
            health.rateIntervalUs = intervalUs;
            const double seconds = static_cast<double>(intervalUs) / 1e6;
            for (const auto &counter : entry.counters) {
                const auto previous = baseline.counters.find(counter.first);
                if (previous == baseline.counters.end()) {
                    continue;
                }
                // Unsigned subtraction, which is also the right answer for a
                // counter that WRAPPED: modulo arithmetic gives the true delta as
                // long as it wrapped at most once, and at these rates a 32-bit
                // counter takes weeks. The reboot case is excluded above, which
                // is the one that actually happens.
                const uint64_t delta = counter.second - previous->second;
                health.rates[counter.first] = static_cast<double>(delta) / seconds;
            }
        }
        out.push_back(std::move(health));
    }
    // Hub first, then leaves by device id -- a stable order, so a panel does not
    // reshuffle its rows every second.
    std::stable_sort(out.begin(), out.end(),
                     [](const DeviceHealth &a, const DeviceHealth &b) {
                         if (a.isHub != b.isHub) {
                             return a.isHub;
                         }
                         return a.deviceId < b.deviceId;
                     });
    return out;
}

void DeviceHealthTracker::forgetOlderThan(const uint64_t wallMs,
                                          const uint64_t maxAgeMs) {
    for (auto it = entries_.begin(); it != entries_.end();) {
        if (wallMs > it->second.lastWallMs && wallMs - it->second.lastWallMs > maxAgeMs) {
            it = entries_.erase(it);
        } else {
            ++it;
        }
    }
}

nlohmann::json DeviceHealth::toJson(const uint64_t quietAfterMs) const {
    nlohmann::json out;
    // ⚠️ A string, not a number: these ids exceed 2^53 and JavaScript would
    // round them. 13793649670644 is fine, but the hub's 203376942053180 is the
    // kind of value that loses its last digits silently.
    out["device_id"] = std::to_string(deviceId);
    out["role"] = isHub ? "hub" : "leaf";
    out["age_ms"] = ageMs;
    out["quiet"] = ageMs > quietAfterMs;
    out["fields"] = fields;
    out["rate_status"] = toString(rateStatus);
    if (rateStatus == RateStatus::kAvailable && !rates.empty()) {
        nlohmann::json rateJson = nlohmann::json::object();
        for (const auto &rate : rates) {
            rateJson[rate.first] = rate.second;
        }
        out["rates"] = std::move(rateJson);
        out["rate_interval_us"] = rateIntervalUs;
    } else {
        // Explicitly null rather than absent or {}: the frontend must render a
        // dash, and "no rates yet" and "a rate of zero" have to look different.
        out["rates"] = nullptr;
    }
    return out;
}

}  // namespace tools
}  // namespace nat
