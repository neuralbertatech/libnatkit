#pragma once

// Replay an instance's materialized Parquet back onto the wire.
//
// An instance is permanent because its data LEFT Kafka (retention is 168h and the
// broker's log dir is a volume). Replaying one therefore means streaming records
// out of the files, not time-travelling in the broker: a snapshot recorded a year
// ago replays identically to one recorded a minute ago, and retention stops being
// part of the story.
//
// The records go to a SCRATCH Kafka topic per replay session rather than through
// the in-process transport. The browser's subscribe path binds Kafka messengers,
// and the in-process channels are only used for PRIVATE colocated edges -- i.e.
// exactly the edges nobody is watching. A viewer forces Kafka. Publishing to a
// scratch topic means every downstream consumer (transform workers, viewers,
// topic-aware combine, the marker overlay, even a re-export of the replayed
// result) works with zero changes, because from the graph's point of view it is
// just another live stream. Permanent data on disk, ephemeral data on the wire.
//
// A pleasant consequence: the Parquet IS the canonical channel-frame projection
// (that is what the exporter writes), so replay emits NatSignalFrameDataSchemaV1
// regardless of what the original sensor published. The IMU needs an alternate
// input mapping to be filterable live; replayed from Parquet it needs nothing.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace nat::kafka {
class BrokerManager;
}

namespace natkit::tools {

struct ReplaySourceSpec {
    // The stream id this data was RECORDED from. Carried through so the caller can
    // map "the source node that used to read stream X" to the scratch topic.
    uint64_t originalStreamId = 0;
    // Absolute path to the instance's Parquet for that source.
    std::string parquetPath;
};

struct ReplayRequest {
    std::vector<ReplaySourceSpec> sources;
    // Optional markers sidecar (JSON Lines) to interleave.
    std::string markersPath;
    // Topic-identifier stem for the scratch topics; must be a valid topic
    // identifier. Callers make it unique per replay session.
    std::string replayIdentifier;
    // Review mode = paced from device_ts_us deltas. Recompute mode = unpaced (as
    // fast as the pipeline drains), for training or re-running a fork's pipeline
    // where nobody is watching frames go by.
    bool paced = true;
    // Playback multiplier for paced mode (0.25x - 8x).
    double speed = 1.0;
    // Safety valve: replaying is a full read into memory, so bound it.
    size_t maxFrames = 2'000'000;
    // Longest gap paced mode will actually WAIT through, in microseconds. Local
    // timing stays exact; only dead air is compressed.
    //
    // Without this, one item far from the rest stalls the replay: a session
    // lifecycle marker sits at the start of a recording, and an instance whose
    // markers bracket a window wider than its data would make review mode wait out
    // the whole difference — in testing, 56 years. Recompute mode ignores this.
    int64_t maxGapUs = 2'000'000;
};

struct ReplayBinding {
    uint64_t originalStreamId = 0;
    uint64_t replayStreamId = 0;
    std::string identifier;
    std::string topic;
    size_t frameCount = 0;
    std::vector<std::string> channelLabels;
};

struct ReplayPlan {
    bool ok = false;
    std::string error{};
    std::vector<ReplayBinding> bindings;
    // The scratch MARKER topic (0 when the instance has no markers sidecar).
    uint64_t markerStreamId = 0;
    std::string markerTopic{};
    size_t markerCount = 0;
    size_t totalFrames = 0;
    // Span of the replayed timeline, in the ORIGINAL device timestamps.
    int64_t firstTsUs = 0;
    int64_t lastTsUs = 0;
};

struct ReplayProgress {
    size_t framesPublished = 0;
    size_t markersPublished = 0;
    int64_t lastTsUs = 0;
    bool finished = false;
    bool cancelled = false;
    std::string error{};
};

// True when this build can read Parquet (same gate as the exporter).
bool replayAvailable();

// Read the artifacts, allocate the scratch topics, and report what a run WOULD
// publish. Separate from running so the caller can bind a graph's source nodes to
// the scratch stream ids before any data flows -- otherwise the first frames would
// be published into a topic nothing is subscribed to yet.
ReplayPlan planReplay(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ReplayRequest& request);

// Run the replay. BLOCKS: call it on its own thread. `cancelled` is polled between
// frames (and inside long paced sleeps) so a stop is responsive. `onProgress` is
// called periodically and once at the end.
void runReplay(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ReplayRequest& request,
    const ReplayPlan& plan,
    const std::atomic<bool>& cancelled,
    const std::function<void(const ReplayProgress&)>& onProgress);

// Remove a replay's scratch topics. Called when the replay ends: they are
// ephemeral by construction, and leaving them behind would accrete a topic per
// replay on a broker whose storage we care about.
void deleteReplayTopics(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ReplayPlan& plan);

}  // namespace natkit::tools
