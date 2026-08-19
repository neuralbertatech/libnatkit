#pragma once

// Stream -> Parquet export, entirely in the backend.
//
// A VP `combine` node already fans a data stream and an experiment's `markers`
// into ONE channel: Data/<id> and Marker/<id> share a stream id (that bundle IS
// the join). This exports such a channel to a Parquet file: drain both topics
// from the beginning, project each data record generically through the
// canonical channel-frame contract, stamp the active cue's label onto every
// frame with nat::core::assignIntervalsToTimeline (the same interval stitcher
// the training path uses), and write the rows.
//
// A plain data-only stream exports too; it just has no label column values.

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace nat::kafka {
class BrokerManager;
}

namespace natkit::tools {

struct ParquetExportRequest {
    // The channel to export. Its DATA topic supplies the rows; its MARKER topic
    // (if the channel bundles one, as a combine output does) supplies the labels
    // + the session window.
    uint64_t streamId = 0;
    // Optional: take markers from a DIFFERENT channel instead. Lets an export
    // node be fed a data stream and an experiment directly, without requiring a
    // combine node in between to bundle them first. 0 = use streamId's own
    // marker topic.
    uint64_t markerStreamId = 0;
    // Optional clip, in device-timestamp microseconds. Absent = the whole
    // session window implied by the markers, or everything when there are none.
    std::optional<int64_t> startUs{};
    std::optional<int64_t> endUs{};
    // Who was recorded. Written into the Parquet key-value metadata as
    // `natkit.participant_id` so the file is self-describing once it leaves the
    // rig — a data file that cannot name its own subject is not usable evidence.
    // Empty is legitimate (an ad-hoc export of a live stream has no participant);
    // it is simply omitted from the metadata rather than written blank.
    std::string participantId{};
    // Column name for the joined cue class.
    std::string labelField = "label";
    // Restrict to one run inside a multi-run session (1-based). 0 = all runs.
    int runIndex = 0;
    // How long to keep polling a topic with no new records before deciding it
    // is drained. Only applies AFTER the first record has arrived.
    int idleTimeoutMs = 2000;
    // How long to wait for the FIRST record. A fresh Kafka consumer has to
    // connect, fetch metadata and get its partition assignment, which routinely
    // takes longer than the idle window — collapsing the two made every export
    // come back empty.
    int firstRecordTimeoutMs = 15000;
    // Hard wall-clock ceiling per topic. An export of a LIVE stream would
    // otherwise never terminate: records keep arriving, so the idle window keeps
    // resetting. This bounds the export to "whatever was on the topic within
    // this budget" and keeps a request from pinning a drogon worker forever.
    int maxDurationMs = 30000;
    // Safety valve so a pathological export cannot exhaust memory.
    size_t maxFrames = 2'000'000;
    // When set, ALSO write the drained markers to this path as JSON Lines.
    //
    // Instance materialization needs the marker timeline back on disk: replay
    // interleaves it with the data, and without it the marker overlay is blank,
    // combine's marker lane is empty, and a re-export of a fork comes back
    // unlabelled. The exporter has already decoded these markers to build the
    // label intervals, so draining the topic a second time would be slower AND
    // racier -- retention could roll between the two passes.
    std::string markersSidecarPath{};
};

struct ParquetExportResult {
    bool ok = false;
    std::string error{};
    // Absolute path of the written file (a temp file the caller serves + unlinks).
    std::string filePath{};
    // Suggested download name.
    std::string fileName{};
    size_t frameCount = 0;
    size_t markerCount = 0;
    size_t labelledFrameCount = 0;
    // The drain stopped on the time/frame budget rather than because the topic
    // ran dry — the file is a prefix, not the whole stream. Never silent.
    bool truncated = false;
    std::string sessionId{};
    std::optional<int64_t> windowStartUs{};
    std::optional<int64_t> windowEndUs{};
    // The schema the ORIGINAL records used. Replay does not need it (it emits the
    // canonical frame), but provenance does: without it a snapshot cannot say what
    // sensor it came from.
    std::string schemaName{};
    // Set when markersSidecarPath was requested and written.
    bool markersSidecarWritten = false;
    size_t markersSidecarCount = 0;
    // Rows per cue class. This is what makes a recorded dataset REVIEWABLE: a
    // frame count alone can't tell you the run is unusable because one gesture
    // never fired, or that the classes are wildly unbalanced. Unlabelled rows are
    // counted under the empty key.
    std::map<std::string, size_t> labelCounts{};
};

// True when the build has a Parquet writer linked in.
bool parquetExportAvailable();

// Drain the channel and write a Parquet file into `outputDir` (a temp dir).
// Never throws; failures come back in result.error.
ParquetExportResult exportStreamToParquet(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ParquetExportRequest& request,
    const std::string& outputDir);

}  // namespace natkit::tools
