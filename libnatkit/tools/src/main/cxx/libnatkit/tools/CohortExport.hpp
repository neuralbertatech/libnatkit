#pragma once

// Cohort export: every completed run in a workspace, as one archive.
//
// The payoff of the workspace container (TEC-NATKIT-65). Without it, getting a
// study's data means opening each experiment, finding each instance and pressing
// Download once per file -- which is both tedious and unauditable, because nothing
// records which runs you happened to skip.
//
// ⚠️ This reads the MATERIALIZED ARTIFACTS ON DISK, never Kafka.
// `/api/export/parquet` re-drains the topic, which is right for a live stream and
// wrong for sealed history: retention may have rolled long ago, and a re-drain
// could produce a different file from the one the instance recorded a sha256 for.
// Sealed history has exactly one correct source and it is the file.
//
// The archive is a POSIX ustar tar, uncompressed and with no external dependency:
// Parquet is already compressed, so deflate would buy nothing, and `tar -xf` is
// universal. ⚠️ Every header carries mtime 0 so that re-exporting an unchanged
// workspace produces a BYTE-IDENTICAL archive -- the ticket asks for deterministic
// re-export, and a wall-clock mtime is the one thing that would break it.

#include <cstdint>
#include <string>
#include <vector>

namespace natkit::tools {

// One file to place in the archive, already located on disk.
struct CohortArtifact {
    std::string absolutePath;
    // Path inside the archive. Shaped participant/experiment/instance/file so an
    // extracted tree is navigable by the thing a researcher actually thinks in.
    std::string archivePath;
    // What the instance recorded when it sealed. Empty = it recorded none.
    std::string recordedSha256;
    bool truncated = false;
};

// One completed run.
struct CohortInstance {
    std::string experimentId;
    std::string instanceId;
    std::string participantId;
    // Set when the participant was recovered by the back-fill rather than captured
    // at record time, or never entered at all (TEC-NATKIT-54). Carried into the
    // manifest because a recovered attribution is a weaker claim than a captured
    // one and the archive must not flatten the difference.
    bool participantBackfilled = false;
    bool participantUnrecorded = false;
    // Non-empty when the run was taken below the calibration minimum on purpose
    // (TEC-NATKIT-63).
    std::string calibrationOverride;
    /**
     * One-line summary of whether the clocks could be trusted (TEC-NATKIT-77).
     *
     * ⚠️ In the manifest because a Parquet leaves this system: whoever analyses
     * it months later has the file and the manifest, not the backend's store and
     * certainly not the status frames, which aged out of Kafka long before.
     * "" means the run predates the record — which is not the same as clean.
     */
    std::string clockSummary;
    // stream_id -> body position, as recorded (TEC-NATKIT-62).
    std::vector<std::pair<std::string, std::string>> sensorPositions;
    uint64_t totalRows = 0;
    std::vector<CohortArtifact> artifacts;
};

// A run that will NOT be in the archive, and why.
//
// ⚠️ These are the whole point of the manifest. A cohort export that quietly omits
// three participants reads as complete, and the omission is invisible precisely
// when it matters most -- at analysis time, months later.
struct CohortSkip {
    std::string experimentId;
    std::string instanceId;
    std::string reason;
};

struct CohortExportInputs {
    std::string workspaceId;
    std::string workspaceLabel;
    std::vector<CohortInstance> instances;
    std::vector<CohortSkip> skips;
};

struct CohortExportResult {
    bool ok = false;
    std::string error;
    // The tar itself.
    std::string archive;
    std::string fileName;
    size_t instanceCount = 0;
    size_t fileCount = 0;
    size_t skipCount = 0;
    // Files whose bytes no longer hash to what the instance recorded. They are
    // still INCLUDED -- withholding data because a checksum moved would be its own
    // kind of lie -- but they are named in the manifest and counted here.
    size_t checksumMismatchCount = 0;
};

// Build the archive. Pure: everything it needs is in `inputs`, so it is testable
// without a broker, a store or a filesystem walk.
CohortExportResult buildCohortArchive(const CohortExportInputs& inputs);

// Gather the inputs for a workspace from the graph/experiment/workspace stores.
// Implemented in StreamViewerWebSocket.cpp, which owns those stores.
CohortExportInputs collectCohortInputs(const std::string& workspaceId,
                                       std::string& error);

}  // namespace natkit::tools
