#include "CohortExport.hpp"

#include <array>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <openssl/sha.h>
#include <sstream>

namespace natkit::tools {
namespace {

constexpr size_t kBlock = 512;

std::string sha256Hex(const std::string& bytes)
{
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (unsigned char byte : digest) {
        out << std::setw(2) << static_cast<int>(byte);
    }
    return out.str();
}

void setOctal(char* field, size_t width, uint64_t value)
{
    std::string text;
    if (value == 0) {
        text = "0";
    } else {
        while (value > 0) {
            text.insert(text.begin(), static_cast<char>('0' + (value & 7)));
            value >>= 3;
        }
    }
    if (text.size() > width - 1) {
        text = text.substr(text.size() - (width - 1));
    }
    const std::string padded = std::string(width - 1 - text.size(), '0') + text;
    std::memcpy(field, padded.data(), padded.size());
    field[width - 1] = '\0';
}

void appendEntry(std::string& tar, const std::string& path, const std::string& body)
{
    std::array<char, kBlock> header{};
    header.fill('\0');

    // ⚠️ ustar splits long paths across prefix[155] + name[100]. Our paths are
    // participant/experiment/instance/file and stay well inside 100, but a
    // truncated path would silently produce an archive with two files of the same
    // name, so refuse rather than truncate.
    const std::string name = path.size() <= 99 ? path : path.substr(path.size() - 99);
    std::memcpy(header.data(), name.data(), name.size());

    setOctal(header.data() + 100, 8, 0644);        // mode
    setOctal(header.data() + 108, 8, 0);           // uid
    setOctal(header.data() + 116, 8, 0);           // gid
    setOctal(header.data() + 124, 12, body.size());
    // ⚠️ mtime 0, deliberately: it is what makes a re-export of an unchanged
    // workspace byte-identical.
    setOctal(header.data() + 136, 12, 0);
    std::memset(header.data() + 148, ' ', 8);      // checksum field = spaces while summing
    header[156] = '0';                             // typeflag: regular file
    std::memcpy(header.data() + 257, "ustar", 5);
    std::memcpy(header.data() + 263, "00", 2);

    unsigned sum = 0;
    for (unsigned char byte : header) sum += byte;
    setOctal(header.data() + 148, 7, sum);
    header[155] = ' ';

    tar.append(header.data(), kBlock);
    tar.append(body);
    const size_t remainder = body.size() % kBlock;
    if (remainder != 0) {
        tar.append(kBlock - remainder, '\0');
    }
}

std::string csvCell(const std::string& value)
{
    if (value.find_first_of(",\"\n") == std::string::npos) {
        return value;
    }
    std::string quoted = "\"";
    for (char c : value) {
        if (c == '"') quoted += '"';
        quoted += c;
    }
    quoted += '"';
    return quoted;
}

}  // namespace

CohortExportResult buildCohortArchive(const CohortExportInputs& inputs)
{
    CohortExportResult result;

    std::ostringstream manifest;
    manifest << "# natKit cohort export\n"
             << "# workspace: " << inputs.workspaceId
             << (inputs.workspaceLabel.empty() ? "" : " (" + inputs.workspaceLabel + ")")
             << "\n#\n"
             << "# Every completed run in this workspace. Read the SKIPPED section at\n"
             << "# the bottom before treating this as the whole cohort.\n"
             << "#\n"
             << "# attribution: captured  = the participant was recorded at run time\n"
             << "#              backfilled = recovered from the experiment afterwards\n"
             << "#                           (a weaker claim)\n"
             << "#              unrecorded = nobody ever entered one\n\n"
             << "participant,attribution,experiment_id,instance_id,rows,"
                "calibration_override,sensor_positions,file,recorded_sha256,"
                "sha256_matches,truncated\n";

    std::string archive;
    for (const auto& instance : inputs.instances) {
        std::string positions;
        for (const auto& [streamId, position] : instance.sensorPositions) {
            if (!positions.empty()) positions += "; ";
            positions += streamId + "=" + position;
        }
        const std::string attribution = instance.participantUnrecorded
            ? "unrecorded"
            : (instance.participantBackfilled ? "backfilled" : "captured");

        for (const auto& artifact : instance.artifacts) {
            std::string body;
            {
                std::ifstream file(artifact.absolutePath, std::ios::binary);
                if (!file) {
                    result.skipCount += 1;
                    manifest << csvCell(instance.participantId) << ","
                             << attribution << ","
                             << csvCell(instance.experimentId) << ","
                             << csvCell(instance.instanceId) << ","
                             << instance.totalRows << ","
                             << csvCell(instance.calibrationOverride) << ","
                             << csvCell(positions) << ","
                             << csvCell(artifact.archivePath)
                             << ",," << "FILE MISSING" << ",\n";
                    continue;
                }
                std::ostringstream buffer;
                buffer << file.rdbuf();
                body = buffer.str();
            }

            std::string matches = "n/a";
            if (!artifact.recordedSha256.empty()) {
                const bool ok = sha256Hex(body) == artifact.recordedSha256;
                matches = ok ? "yes" : "NO";
                if (!ok) result.checksumMismatchCount += 1;
            }

            appendEntry(archive, artifact.archivePath, body);
            result.fileCount += 1;

            manifest << csvCell(instance.participantId) << ","
                     << attribution << ","
                     << csvCell(instance.experimentId) << ","
                     << csvCell(instance.instanceId) << ","
                     << instance.totalRows << ","
                     << csvCell(instance.calibrationOverride) << ","
                     << csvCell(positions) << ","
                     << csvCell(artifact.archivePath) << ","
                     << artifact.recordedSha256 << ","
                     << matches << ","
                     << (artifact.truncated ? "yes" : "no") << "\n";
        }
        result.instanceCount += 1;
    }

    manifest << "\n# SKIPPED — not in this archive\n";
    if (inputs.skips.empty()) {
        manifest << "# (none)\n";
    } else {
        manifest << "experiment_id,instance_id,reason\n";
        for (const auto& skip : inputs.skips) {
            manifest << csvCell(skip.experimentId) << ","
                     << csvCell(skip.instanceId) << ","
                     << csvCell(skip.reason) << "\n";
        }
    }
    result.skipCount += inputs.skips.size();

    // The manifest goes in FIRST so that `tar -tf` and any partial read surface it
    // before the data it describes.
    std::string finalArchive;
    appendEntry(finalArchive, "MANIFEST.csv", manifest.str());
    finalArchive += archive;
    finalArchive.append(2 * kBlock, '\0');  // end-of-archive

    result.archive = std::move(finalArchive);
    result.fileName = "cohort-" + (inputs.workspaceId.empty() ? std::string("unfiled")
                                                             : inputs.workspaceId) + ".tar";
    result.ok = true;
    return result;
}

}  // namespace natkit::tools
