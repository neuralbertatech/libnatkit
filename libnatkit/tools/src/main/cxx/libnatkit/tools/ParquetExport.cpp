#include "ParquetExport.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <thread>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include "libnatkit-core.hpp"
#include "libnatkit-kafka.hpp"

#include "StreamViewerWebSocket.hpp"  // projectRecordToChannelFrame

#if NATKIT_HAVE_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#endif

namespace natkit::tools {
namespace {

// Marker/meta consumers read from the START of the topic (RdKafka
// OFFSET_BEGINNING). An export is inherently historical, so both the data and
// the marker topic are drained from the beginning.
constexpr int64_t kStartOffsetBeginning = -2;

constexpr const char* kCueMarkerType = "cue";
constexpr const char* kSessionMarkerType = "session";

// Attribute keys that carry a cue's CLASS, most-preferred first. "gesture" is
// the historical EMG name the frontend still publishes; "label"/"class" are the
// generic names it is migrating to. Checking all three keeps exports working
// across that rename.
constexpr const char* kCueLabelKeys[] = {"gesture", "label", "class"};

struct DecodedMarker {
    std::string sessionId;
    std::string markerType;
    std::string markerId;
    std::string event;
    std::string label;
    int64_t emittedAtUs = 0;
    nlohmann::json attributes = nlohmann::json::object();
};

struct ExportFrame {
    std::string deviceId;
    uint64_t seqNo = 0;
    int64_t deviceTsUs = 0;
    uint32_t sampleRateHz = 0;
    uint32_t samplesPerChannel = 0;
    std::vector<std::string> channelLabels;
    // Row-major: channel-major runs of samplesPerChannel values.
    std::vector<float> samples;
};

// A closed [start,end] cue window plus the class it labels.
struct LabelInterval {
    int64_t startUs = 0;
    int64_t endUs = 0;
    std::string label;
    nlohmann::json attributes;
};

std::string cueLabelOf(const DecodedMarker& marker)
{
    for (const char* key : kCueLabelKeys) {
        if (marker.attributes.is_object() && marker.attributes.contains(key)) {
            const auto& value = marker.attributes.at(key);
            if (value.is_string() && !value.get<std::string>().empty()) {
                return value.get<std::string>();
            }
            if (value.is_number()) {
                return value.dump();
            }
        }
    }
    return marker.label;
}

// Pair start/end markers by marker_id into closed intervals, mirroring
// natvr.markers.build_marker_intervals so an exported dataset and a trained
// model agree on which frame belongs to which cue.
std::vector<LabelInterval> buildIntervals(
    const std::vector<DecodedMarker>& markers, const std::string& markerType)
{
    std::vector<const DecodedMarker*> ordered;
    ordered.reserve(markers.size());
    for (const auto& marker : markers) {
        if (marker.markerType == markerType) {
            ordered.push_back(&marker);
        }
    }
    std::stable_sort(ordered.begin(), ordered.end(),
                     [](const DecodedMarker* a, const DecodedMarker* b) {
                         return a->emittedAtUs < b->emittedAtUs;
                     });

    std::unordered_map<std::string, std::vector<const DecodedMarker*>> open;
    std::vector<LabelInterval> intervals;
    for (const auto* marker : ordered) {
        if (marker->event == "end") {
            auto search = open.find(marker->markerId);
            if (search == open.end() || search->second.empty()) {
                continue;
            }
            const auto* start = search->second.back();
            search->second.pop_back();
            if (marker->emittedAtUs < start->emittedAtUs) {
                continue;
            }
            intervals.push_back(LabelInterval{start->emittedAtUs,
                                              marker->emittedAtUs,
                                              cueLabelOf(*start),
                                              start->attributes});
            continue;
        }
        open[marker->markerId].push_back(marker);
    }
    return intervals;
}

// Drain one topic until it goes idle for idleTimeoutMs, calling `onRecord` for
// each decoded record. Returns false only on a messenger failure.
// onRecord returns false to stop the drain (e.g. we have read past the window).
size_t drainTopic(const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
                  const std::shared_ptr<nat::core::BasicTopicInformation>& topic,
                  int64_t startOffset,
                  int firstRecordTimeoutMs,
                  int idleTimeoutMs,
                  int maxDurationMs,
                  size_t maxRecords,
                  bool& opened,
                  bool& hitBudget,
                  const std::function<bool(nat::core::Schema&)>& onRecord)
{
    hitBudget = false;
    opened = false;
    if (topic == nullptr) {
        return 0;
    }
    auto messenger = brokerManager->createMessenger(topic, startOffset);
    if (messenger == nullptr) {
        return 0;
    }
    opened = true;

    // Three clocks: a generous one for the first record (consumer connect +
    // metadata + assignment), a short idle window once data is flowing, and a
    // hard overall budget so a live stream cannot drain forever.
    const auto hardDeadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(maxDurationMs);
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(firstRecordTimeoutMs);
    size_t seen = 0;

    while (seen < maxRecords) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= hardDeadline) {
            hitBudget = true;
            break;
        }
        if (now >= deadline) {
            break;
        }
        auto message = messenger->tryGetNexMessage();
        if (!message.has_value() || message.value() == nullptr) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }
        const bool keepGoing = onRecord(*message.value());
        ++seen;
        if (!keepGoing) {
            break;
        }
        deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(idleTimeoutMs);
    }
    return seen;
}

// Project a record onto channel frames using the SHARED projection in
// StreamViewerWebSocket (canonical contract first, then any alternate input
// mapping). Reusing it keeps export supporting exactly the sensor set that
// transforms and viewers do -- notably IMU and Muse, whose descriptors don't
// literally match the contract.
std::optional<ExportFrame> normalizeFrame(nat::core::Schema& record,
                                         uint64_t sourceStreamId)
{
    const auto projected = projectRecordToChannelFrame(record, sourceStreamId);
    if (!projected.has_value()) {
        return std::nullopt;
    }
    const auto& source = projected.value();
    if (source.channelLabels.empty() || source.samplesPerChannel == 0) {
        return std::nullopt;
    }

    ExportFrame frame;
    frame.deviceId = source.deviceId;
    frame.seqNo = source.seqNo;
    frame.deviceTsUs = static_cast<int64_t>(source.deviceTsUs);
    frame.sampleRateHz = source.sampleRateHz;
    frame.samplesPerChannel = source.samplesPerChannel;
    frame.channelLabels = source.channelLabels;
    frame.samples = source.samples;
    return frame;
}

std::string sanitizeForFileName(const std::string& value)
{
    std::string out;
    out.reserve(value.size());
    for (const char ch : value) {
        out.push_back((std::isalnum(static_cast<unsigned char>(ch)) != 0 ||
                       ch == '-' || ch == '_')
                          ? ch
                          : '-');
    }
    if (out.empty()) {
        out = "stream";
    }
    return out;
}

}  // namespace

bool parquetExportAvailable()
{
#if NATKIT_HAVE_PARQUET
    return true;
#else
    return false;
#endif
}

ParquetExportResult exportStreamToParquet(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ParquetExportRequest& request,
    const std::string& outputDir)
{
    ParquetExportResult result;

#if !NATKIT_HAVE_PARQUET
    (void)brokerManager;
    (void)request;
    (void)outputDir;
    result.error =
        "This backend was built without Parquet support (libparquet-dev was "
        "missing at build time).";
    return result;
#else
    if (brokerManager == nullptr) {
        result.error = "Broker is not available.";
        return result;
    }

    // Resolve the channel's DATA and MARKER topics. They share a stream id when
    // a combine node bundled them, which is exactly the data+markers join.
    // Markers come from this channel's own MARKER topic (the combine-bundle
    // case) unless an explicit marker channel was named — which lets an export
    // node take a data stream and an experiment as two separate inputs.
    const uint64_t markerChannelId = request.markerStreamId != 0
                                         ? request.markerStreamId
                                         : request.streamId;

    std::shared_ptr<nat::core::BasicTopicInformation> dataTopic;
    std::shared_ptr<nat::core::BasicTopicInformation> markerTopic;
    for (auto& topic : brokerManager->getAllTopics()) {
        if (topic == nullptr) {
            continue;
        }
        const bool isDataChannel = topic->id == request.streamId;
        const bool isMarkerChannel = topic->id == markerChannelId;
        if (!isDataChannel && !isMarkerChannel) {
            continue;
        }
        std::shared_ptr<nat::core::BasicTopicInformation> shared(topic.release());
        if (isDataChannel && shared->type == nat::core::StreamType::DATA &&
            dataTopic == nullptr) {
            dataTopic = shared;
        } else if (isMarkerChannel &&
                   shared->type == nat::core::StreamType::MARKER &&
                   markerTopic == nullptr) {
            markerTopic = shared;
        }
    }

    if (dataTopic == nullptr) {
        result.error =
            "No DATA topic found for stream " + std::to_string(request.streamId) +
            ". Start the graph so the stream exists, and check broker retention.";
        return result;
    }

    // --- markers: session window + cue labels -----------------------------
    std::vector<DecodedMarker> markers;
    if (markerTopic != nullptr) {
        bool markerOpened = false;
        bool markerHitBudget = false;
        drainTopic(
            brokerManager, markerTopic, kStartOffsetBeginning,
            request.firstRecordTimeoutMs, request.idleTimeoutMs,
            request.maxDurationMs, 500'000, markerOpened, markerHitBudget,
            [&markers](nat::core::Schema& record) -> bool {
                auto* marker = dynamic_cast<nat::core::MarkerEventV1*>(&record);
                if (marker == nullptr) {
                    return true;
                }
                DecodedMarker decoded;
                decoded.sessionId = marker->getSessionId();
                decoded.markerType = marker->getMarkerType();
                decoded.markerId = marker->getMarkerId();
                decoded.event = marker->getEvent();
                decoded.label = marker->getLabel();
                decoded.emittedAtUs = static_cast<int64_t>(marker->getEmittedAtUs());
                if (!marker->getAttributesJson().empty()) {
                    decoded.attributes = nlohmann::json::parse(
                        marker->getAttributesJson(), nullptr, false);
                    if (decoded.attributes.is_discarded()) {
                        decoded.attributes = nlohmann::json::object();
                    }
                }
                markers.push_back(std::move(decoded));
                return true;
            });
        if (!markerOpened) {
            result.error = "Failed to open the marker topic for this stream.";
            return result;
        }
    }
    result.markerCount = markers.size();
    if (!markers.empty()) {
        result.sessionId = markers.front().sessionId;
    }
    result.schemaName = dataTopic->schemaName;

    // Session window: the lifecycle marker pair(s). runIndex picks one run.
    std::optional<int64_t> windowStart = request.startUs;
    std::optional<int64_t> windowEnd = request.endUs;
    const auto sessionIntervals = buildIntervals(markers, kSessionMarkerType);
    if (!windowStart.has_value() && !sessionIntervals.empty()) {
        if (request.runIndex > 0) {
            if (static_cast<size_t>(request.runIndex) > sessionIntervals.size()) {
                result.error = "run_index " + std::to_string(request.runIndex) +
                               " is out of range; the session has " +
                               std::to_string(sessionIntervals.size()) + " run(s).";
                return result;
            }
            windowStart = sessionIntervals[request.runIndex - 1].startUs;
            windowEnd = sessionIntervals[request.runIndex - 1].endUs;
        } else {
            windowStart = sessionIntervals.front().startUs;
            windowEnd = sessionIntervals.back().endUs;
        }
    }
    result.windowStartUs = windowStart;
    result.windowEndUs = windowEnd;

    // --- data: SEEK to the window, then drain until past it -----------------
    //
    // Reading from OFFSET_BEGINNING is hopeless on a busy topic: a live IMU
    // stream accumulates ~1e6 records, and the time budget expired ~600k records
    // short of a session recorded minutes ago -- every frame read was hours too
    // old, so the export reported "no frames inside the window" while sitting on
    // plenty of data. Resolve a start offset from the window instead.
    //
    // Kafka's offsetsForTimes keys on the BROKER append timestamp, while the
    // window comes from in-payload device_ts_us. Those track each other closely
    // (the bridge publishes immediately) but are not identical, so rewind by a
    // slack margin rather than landing exactly on the boundary.
    int64_t dataStartOffset = kStartOffsetBeginning;
    if (windowStart.has_value()) {
        constexpr int64_t kSeekSlackUs = 10'000'000;  // 10s
        const auto seekFromUs =
            std::max<int64_t>(0, windowStart.value() - kSeekSlackUs);
        const auto extent = brokerManager->queryStreamTime(
            dataTopic->toTopicString(), seekFromUs);
        if (extent.valid && extent.offsetForTimestamp >= 0) {
            dataStartOffset = extent.offsetForTimestamp;
        }
        // Invalid extent (or no record at/after that time) => fall back to
        // OFFSET_BEGINNING rather than guessing.
    }

    std::vector<ExportFrame> frames;
    size_t undecodable = 0;
    size_t outsideWindow = 0;
    size_t consecutivePastEnd = 0;
    bool dataOpened = false;
    bool dataHitBudget = false;
    const size_t dataRecordsSeen = drainTopic(
        brokerManager, dataTopic, dataStartOffset, request.firstRecordTimeoutMs,
        request.idleTimeoutMs, request.maxDurationMs, request.maxFrames,
        dataOpened, dataHitBudget,
        [&](nat::core::Schema& record) -> bool {
            auto frame = normalizeFrame(record, request.streamId);
            if (!frame.has_value()) {
                ++undecodable;
                return true;
            }
            if (windowStart.has_value() &&
                frame->deviceTsUs < windowStart.value()) {
                ++outsideWindow;
                return true;
            }
            if (windowEnd.has_value() && frame->deviceTsUs > windowEnd.value()) {
                ++outsideWindow;
                // Past the window: stop, but tolerate mild reordering first so a
                // single out-of-order frame can't truncate the export.
                return ++consecutivePastEnd < 500;
            }
            consecutivePastEnd = 0;
            frames.push_back(std::move(frame.value()));
            return true;
        });
    if (!dataOpened) {
        result.error = "Failed to open the data topic for this stream.";
        return result;
    }

    if (frames.empty()) {
        // Say which of the three ways this went wrong, so the operator isn't
        // left guessing between "nothing on the topic", "wrong schema" and
        // "wrong window".
        if (dataRecordsSeen == 0) {
            result.error =
                "No records on the data topic for stream " +
                std::to_string(request.streamId) +
                ". Is the graph running / has this stream ever published? "
                "(Kafka retention also applies.)";
        } else if (undecodable > 0 && outsideWindow == 0) {
            result.error =
                "No exportable frames: " + std::to_string(undecodable) +
                " record(s) did not match the canonical channel-frame contract "
                "(only numeric channel frames can be exported).";
        } else {
            result.error =
                "No data frames inside the session window (" +
                std::to_string(outsideWindow) + " of " +
                std::to_string(dataRecordsSeen) + " scanned frame(s) fell " +
                "outside it, seeking from offset " +
                std::to_string(dataStartOffset) + "). The stream has data but " +
                "none of it overlaps the experiment's window -- check that the " +
                "device was publishing during the run, or pass explicit " +
                "start_us/end_us.";
        }
        return result;
    }

    std::stable_sort(frames.begin(), frames.end(),
                     [](const ExportFrame& a, const ExportFrame& b) {
                         return std::tie(a.deviceTsUs, a.seqNo) <
                                std::tie(b.deviceTsUs, b.seqNo);
                     });
    result.frameCount = frames.size();
    result.truncated = dataHitBudget || frames.size() >= request.maxFrames;

    // --- label join: the SAME stitcher the training path uses --------------
    // Markers sidecar (instance materialization). Written in timestamp order and
    // CLIPPED TO THE RESOLVED WINDOW.
    //
    // Clipping matters: one experiment publishes every run to the SAME
    // Marker/<experiment_id> topic, so an unclipped sidecar hands an instance the
    // whole session's marker history — other runs included. Replaying that instance
    // would then emit markers belonging to runs it does not contain, and a
    // re-export of the replayed result would join labels that were never part of
    // this recording. An instance has to be self-contained.
    //
    // Written before the (much longer) data drain, so a snapshot that fails partway
    // still has its timeline on disk.
    if (!request.markersSidecarPath.empty() && !markers.empty()) {
        std::vector<DecodedMarker> ordered;
        ordered.reserve(markers.size());
        for (const auto& marker : markers) {
            const bool afterStart =
                !windowStart.has_value() || marker.emittedAtUs >= windowStart.value();
            const bool beforeEnd =
                !windowEnd.has_value() || marker.emittedAtUs <= windowEnd.value();
            if (afterStart && beforeEnd) {
                ordered.push_back(marker);
            }
        }
        std::stable_sort(ordered.begin(), ordered.end(),
                         [](const DecodedMarker& left, const DecodedMarker& right) {
                             return left.emittedAtUs < right.emittedAtUs;
                         });
        const std::filesystem::path sidecar(request.markersSidecarPath);
        std::error_code dir_error;
        if (sidecar.has_parent_path()) {
            std::filesystem::create_directories(sidecar.parent_path(), dir_error);
        }
        std::ofstream out(sidecar, std::ios::binary | std::ios::trunc);
        if (!out) {
            result.error = "Failed to open the markers sidecar for write: " +
                           request.markersSidecarPath;
            return result;
        }
        for (const auto& marker : ordered) {
            nlohmann::json line = {
                {"session_id", marker.sessionId},
                {"marker_type", marker.markerType},
                {"marker_id", marker.markerId},
                {"event", marker.event},
                {"label", marker.label},
                {"emitted_at_us", marker.emittedAtUs},
                {"attributes", marker.attributes},
            };
            out << line.dump() << "\n";
        }
        if (!out.good()) {
            result.error =
                "Failed while writing the markers sidecar: " + request.markersSidecarPath;
            return result;
        }
        result.markersSidecarWritten = true;
        result.markersSidecarCount = ordered.size();
    }

    const auto cueIntervals = buildIntervals(markers, kCueMarkerType);
    std::vector<int32_t> assignments(frames.size(), -1);
    if (!cueIntervals.empty()) {
        std::vector<int64_t> timestamps;
        timestamps.reserve(frames.size());
        for (const auto& frame : frames) {
            timestamps.push_back(frame.deviceTsUs);
        }
        std::vector<nat::core::TimelineInterval> timeline;
        timeline.reserve(cueIntervals.size());
        for (size_t index = 0; index < cueIntervals.size(); ++index) {
            timeline.push_back(nat::core::TimelineInterval{
                cueIntervals[index].startUs, cueIntervals[index].endUs,
                static_cast<int32_t>(index)});
        }
        assignments = nat::core::assignIntervalsToTimeline(timestamps, timeline, -1);
    }

    // --- build the Arrow table --------------------------------------------
    const size_t rowCount = frames.size();
    const size_t channelCount = frames.front().channelLabels.size();

    arrow::StringBuilder deviceIdBuilder;
    arrow::UInt64Builder seqNoBuilder;
    arrow::Int64Builder deviceTsBuilder;
    arrow::UInt32Builder sampleRateBuilder;
    arrow::UInt32Builder nChannelsBuilder;
    arrow::UInt32Builder samplesPerChannelBuilder;
    arrow::StringBuilder labelBuilder;
    arrow::StringBuilder labelPhaseBuilder;
    arrow::Int64Builder labelCueIdBuilder;

    std::vector<std::shared_ptr<arrow::ListBuilder>> channelBuilders;
    std::vector<arrow::FloatBuilder*> channelValueBuilders;
    channelBuilders.reserve(channelCount);
    channelValueBuilders.reserve(channelCount);
    for (size_t index = 0; index < channelCount; ++index) {
        auto valueBuilder = std::make_shared<arrow::FloatBuilder>();
        channelValueBuilders.push_back(valueBuilder.get());
        channelBuilders.push_back(
            std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                                 valueBuilder));
    }

    const auto arrowFail = [&result](const arrow::Status& status,
                                     const char* what) {
        result.error = std::string("Parquet export failed (") + what +
                       "): " + status.ToString();
        return result;
    };

    for (size_t row = 0; row < rowCount; ++row) {
        const auto& frame = frames[row];
        auto status = deviceIdBuilder.Append(frame.deviceId);
        if (!status.ok()) return arrowFail(status, "device_id");
        status = seqNoBuilder.Append(frame.seqNo);
        if (!status.ok()) return arrowFail(status, "seq_no");
        status = deviceTsBuilder.Append(frame.deviceTsUs);
        if (!status.ok()) return arrowFail(status, "device_ts_us");
        status = sampleRateBuilder.Append(frame.sampleRateHz);
        if (!status.ok()) return arrowFail(status, "sample_rate_hz");
        status = nChannelsBuilder.Append(
            static_cast<uint32_t>(frame.channelLabels.size()));
        if (!status.ok()) return arrowFail(status, "n_channels");
        status = samplesPerChannelBuilder.Append(frame.samplesPerChannel);
        if (!status.ok()) return arrowFail(status, "samples_per_channel");

        const int32_t assignment = assignments[row];
        if (assignment >= 0 &&
            static_cast<size_t>(assignment) < cueIntervals.size()) {
            const auto& interval = cueIntervals[assignment];
            status = labelBuilder.Append(interval.label);
            if (!status.ok()) return arrowFail(status, "label");
            ++result.labelledFrameCount;
            ++result.labelCounts[interval.label];

            const auto& attributes = interval.attributes;
            if (attributes.is_object() && attributes.contains("phase") &&
                attributes.at("phase").is_string()) {
                status = labelPhaseBuilder.Append(
                    attributes.at("phase").get<std::string>());
            } else {
                status = labelPhaseBuilder.AppendNull();
            }
            if (!status.ok()) return arrowFail(status, "label_phase");

            if (attributes.is_object() && attributes.contains("cue_id") &&
                attributes.at("cue_id").is_number_integer()) {
                status = labelCueIdBuilder.Append(
                    attributes.at("cue_id").get<int64_t>());
            } else {
                status = labelCueIdBuilder.AppendNull();
            }
            if (!status.ok()) return arrowFail(status, "label_cue_id");
        } else {
            // Inside the session but between cues (lead-in, inter-cue rest).
            // Counted under the empty key so a review can see how much of a run
            // carries no class at all.
            ++result.labelCounts[std::string{}];
            status = labelBuilder.AppendNull();
            if (!status.ok()) return arrowFail(status, "label");
            status = labelPhaseBuilder.AppendNull();
            if (!status.ok()) return arrowFail(status, "label_phase");
            status = labelCueIdBuilder.AppendNull();
            if (!status.ok()) return arrowFail(status, "label_cue_id");
        }

        // One list column per channel, matching natVR's channel_<i> convention.
        for (size_t channel = 0; channel < channelCount; ++channel) {
            status = channelBuilders[channel]->Append();
            if (!status.ok()) return arrowFail(status, "channel list");
            if (channel >= frame.channelLabels.size()) {
                continue;  // ragged frame: leave the list empty
            }
            const size_t offset = channel * frame.samplesPerChannel;
            for (uint32_t sample = 0; sample < frame.samplesPerChannel; ++sample) {
                const size_t index = offset + sample;
                if (index >= frame.samples.size()) {
                    break;
                }
                status = channelValueBuilders[channel]->Append(frame.samples[index]);
                if (!status.ok()) return arrowFail(status, "channel sample");
            }
        }
    }

    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    const auto finishInto = [&](arrow::ArrayBuilder& builder,
                                const std::string& name,
                                const std::shared_ptr<arrow::DataType>& type) {
        std::shared_ptr<arrow::Array> array;
        const auto status = builder.Finish(&array);
        if (!status.ok()) {
            return false;
        }
        fields.push_back(arrow::field(name, type));
        arrays.push_back(array);
        return true;
    };

    if (!finishInto(deviceIdBuilder, "device_id", arrow::utf8()) ||
        !finishInto(seqNoBuilder, "seq_no", arrow::uint64()) ||
        !finishInto(deviceTsBuilder, "device_ts_us", arrow::int64()) ||
        !finishInto(sampleRateBuilder, "sample_rate_hz", arrow::uint32()) ||
        !finishInto(nChannelsBuilder, "n_channels", arrow::uint32()) ||
        !finishInto(samplesPerChannelBuilder, "samples_per_channel",
                    arrow::uint32())) {
        result.error = "Parquet export failed while finalizing frame columns.";
        return result;
    }

    for (size_t channel = 0; channel < channelCount; ++channel) {
        const std::string name = "channel_" + std::to_string(channel);
        if (!finishInto(*channelBuilders[channel], name,
                        arrow::list(arrow::float32()))) {
            result.error = "Parquet export failed while finalizing " + name + ".";
            return result;
        }
    }

    const std::string labelField =
        request.labelField.empty() ? "label" : request.labelField;
    if (!finishInto(labelBuilder, labelField, arrow::utf8()) ||
        !finishInto(labelCueIdBuilder, labelField + "_cue_id", arrow::int64()) ||
        !finishInto(labelPhaseBuilder, labelField + "_phase", arrow::utf8())) {
        result.error = "Parquet export failed while finalizing label columns.";
        return result;
    }

    // Channel labels travel as schema metadata rather than a repeated column.
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    metadata->Append("natkit.stream_id", std::to_string(request.streamId));
    metadata->Append("natkit.session_id", result.sessionId);
    metadata->Append("natkit.channel_labels",
                     nlohmann::json(frames.front().channelLabels).dump());
    metadata->Append("natkit.marker_count", std::to_string(result.markerCount));
    // The ORIGINAL sensor schema. Replay re-emits the canonical channel frame, so
    // it does not need this -- but a snapshot that cannot say what sensor produced
    // it is not much of a provenance record.
    metadata->Append("natkit.schema_name", result.schemaName);
    if (windowStart.has_value()) {
        metadata->Append("natkit.window_start_us",
                         std::to_string(windowStart.value()));
    }
    if (windowEnd.has_value()) {
        metadata->Append("natkit.window_end_us",
                         std::to_string(windowEnd.value()));
    }

    auto schema = arrow::schema(fields, metadata);
    auto table = arrow::Table::Make(schema, arrays,
                                    static_cast<int64_t>(rowCount));
    if (table == nullptr) {
        result.error = "Parquet export failed to assemble the table.";
        return result;
    }

    // <session>__<device>, but drop the device half when it adds nothing: a
    // schema with no device_id of its own gets a synthesized "stream-<id>",
    // which would otherwise produce "stream-123__stream-123.parquet".
    const std::string sessionPart =
        result.sessionId.empty()
            ? std::string("stream-") + std::to_string(request.streamId)
            : sanitizeForFileName(result.sessionId);
    const std::string devicePart = sanitizeForFileName(frames.front().deviceId);
    const std::string stem =
        (devicePart.empty() || devicePart == sessionPart || devicePart == "stream")
            ? sessionPart
            : sessionPart + "__" + devicePart;
    result.fileName = stem + ".parquet";
    result.filePath = outputDir + "/" + stem + "-" +
                      std::to_string(request.streamId) + ".parquet";

    auto outfileResult = arrow::io::FileOutputStream::Open(result.filePath);
    if (!outfileResult.ok()) {
        result.error = "Could not open the export file for writing: " +
                       outfileResult.status().ToString();
        return result;
    }
    // store_schema() is required for the Arrow schema metadata (channel labels,
    // session id, window) to be written into the Parquet key-value metadata —
    // without it the table's metadata is silently dropped on write.
    const auto arrowProperties =
        parquet::ArrowWriterProperties::Builder().store_schema()->build();
    const auto writeStatus = parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), *outfileResult,
        /*chunk_size=*/std::max<int64_t>(1, static_cast<int64_t>(rowCount)),
        parquet::default_writer_properties(), arrowProperties);
    if (!writeStatus.ok()) {
        result.error = "Parquet write failed: " + writeStatus.ToString();
        return result;
    }

    result.ok = true;
    return result;
#endif
}

}  // namespace natkit::tools
