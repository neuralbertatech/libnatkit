#include "ReplaySource.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <thread>

#include <nlohmann/json.hpp>

#include "libnatkit-core.hpp"
#include "libnatkit-kafka.hpp"

#include "GraphTopicResolution.hpp"  // makeTopicInfo — one definition of topic identity

#if NATKIT_HAVE_PARQUET
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#endif

namespace natkit::tools {

namespace {

#if NATKIT_HAVE_PARQUET

// One replayed frame, already in the canonical channel-frame shape. Samples are
// channel-major (channelLabels.size() runs of samplesPerChannel), matching both
// what the exporter wrote and what NatSignalFrameDataSchemaV1 expects.
struct ReplayFrame {
    int64_t deviceTsUs = 0;
    std::string deviceId;
    uint64_t seqNo = 0;
    uint32_t sampleRateHz = 0;
    uint32_t samplesPerChannel = 0;
    std::vector<float> samples;
};

struct LoadedSource {
    uint64_t originalStreamId = 0;
    std::vector<std::string> channelLabels;
    std::vector<ReplayFrame> frames;
};

// One item on the merged timeline: either a data frame from a source, or a marker.
// Replay drives a SINGLE clock over this merged sequence, because two streams
// recorded together must replay together — ordering them file-by-file would
// desynchronise them, and markers that arrive after the frames they label would
// leave the overlay and the label join wrong.
struct TimelineItem {
    int64_t tsUs = 0;
    // -1 = marker, else index into the sources vector.
    int sourceIndex = -1;
    size_t itemIndex = 0;
};

std::string arrowError(const arrow::Status& status, const char* what)
{
    return std::string("Replay failed (") + what + "): " + status.ToString();
}

// Read the channel labels the exporter stashed in schema metadata (they travel
// there rather than as a repeated column).
std::vector<std::string> readChannelLabels(const arrow::Schema& schema)
{
    std::vector<std::string> labels;
    const auto metadata = schema.metadata();
    if (metadata == nullptr) {
        return labels;
    }
    const auto index = metadata->FindKey("natkit.channel_labels");
    if (index < 0) {
        return labels;
    }
    const auto parsed = nlohmann::json::parse(metadata->value(index), nullptr, false);
    if (parsed.is_discarded() || !parsed.is_array()) {
        return labels;
    }
    for (const auto& entry : parsed) {
        if (entry.is_string()) {
            labels.push_back(entry.get<std::string>());
        }
    }
    return labels;
}

bool loadParquet(const std::string& path, LoadedSource& out, std::string& error)
{
    auto input_result = arrow::io::ReadableFile::Open(path);
    if (!input_result.ok()) {
        error = "Could not open " + path + ": " + input_result.status().ToString();
        return false;
    }

    // This Arrow returns a Result<unique_ptr<FileReader>> rather than filling an
    // out-param.
    auto reader_result =
        parquet::arrow::OpenFile(*input_result, arrow::default_memory_pool());
    if (!reader_result.ok()) {
        error = arrowError(reader_result.status(), "opening parquet");
        return false;
    }
    auto reader = std::move(*reader_result);
    std::shared_ptr<arrow::Table> table;
    const auto status = reader->ReadTable(&table);
    if (!status.ok()) {
        error = arrowError(status, "reading table");
        return false;
    }
    if (table == nullptr || table->num_rows() == 0) {
        error = "The instance artifact " + path + " has no rows.";
        return false;
    }

    // A table read from Parquet can be chunked; combining once keeps the row loop
    // simple and is cheap next to the file read itself.
    auto combined = table->CombineChunks(arrow::default_memory_pool());
    if (!combined.ok()) {
        error = arrowError(combined.status(), "combining chunks");
        return false;
    }
    table = *combined;

    out.channelLabels = readChannelLabels(*table->schema());
    if (out.channelLabels.empty()) {
        error = "The instance artifact " + path +
                " has no natkit.channel_labels metadata — it was not written by "
                "this exporter, so its channels cannot be reconstructed.";
        return false;
    }

    const auto column = [&table](const char* name) -> std::shared_ptr<arrow::Array> {
        const auto index = table->schema()->GetFieldIndex(name);
        if (index < 0) {
            return nullptr;
        }
        return table->column(index)->chunk(0);
    };

    auto device_ids = std::dynamic_pointer_cast<arrow::StringArray>(column("device_id"));
    auto seq_nos = std::dynamic_pointer_cast<arrow::UInt64Array>(column("seq_no"));
    auto timestamps =
        std::dynamic_pointer_cast<arrow::Int64Array>(column("device_ts_us"));
    auto sample_rates =
        std::dynamic_pointer_cast<arrow::UInt32Array>(column("sample_rate_hz"));
    auto samples_per_channel =
        std::dynamic_pointer_cast<arrow::UInt32Array>(column("samples_per_channel"));
    if (timestamps == nullptr || samples_per_channel == nullptr) {
        error = "The instance artifact " + path +
                " is missing the device_ts_us / samples_per_channel columns.";
        return false;
    }

    std::vector<std::shared_ptr<arrow::ListArray>> channels;
    channels.reserve(out.channelLabels.size());
    for (size_t index = 0; index < out.channelLabels.size(); ++index) {
        auto channel = std::dynamic_pointer_cast<arrow::ListArray>(
            column(("channel_" + std::to_string(index)).c_str()));
        if (channel == nullptr) {
            error = "The instance artifact " + path + " is missing channel_" +
                    std::to_string(index) + ".";
            return false;
        }
        channels.push_back(channel);
    }

    const auto rows = static_cast<int64_t>(table->num_rows());
    out.frames.reserve(static_cast<size_t>(rows));
    for (int64_t row = 0; row < rows; ++row) {
        ReplayFrame frame;
        // device_ts_us is preserved EXACTLY. Restamping to "now" would silently
        // destroy every label (the cue join is a timestamp interval join) and
        // desynchronise two streams replayed together.
        frame.deviceTsUs = timestamps->Value(row);
        frame.deviceId = device_ids != nullptr && device_ids->IsValid(row)
                             ? device_ids->GetString(row)
                             : std::string{};
        frame.seqNo = seq_nos != nullptr ? seq_nos->Value(row) : 0;
        frame.sampleRateHz = sample_rates != nullptr ? sample_rates->Value(row) : 0;
        frame.samplesPerChannel = samples_per_channel->Value(row);
        frame.samples.reserve(out.channelLabels.size() * frame.samplesPerChannel);
        for (const auto& channel : channels) {
            const auto values =
                std::dynamic_pointer_cast<arrow::FloatArray>(channel->values());
            if (values == nullptr) {
                error = "Channel samples in " + path + " are not float32.";
                return false;
            }
            const auto offset = channel->value_offset(row);
            const auto length = channel->value_length(row);
            for (int32_t index = 0; index < length; ++index) {
                frame.samples.push_back(values->Value(offset + index));
            }
        }
        out.frames.push_back(std::move(frame));
    }
    return true;
}

std::vector<nlohmann::json> loadMarkers(const std::string& path)
{
    std::vector<nlohmann::json> markers;
    if (path.empty()) {
        return markers;
    }
    std::ifstream input(path);
    if (!input) {
        return markers;
    }
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        auto parsed = nlohmann::json::parse(line, nullptr, false);
        if (!parsed.is_discarded() && parsed.is_object()) {
            markers.push_back(std::move(parsed));
        }
    }
    std::stable_sort(markers.begin(), markers.end(),
                     [](const nlohmann::json& left, const nlohmann::json& right) {
                         return left.value("emitted_at_us", int64_t{0}) <
                                right.value("emitted_at_us", int64_t{0});
                     });
    return markers;
}

// Everything the plan needs, cached so runReplay doesn't re-read the files.
struct LoadedReplay {
    std::vector<LoadedSource> sources;
    std::vector<nlohmann::json> markers;
    std::vector<TimelineItem> timeline;
};

bool loadReplay(const ReplayRequest& request, LoadedReplay& out, std::string& error)
{
    size_t total = 0;
    for (const auto& spec : request.sources) {
        LoadedSource loaded;
        loaded.originalStreamId = spec.originalStreamId;
        if (!loadParquet(spec.parquetPath, loaded, error)) {
            return false;
        }
        total += loaded.frames.size();
        if (total > request.maxFrames) {
            error = "This instance holds more than " +
                    std::to_string(request.maxFrames) +
                    " frames, which replay will not load into memory at once.";
            return false;
        }
        out.sources.push_back(std::move(loaded));
    }
    out.markers = loadMarkers(request.markersPath);

    // Merge everything onto ONE timeline ordered by the original timestamps.
    for (size_t index = 0; index < out.sources.size(); ++index) {
        for (size_t frame = 0; frame < out.sources[index].frames.size(); ++frame) {
            out.timeline.push_back(TimelineItem{
                out.sources[index].frames[frame].deviceTsUs,
                static_cast<int>(index), frame});
        }
    }
    for (size_t index = 0; index < out.markers.size(); ++index) {
        out.timeline.push_back(TimelineItem{
            out.markers[index].value("emitted_at_us", int64_t{0}), -1, index});
    }
    std::stable_sort(out.timeline.begin(), out.timeline.end(),
                     [](const TimelineItem& left, const TimelineItem& right) {
                         return left.tsUs < right.tsUs;
                     });
    if (out.timeline.empty()) {
        error = "Nothing to replay: the instance has neither frames nor markers.";
        return false;
    }
    return true;
}

#endif  // NATKIT_HAVE_PARQUET

}  // namespace

bool replayAvailable()
{
#if NATKIT_HAVE_PARQUET
    return true;
#else
    return false;
#endif
}

ReplayPlan planReplay(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ReplayRequest& request)
{
    ReplayPlan plan;
#if !NATKIT_HAVE_PARQUET
    (void)brokerManager;
    (void)request;
    plan.error =
        "This backend was built without Parquet support, so an instance cannot be "
        "replayed. Rebuild the image with libparquet-dev.";
    return plan;
#else
    if (request.sources.empty()) {
        plan.error = "Replay needs at least one recorded source.";
        return plan;
    }

    LoadedReplay loaded;
    if (!loadReplay(request, loaded, plan.error)) {
        return plan;
    }

    for (size_t index = 0; index < loaded.sources.size(); ++index) {
        const auto& source = loaded.sources[index];
        // One scratch DATA topic per source. Its schema is the CANONICAL channel
        // frame regardless of what the sensor originally published — the Parquet is
        // already that projection, so a fork's transforms bind to it directly with
        // no alternate input mapping. Forks are strictly easier to wire than live
        // boards.
        const auto identifier =
            request.replayIdentifier + "-s" + std::to_string(index);
        auto topic = nat::tools::makeTopicInfo(
            nat::core::StreamType::DATA, "device_id", identifier,
            nat::core::NatSignalFrameDataSchemaV1::name);
        if (topic == nullptr) {
            plan.error = "Could not build a scratch topic for " + identifier;
            return plan;
        }
        ReplayBinding binding;
        binding.originalStreamId = source.originalStreamId;
        binding.replayStreamId = topic->id;
        binding.identifier = identifier;
        binding.topic = topic->toTopicString();
        binding.frameCount = source.frames.size();
        binding.channelLabels = source.channelLabels;
        plan.bindings.push_back(std::move(binding));
        plan.totalFrames += source.frames.size();
    }

    if (!loaded.markers.empty()) {
        auto marker_topic = nat::tools::makeTopicInfo(
            nat::core::StreamType::MARKER, "session_id", request.replayIdentifier,
            nat::core::MarkerEventV1::name);
        if (marker_topic != nullptr) {
            plan.markerStreamId = marker_topic->id;
            plan.markerTopic = marker_topic->toTopicString();
            plan.markerCount = loaded.markers.size();
        }
    }

    plan.firstTsUs = loaded.timeline.front().tsUs;
    plan.lastTsUs = loaded.timeline.back().tsUs;
    plan.ok = true;
    return plan;
#endif
}

void runReplay(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ReplayRequest& request,
    const ReplayPlan& plan,
    const std::atomic<bool>& cancelled,
    const std::function<void(const ReplayProgress&)>& onProgress)
{
    ReplayProgress progress;
#if !NATKIT_HAVE_PARQUET
    (void)brokerManager;
    (void)request;
    (void)plan;
    (void)cancelled;
    progress.error = "Replay needs a Parquet-enabled build.";
    progress.finished = true;
    onProgress(progress);
    return;
#else
    LoadedReplay loaded;
    std::string error;
    if (!loadReplay(request, loaded, error)) {
        progress.error = error;
        progress.finished = true;
        onProgress(progress);
        return;
    }

    // Messengers: one per source data topic, plus the marker topic.
    std::vector<std::unique_ptr<nat::core::TopicMessenger>> dataMessengers;
    for (const auto& binding : plan.bindings) {
        auto topic = nat::tools::makeTopicInfo(
            nat::core::StreamType::DATA, "device_id", binding.identifier,
            nat::core::NatSignalFrameDataSchemaV1::name);
        if (topic == nullptr) {
            progress.error = "Could not open the scratch topic " + binding.identifier;
            progress.finished = true;
            onProgress(progress);
            return;
        }
        dataMessengers.push_back(brokerManager->createMessenger(topic));
    }
    std::unique_ptr<nat::core::TopicMessenger> markerMessenger;
    if (plan.markerStreamId != 0) {
        auto topic = nat::tools::makeTopicInfo(
            nat::core::StreamType::MARKER, "session_id", request.replayIdentifier,
            nat::core::MarkerEventV1::name);
        if (topic != nullptr) {
            markerMessenger = brokerManager->createMessenger(topic);
        }
    }

    const double speed = request.speed > 0.01 ? request.speed : 1.0;
    const auto startedAt = std::chrono::steady_clock::now();
    int64_t previousTsUs = loaded.timeline.front().tsUs;
    double pacedOffsetUs = 0.0;
    size_t sinceReport = 0;

    for (const auto& item : loaded.timeline) {
        if (cancelled.load()) {
            progress.cancelled = true;
            break;
        }

        // Review mode: pace to wall-clock from the ORIGINAL timestamp deltas, so
        // the recording plays back like a video. Recompute mode skips this
        // entirely — nobody is watching frames go by, and the point is to get the
        // result out of the pipeline as fast as it drains.
        //
        // Paced from CUMULATIVE per-item deltas rather than each item's absolute
        // offset from the first, so a long gap can be clamped (maxGapUs) without
        // permanently shifting everything after it. Local timing stays exact; only
        // dead air is compressed. Necessary because a single item far from the rest
        // — a lifecycle marker, or markers bracketing a window wider than the data —
        // would otherwise stall the replay for as long as that difference.
        if (request.paced) {
            const auto rawGapUs = item.tsUs - previousTsUs;
            const auto gapUs =
                std::clamp<int64_t>(rawGapUs, 0, std::max<int64_t>(0, request.maxGapUs));
            pacedOffsetUs += static_cast<double>(gapUs) / speed;
            while (!cancelled.load()) {
                const auto elapsedUs =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - startedAt)
                        .count();
                const auto remainingUs = pacedOffsetUs - static_cast<double>(elapsedUs);
                if (remainingUs <= 0) {
                    break;
                }
                // Cap a single sleep so a stop stays responsive.
                const auto sleepUs = std::min<double>(remainingUs, 50'000.0);
                std::this_thread::sleep_for(
                    std::chrono::microseconds(static_cast<int64_t>(sleepUs)));
            }
        }
        previousTsUs = item.tsUs;

        if (item.sourceIndex < 0) {
            if (markerMessenger == nullptr) {
                continue;
            }
            const auto& marker = loaded.markers[item.itemIndex];
            nat::core::MarkerEventV1 record(
                marker.value("session_id", std::string{}),
                marker.value("marker_type", std::string{}),
                marker.value("marker_id", std::string{}),
                marker.value("event", std::string{}),
                marker.value("label", std::string{}),
                static_cast<uint64_t>(marker.value("emitted_at_us", int64_t{0})),
                marker.contains("attributes") ? marker["attributes"].dump()
                                              : std::string("{}"));
            markerMessenger->sendMessage(record);
            ++progress.markersPublished;
        } else {
            const auto& source = loaded.sources[static_cast<size_t>(item.sourceIndex)];
            const auto& frame = source.frames[item.itemIndex];
            nat::core::NatSignalFrameDataSchemaV1 record(
                frame.deviceId, frame.seqNo,
                static_cast<uint64_t>(frame.deviceTsUs), frame.sampleRateHz,
                source.channelLabels, frame.samples, frame.samplesPerChannel);
            dataMessengers[static_cast<size_t>(item.sourceIndex)]->sendMessage(record);
            ++progress.framesPublished;
        }
        progress.lastTsUs = item.tsUs;

        // Report the FIRST item immediately (the caller waits on it to know the
        // scratch topic exists), then periodically.
        if (++sinceReport >= 500 ||
            progress.framesPublished + progress.markersPublished == 1) {
            sinceReport = 0;
            onProgress(progress);
        }
    }

    for (auto& messenger : dataMessengers) {
        if (messenger != nullptr) {
            messenger->flush();
        }
    }
    if (markerMessenger != nullptr) {
        markerMessenger->flush();
    }
    progress.finished = true;
    onProgress(progress);
#endif
}

void deleteReplayTopics(
    const std::shared_ptr<nat::kafka::BrokerManager>& brokerManager,
    const ReplayPlan& plan)
{
    if (brokerManager == nullptr) {
        return;
    }
    for (const auto& binding : plan.bindings) {
        if (!binding.topic.empty()) {
            brokerManager->deleteTopic(binding.topic);
        }
    }
    if (!plan.markerTopic.empty()) {
        brokerManager->deleteTopic(plan.markerTopic);
    }
}

}  // namespace natkit::tools
