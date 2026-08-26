#include "StreamViewerWebSocket.hpp"
#include "RecordingState.hpp"

#include "GraphTopicResolution.hpp"
#include "GraphTransportPlan.hpp"
#include "InProcessTransport.hpp"
#include "CohortExport.hpp"
#include "DeviceHealth.hpp"
#include "ParquetExport.hpp"
#include "ReplaySource.hpp"

#include <set>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <regex>
#include <sstream>
#include <deque>
#include <unordered_map>
#include <unordered_set>

#include <openssl/evp.h>
#include <vector>

using namespace drogon;

// Static member definition
std::shared_ptr<nat::kafka::BrokerManager> StreamViewerWebSocket::broker_manager_;

namespace {

uint64_t nowUs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::optional<uint64_t> parseStreamId(const nlohmann::json& value)
{
    try {
        if (value.is_number_unsigned()) {
            return value.get<uint64_t>();
        }
        if (value.is_number_integer()) {
            const auto signed_value = value.get<int64_t>();
            if (signed_value < 0) {
                return std::nullopt;
            }
            return static_cast<uint64_t>(signed_value);
        }
        if (value.is_string()) {
            const auto& text = value.get_ref<const std::string&>();
            if (text.empty()) {
                return std::nullopt;
            }
            std::size_t parsed_chars = 0;
            const auto parsed = std::stoull(text, &parsed_chars, 10);
            if (parsed_chars != text.size()) {
                return std::nullopt;
            }
            return static_cast<uint64_t>(parsed);
        }
    } catch (const std::exception&) {
    }

    return std::nullopt;
}

bool isValidTopicIdentifier(const std::string& value)
{
    static const std::regex pattern("^[A-Za-z0-9][A-Za-z0-9_-]*$");
    return std::regex_match(value, pattern);
}

size_t resolveTransformSlotCapacity()
{
    const char* transform_threads = std::getenv("NATKIT_TRANSFORM_THREADS");
    if (transform_threads != nullptr) {
        try {
            const auto parsed = std::stoul(transform_threads);
            if (parsed > 0) {
                return static_cast<size_t>(parsed);
            }
        } catch (const std::exception&) {
        }
    }

    const char* emg_threads = std::getenv("NATKIT_EMG_TRANSFORM_THREADS");
    if (emg_threads != nullptr) {
        try {
            const auto parsed = std::stoul(emg_threads);
            if (parsed > 0) {
                return static_cast<size_t>(parsed);
            }
        } catch (const std::exception&) {
        }
    }
    const auto detected = std::thread::hardware_concurrency();
    return detected == 0 ? 1U : static_cast<size_t>(detected);
}

std::string buildTransformSlotId(size_t slot_index)
{
    std::ostringstream stream;
    stream << "natkit-local-transform-worker:slot-";
    stream << std::setw(2) << std::setfill('0') << (slot_index + 1);
    return stream.str();
}

std::string classifyTransformWorkerStatus(
    size_t active_count,
    uint64_t last_heartbeat_us)
{
    if (active_count == 0) {
        return "idle";
    }
    const uint64_t current_us = nowUs();
    const uint64_t age_us =
        current_us > last_heartbeat_us ? current_us - last_heartbeat_us : 0;
    return age_us >= 3'000'000ULL ? "stalled" : "live";
}

std::shared_ptr<nat::core::BasicTopicInformation> createTopicInfo(
    const nat::core::StreamType stream_type,
    const std::string& topic_namespace,
    const std::string& identifier,
    const std::string& schema_name)
{
    // Single source of truth for the deterministic topic identity, shared with
    // the graph-resolution unit tests.
    return nat::tools::makeTopicInfo(
        stream_type, topic_namespace, identifier, schema_name);
}

std::vector<std::string> parseStringArray(const nlohmann::json& value)
{
    std::vector<std::string> parsed_values{};
    if (!value.is_array()) {
        return parsed_values;
    }

    for (const auto& item : value) {
        if (item.is_string()) {
            parsed_values.push_back(item.get<std::string>());
        }
    }
    return parsed_values;
}

std::unique_ptr<nat::core::SessionMetadataRecord> parseSessionMetadataRecord(
    const nlohmann::json& value)
{
    return std::unique_ptr<nat::core::SessionMetadataRecord>(
        new nat::core::SessionMetadataRecord(
            value.value("session_id", std::string{}),
            value.value("purpose", std::string{}),
            value.value("participant_id", std::string{}),
            value.value("protocol_id", std::string{}),
            parseStringArray(value.value("device_ids", nlohmann::json::array())),
            parseStringArray(value.value("tags", nlohmann::json::array())),
            value.value("notes", std::string{}),
            value.value("created_at_us", static_cast<uint64_t>(0)),
            value.value("updated_at_us", static_cast<uint64_t>(0))));
}

std::unique_ptr<nat::core::MarkerEventV1> parseMarkerEventRecord(
    const nlohmann::json& value)
{
    const std::string attributes_json =
        value.contains("attributes") ? value["attributes"].dump() : std::string("{}");
    return std::unique_ptr<nat::core::MarkerEventV1>(
        new nat::core::MarkerEventV1(
            value.value("session_id", std::string{}),
            value.value("marker_type", std::string{}),
            value.value("marker_id", std::string{}),
            value.value("event", std::string{}),
            value.value("label", std::string{}),
            value.value("emitted_at_us", static_cast<uint64_t>(0)),
            attributes_json));
}

std::optional<nlohmann::json> getDescriptorJsonForSchemaName(
    const std::string& schema_name)
{
    auto descriptor_maybe =
        nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
            schema_name);
    if (!descriptor_maybe.has_value()) {
        return std::nullopt;
    }

    auto encoded = descriptor_maybe.value()->encodeToBytes(
        nat::core::SerializationType::Json);
    if (!encoded) {
        return std::nullopt;
    }

    return nlohmann::json::parse(
        std::string(encoded->begin(), encoded->end()));
}

const double kPi = 3.14159265358979323846;

const nat::core::SchemaFieldDescriptor* findDescriptorFieldByPath(
    const nat::core::SchemaFieldDescriptor& root,
    const std::string& path)
{
    const auto path_maybe = nat::core::SchemaPath::parse(path);
    if (!path_maybe.has_value()) {
        return nullptr;
    }

    const nat::core::SchemaFieldDescriptor* current = &root;
    const auto& segments = path_maybe.value().getSegments();
    for (size_t index = 0; index < segments.size(); ++index) {
        const auto& segment = segments[index];
        if (segment.isArrayIndex) {
            const auto& array_item = current->getArrayItemField();
            if (current->getValueType() != nat::core::FieldValueType::Array ||
                !array_item) {
                return nullptr;
            }
            current = array_item.get();
            continue;
        }

        const nat::core::SchemaFieldDescriptor* next =
            current->findChildField(segment.fieldId);
        if (next == nullptr) {
            return nullptr;
        }
        current = next;
    }

    return current;
}

bool fieldTypeMatchesAny(
    const nat::core::SchemaFieldDescriptor* field,
    const std::vector<nat::core::FieldValueType>& types)
{
    if (field == nullptr) {
        return false;
    }
    for (const auto type : types) {
        if (field->getValueType() == type) {
            return true;
        }
    }
    return false;
}

bool descriptorSupportsNumericChannelFrame(
    const nat::core::DataSchemaDescriptor& descriptor)
{
    const auto& root = descriptor.getRootField();
    if (!fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "device_id"),
            std::vector<nat::core::FieldValueType>{nat::core::FieldValueType::String}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "seq_no"),
            std::vector<nat::core::FieldValueType>{nat::core::FieldValueType::Uint64}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "device_ts_us"),
            std::vector<nat::core::FieldValueType>{nat::core::FieldValueType::Uint64}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "sample_rate_hz"),
            std::vector<nat::core::FieldValueType>{nat::core::FieldValueType::Uint32}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "channels"),
            std::vector<nat::core::FieldValueType>{nat::core::FieldValueType::Array}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "channels.0.samples"),
            std::vector<nat::core::FieldValueType>{nat::core::FieldValueType::Array}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, "channels.0.samples.0"),
            std::vector<nat::core::FieldValueType>{
                nat::core::FieldValueType::Int16,
                nat::core::FieldValueType::Uint32,
                nat::core::FieldValueType::Uint64,
                nat::core::FieldValueType::Float32,
                nat::core::FieldValueType::Float64})) {
        return false;
    }

    const auto* label_field = findDescriptorFieldByPath(root, "channels.0.label");
    return label_field == nullptr ||
           label_field->getValueType() == nat::core::FieldValueType::String;
}

int getDataTopicPriority(const nat::core::BasicTopicInformation& topic)
{
    const std::string& schema_name = topic.schemaName;
    if (schema_name == nat::core::NatSignalFrameDataSchemaV1::name) {
        return 700;
    }
    if (schema_name == nat::core::ExgPillEmgDataSchemaV1::name) {
        return 650;
    }
    if (schema_name == nat::core::ExgPillEmgTransformDataSchemaV1::name) {
        return 600;
    }
    if (schema_name == nat::core::NatMuseBulkDataSchema::name) {
        return 500;
    }
    if (schema_name == nat::core::NatMuseDataSchema::name) {
        return 450;
    }
    if (schema_name == nat::core::NatImuBulkDataSchema::name) {
        return 400;
    }
    if (schema_name == nat::core::NatImuDataSchema::name) {
        return 350;
    }
    return 0;
}

std::shared_ptr<nat::core::BasicTopicInformation> choosePreferredDataTopic(
    const std::vector<std::unique_ptr<nat::core::BasicTopicInformation>>& data_topics)
{
    const nat::core::BasicTopicInformation* best_topic = nullptr;
    int best_priority = -1;
    for (const auto& topic : data_topics) {
        if (!topic) {
            continue;
        }
        const int priority = getDataTopicPriority(*topic);
        if (best_topic == nullptr || priority > best_priority) {
            best_topic = topic.get();
            best_priority = priority;
        }
    }
    if (best_topic == nullptr) {
        return nullptr;
    }
    return std::make_shared<nat::core::BasicTopicInformation>(*best_topic);
}

std::shared_ptr<nat::core::BasicTopicInformation> findTransformSourceTopicForStream(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    uint64_t source_stream_id);

// Marker/meta consumers read from the START of the topic (OFFSET_BEGINNING, -2)
// rather than live-tailing (OFFSET_END). Markers are low-volume, a subscriber
// wants the whole cue timeline (not just events after it connected), and a
// marker topic only materializes on the first publish — so a live-tail consumer
// that bound before/around the first publish would miss the burst. Reading from
// the beginning is robust regardless of when the subscription binds.
constexpr int64_t kMarkerConsumerStartOffset = -2;  // RdKafka OFFSET_BEGINNING

// Resolve a stream_id to a MARKER (or META) topic. Markers/meta are not DATA
// topics, so the DATA-only resolution in handleSubscribe/the streaming thread
// misses them. A subscriber to an experiment node's `markers` output (Phase 2 —
// the marker renderer) needs the matching MARKER topic bound so its
// MarkerEventV1 records get forwarded. We scan the FLAT getAllTopics() rather
// than getAllStreams() — the stream grouping drops marker topics (a marker
// stream surfaces there with no topics attached), whereas the flat topic list
// carries each topic with its real StreamType.
std::shared_ptr<nat::core::BasicTopicInformation> findMarkerOrMetaTopicForStreamId(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    uint64_t stream_id)
{
    if (!broker_manager) {
        return nullptr;
    }
    for (auto& topic : broker_manager->getAllTopics()) {
        if (topic && topic->id == stream_id &&
            (topic->type == nat::core::StreamType::MARKER ||
             topic->type == nat::core::StreamType::META)) {
            return std::make_shared<nat::core::BasicTopicInformation>(*topic);
        }
    }
    return nullptr;
}

struct NormalizedNumericChannelFrame {
    std::string deviceId;
    uint64_t seqNo = 0;
    uint64_t deviceTsUs = 0;
    uint32_t sampleRateHz = 0;
    std::vector<std::string> channelLabels{};
    std::vector<float> samples{};
    uint32_t samplesPerChannel = 0;
};

struct TransformInputChannelMappingDefinition {
    std::string label;
    std::string sampleArrayPath;
};

struct TransformInputMappingDefinition {
    std::string id;
    std::string label;
    std::string schemaName;
    std::string seqNoPath;
    std::string deviceTsUsPath;
    std::string sampleRateHzPath;
    std::optional<uint32_t> sampleRateHzValue{};
    std::vector<TransformInputChannelMappingDefinition> channels{};
};

std::optional<float> tryConvertFieldToFloat(
    const nat::core::FieldValueRef& field)
{
    switch (field.getValueType()) {
    case nat::core::FieldValueType::Int16: {
        const auto value = field.getInt16();
        if (value.has_value()) {
            return static_cast<float>(value.value());
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Uint32: {
        const auto value = field.getUint32();
        if (value.has_value()) {
            return static_cast<float>(value.value());
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Uint64: {
        const auto value = field.getUint64();
        if (value.has_value()) {
            return static_cast<float>(value.value());
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Float32: {
        const auto value = field.getFloat32();
        if (value.has_value()) {
            return value.value();
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Float64: {
        const auto value = field.getFloat64();
        if (value.has_value()) {
            return static_cast<float>(value.value());
        }
        return std::nullopt;
    }
    default:
        return std::nullopt;
    }
}

std::optional<uint64_t> tryConvertFieldToUint64(
    const nat::core::FieldValueRef& field)
{
    switch (field.getValueType()) {
    case nat::core::FieldValueType::Uint32: {
        const auto value = field.getUint32();
        if (value.has_value()) {
            return static_cast<uint64_t>(value.value());
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Uint64: {
        const auto value = field.getUint64();
        if (value.has_value()) {
            return value.value();
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Float32: {
        const auto value = field.getFloat32();
        if (value.has_value() && value.value() >= 0.0f) {
            return static_cast<uint64_t>(value.value());
        }
        return std::nullopt;
    }
    case nat::core::FieldValueType::Float64: {
        const auto value = field.getFloat64();
        if (value.has_value() && value.value() >= 0.0) {
            return static_cast<uint64_t>(value.value());
        }
        return std::nullopt;
    }
    default:
        return std::nullopt;
    }
}

bool descriptorSupportsAlternateInputMapping(
    const nat::core::DataSchemaDescriptor& descriptor,
    const TransformInputMappingDefinition& mapping)
{
    if (!mapping.schemaName.empty() &&
        descriptor.getTargetSchemaName() != mapping.schemaName) {
        return false;
    }

    const auto& root = descriptor.getRootField();
    if (!fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, mapping.seqNoPath),
            std::vector<nat::core::FieldValueType>{
                nat::core::FieldValueType::Uint32,
                nat::core::FieldValueType::Uint64}) ||
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, mapping.deviceTsUsPath),
            std::vector<nat::core::FieldValueType>{
                nat::core::FieldValueType::Uint64})) {
        return false;
    }

    if (!mapping.sampleRateHzValue.has_value() &&
        !fieldTypeMatchesAny(
            findDescriptorFieldByPath(root, mapping.sampleRateHzPath),
            std::vector<nat::core::FieldValueType>{
                nat::core::FieldValueType::Uint32,
                nat::core::FieldValueType::Uint64,
                nat::core::FieldValueType::Float32,
                nat::core::FieldValueType::Float64})) {
        return false;
    }

    if (mapping.channels.empty()) {
        return false;
    }

    for (const auto& channel : mapping.channels) {
        if (!fieldTypeMatchesAny(
                findDescriptorFieldByPath(root, channel.sampleArrayPath),
                std::vector<nat::core::FieldValueType>{
                    nat::core::FieldValueType::Array}) ||
            !fieldTypeMatchesAny(
                findDescriptorFieldByPath(root, channel.sampleArrayPath + ".0"),
                std::vector<nat::core::FieldValueType>{
                    nat::core::FieldValueType::Int16,
                    nat::core::FieldValueType::Uint32,
                    nat::core::FieldValueType::Uint64,
                    nat::core::FieldValueType::Float32,
                    nat::core::FieldValueType::Float64})) {
            return false;
        }
    }

    return true;
}

const std::vector<TransformInputMappingDefinition>&
getAlternateTransformInputMappings()
{
    static const std::vector<TransformInputMappingDefinition> mappings{
        TransformInputMappingDefinition{
            "natmuse_eeg_v1",
            "Muse EEG channels",
            nat::core::NatMuseDataSchema::name,
            "eeg_sequence",
            "time",
            std::string{},
            256U,
            std::vector<TransformInputChannelMappingDefinition>{
                {"TP9", "eeg.tp9"},
                {"AF7", "eeg.af7"},
                {"AF8", "eeg.af8"},
                {"TP10", "eeg.tp10"}}},
        // IMU accel/gyro/mag as filterable channels. The bulk frame is
        // sample-major on the wire; NatImuBulkDataSchemaDescriptor projects it into
        // the per-axis sample arrays referenced here. sample_rate_hz comes from the
        // frame envelope (real path, not a fixed value). Quaternion is intentionally
        // excluded — a unit rotation isn't a meaningful per-channel filter target.
        //
        // ⚠️ THIS LIST IS ALSO THE PARQUET COLUMN LIST. ParquetExport has no IMU
        // schema of its own -- it takes its columns from projectRecordToChannelFrame,
        // which is driven by this mapping. So a channel missing here is a column
        // missing from every exported file, silently.
        //
        // ⚠️ The magnetometer columns are present for frames of EVERY version, and
        // read zero for version 1 (anything recorded before 2026-08). The mapping
        // has no way to express "absent" -- it is a fixed column list -- so the
        // distinction lives in has_data.magnetometer on the JSON path only. Do not
        // read a column of zeroes in an old Parquet file as a measurement.
        TransformInputMappingDefinition{
            "natimu_motion_v1",
            "IMU accel/gyro/mag",
            nat::core::NatImuBulkDataSchema::name,
            "seq_no",
            "device_ts_us",
            "sample_rate_hz",
            std::nullopt,
            std::vector<TransformInputChannelMappingDefinition>{
                {"Accel X", "accel_x"},
                {"Accel Y", "accel_y"},
                {"Accel Z", "accel_z"},
                {"Gyro X", "gyro_x"},
                {"Gyro Y", "gyro_y"},
                {"Gyro Z", "gyro_z"},
                {"Mag X", "mag_x"},
                {"Mag Y", "mag_y"},
                {"Mag Z", "mag_z"}}}};
    return mappings;
}

std::optional<TransformInputMappingDefinition>
findCompatibleAlternateInputMapping(
    const nat::core::DataSchemaDescriptor& descriptor)
{
    for (const auto& mapping : getAlternateTransformInputMappings()) {
        if (descriptorSupportsAlternateInputMapping(descriptor, mapping)) {
            return mapping;
        }
    }
    return std::nullopt;
}

std::optional<TransformInputMappingDefinition>
findRequestedAlternateInputMapping(
    const nat::core::DataSchemaDescriptor& descriptor,
    const std::string& requested_mapping_id)
{
    for (const auto& mapping : getAlternateTransformInputMappings()) {
        if (mapping.id == requested_mapping_id &&
            descriptorSupportsAlternateInputMapping(descriptor, mapping)) {
            return mapping;
        }
    }
    return std::nullopt;
}

std::optional<NormalizedNumericChannelFrame> tryNormalizeNumericChannelFrame(
    const nat::core::Schema& record,
    const nat::core::DataSchemaDescriptor& descriptor)
{
    NormalizedNumericChannelFrame normalized{};

    const auto device_id = descriptor.getString(record, "device_id");
    const auto seq_no = descriptor.getUint64(record, "seq_no");
    const auto device_ts_us = descriptor.getUint64(record, "device_ts_us");
    const auto sample_rate_hz = descriptor.getUint32(record, "sample_rate_hz");
    const auto channels_field = descriptor.tryGetFieldValue(record, "channels");
    if (!device_id.has_value() || !seq_no.has_value() ||
        !device_ts_us.has_value() || !sample_rate_hz.has_value() ||
        !channels_field.has_value() ||
        channels_field.value().getValueType() != nat::core::FieldValueType::Array) {
        return std::nullopt;
    }

    normalized.deviceId = device_id.value();
    normalized.seqNo = seq_no.value();
    normalized.deviceTsUs = device_ts_us.value();
    normalized.sampleRateHz = sample_rate_hz.value();

    const size_t channel_count = channels_field.value().getElementCount();
    if (channel_count == 0) {
        return std::nullopt;
    }

    normalized.channelLabels.reserve(channel_count);
    for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
        const std::string label_path =
            "channels." + std::to_string(channel_index) + ".label";
        const auto label = descriptor.getString(record, label_path);
        normalized.channelLabels.push_back(
            label.has_value() ? label.value() : std::string{});

        const std::string samples_path =
            "channels." + std::to_string(channel_index) + ".samples";
        const auto sample_array = descriptor.tryGetFieldValue(record, samples_path);
        if (!sample_array.has_value() ||
            sample_array.value().getValueType() != nat::core::FieldValueType::Array) {
            return std::nullopt;
        }

        const size_t sample_count = sample_array.value().getElementCount();
        if (channel_index == 0) {
            if (sample_count == 0) {
                return std::nullopt;
            }
            normalized.samplesPerChannel = static_cast<uint32_t>(sample_count);
            normalized.samples.reserve(channel_count * sample_count);
        } else if (sample_count != normalized.samplesPerChannel) {
            return std::nullopt;
        }

        for (size_t sample_index = 0; sample_index < sample_count; ++sample_index) {
            const std::string sample_path =
                samples_path + "." + std::to_string(sample_index);
            const auto sample_field = descriptor.tryGetFieldValue(record, sample_path);
            if (!sample_field.has_value()) {
                return std::nullopt;
            }
            const auto sample = tryConvertFieldToFloat(sample_field.value());
            if (!sample.has_value()) {
                return std::nullopt;
            }
            normalized.samples.push_back(sample.value());
        }
    }

    return normalized;
}

std::optional<NormalizedNumericChannelFrame> tryNormalizeNumericChannelFrame(
    const nat::core::Schema& record,
    const nat::core::DataSchemaDescriptor& descriptor,
    const TransformInputMappingDefinition& mapping,
    uint64_t source_stream_id)
{
    NormalizedNumericChannelFrame normalized{};

    const auto seq_field = descriptor.tryGetFieldValue(record, mapping.seqNoPath);
    const auto ts_field =
        descriptor.tryGetFieldValue(record, mapping.deviceTsUsPath);
    if (!seq_field.has_value() || !ts_field.has_value()) {
        return std::nullopt;
    }

    const auto seq_no = tryConvertFieldToUint64(seq_field.value());
    const auto device_ts_us = tryConvertFieldToUint64(ts_field.value());
    if (!seq_no.has_value() || !device_ts_us.has_value()) {
        return std::nullopt;
    }

    uint32_t sample_rate_hz = 0;
    if (mapping.sampleRateHzValue.has_value()) {
        sample_rate_hz = mapping.sampleRateHzValue.value();
    } else {
        const auto sample_rate_field =
            descriptor.tryGetFieldValue(record, mapping.sampleRateHzPath);
        if (!sample_rate_field.has_value()) {
            return std::nullopt;
        }
        const auto sample_rate = tryConvertFieldToUint64(sample_rate_field.value());
        if (!sample_rate.has_value() || sample_rate.value() == 0) {
            return std::nullopt;
        }
        sample_rate_hz = static_cast<uint32_t>(sample_rate.value());
    }

    normalized.deviceId = "stream-" + std::to_string(source_stream_id);
    normalized.seqNo = seq_no.value();
    normalized.deviceTsUs = device_ts_us.value();
    normalized.sampleRateHz = sample_rate_hz;
    normalized.channelLabels.reserve(mapping.channels.size());

    for (size_t channel_index = 0; channel_index < mapping.channels.size();
         ++channel_index) {
        const auto& channel = mapping.channels[channel_index];
        const auto sample_array =
            descriptor.tryGetFieldValue(record, channel.sampleArrayPath);
        if (!sample_array.has_value() ||
            sample_array.value().getValueType() != nat::core::FieldValueType::Array) {
            return std::nullopt;
        }

        const size_t sample_count = sample_array.value().getElementCount();
        if (channel_index == 0) {
            if (sample_count == 0) {
                return std::nullopt;
            }
            normalized.samplesPerChannel = static_cast<uint32_t>(sample_count);
            normalized.samples.reserve(mapping.channels.size() * sample_count);
        } else if (sample_count != normalized.samplesPerChannel) {
            return std::nullopt;
        }

        normalized.channelLabels.push_back(channel.label);
        for (size_t sample_index = 0; sample_index < sample_count; ++sample_index) {
            const auto sample_field = descriptor.tryGetFieldValue(
                record,
                channel.sampleArrayPath + "." + std::to_string(sample_index));
            if (!sample_field.has_value()) {
                return std::nullopt;
            }
            const auto sample = tryConvertFieldToFloat(sample_field.value());
            if (!sample.has_value()) {
                return std::nullopt;
            }
            normalized.samples.push_back(sample.value());
        }
    }

    return normalized;
}

std::optional<NormalizedNumericChannelFrame> tryNormalizeNumericChannelFrame(
    const nat::core::Schema& record,
    const nat::core::DataSchemaDescriptor& descriptor,
    const std::optional<TransformInputMappingDefinition>& mapping,
    uint64_t source_stream_id)
{
    if (mapping.has_value()) {
        return tryNormalizeNumericChannelFrame(
            record, descriptor, mapping.value(), source_stream_id);
    }
    return tryNormalizeNumericChannelFrame(record, descriptor);
}

template <typename SignalRecordT>
nlohmann::json formatSignalFrameAsJson(
    const SignalRecordT& data,
    const std::string& schema_version,
    uint64_t stream_id,
    const std::string& encoding_type,
    size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "emg_data";
    json["stream_id"] = std::to_string(stream_id);
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    json["schema_version"] = schema_version;
    json["device_id"] = data.getDeviceId();
    json["seq_no"] = data.getSeqNo();
    json["device_ts_us"] = data.getDeviceTsUs();
    json["n_channels"] = data.getChannelCount();
    json["samples_per_channel"] = data.getSamplesPerChannel();
    json["sample_rate_hz"] = data.getSampleRateHz();
    json["channel_labels"] = data.getChannelLabels();
    json["payload"] = nlohmann::json::array();

    const auto& samples = data.getSamples();
    const size_t samples_per_channel = data.getSamplesPerChannel();
    for (size_t channel_index = 0; channel_index < data.getChannelCount(); ++channel_index) {
        nlohmann::json channel = nlohmann::json::array();
        const size_t offset = channel_index * samples_per_channel;
        for (size_t sample_index = 0; sample_index < samples_per_channel; ++sample_index) {
            channel.push_back(samples[offset + sample_index]);
        }
        json["payload"].push_back(channel);
    }

    return json;
}

// Descriptor-driven generic channel-frame message (Phase 3 of the
// visual-programming rework). ANY record whose descriptor matches the canonical
// channel-frame contract is projected to this one wire message via
// tryNormalizeNumericChannelFrame — no per-sensor formatter, no dynamic_cast.
// The frontend treats `frame` exactly like the legacy `emg_data` message (kept
// as a compatibility alias). This is how a new sensor is onboarded with zero
// dispatch/formatter edits.
nlohmann::json formatNormalizedFrameAsJson(
    const NormalizedNumericChannelFrame& frame,
    uint64_t stream_id,
    const std::string& encoding_type,
    const std::string& schema_name)
{
    nlohmann::json json;
    json["type"] = "frame";
    json["stream_id"] = std::to_string(stream_id);
    json["schema_name"] = schema_name;
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = 0;
    json["schema_version"] = "1";
    json["device_id"] = frame.deviceId;
    json["seq_no"] = frame.seqNo;
    json["device_ts_us"] = frame.deviceTsUs;
    json["n_channels"] = frame.channelLabels.size();
    json["samples_per_channel"] = frame.samplesPerChannel;
    json["sample_rate_hz"] = frame.sampleRateHz;
    json["channel_labels"] = frame.channelLabels;
    json["payload"] = nlohmann::json::array();

    const size_t samples_per_channel = frame.samplesPerChannel;
    for (size_t channel_index = 0; channel_index < frame.channelLabels.size();
         ++channel_index) {
        nlohmann::json channel = nlohmann::json::array();
        const size_t offset = channel_index * samples_per_channel;
        for (size_t sample_index = 0; sample_index < samples_per_channel;
             ++sample_index) {
            if (offset + sample_index < frame.samples.size()) {
                channel.push_back(frame.samples[offset + sample_index]);
            }
        }
        json["payload"].push_back(channel);
    }

    return json;
}

// Project a MarkerEventV1 record to a `marker` wire message (Phase 2 — markers
// as a first-class, observable stream). The marker renderer subscribes to a
// marker stream and draws cue/session events as ticks/regions + data-chart
// overlays. attributes are parsed back into an object (they were stored as a
// JSON string) so the frontend gets structured fields (e.g. the cue class).
nlohmann::json formatMarkerEventAsJson(
    const nat::core::MarkerEventV1& marker,
    uint64_t stream_id,
    const std::string& encoding_type,
    size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "marker";
    json["stream_id"] = std::to_string(stream_id);
    json["schema_name"] = nat::core::MarkerEventV1::name;
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    json["session_id"] = marker.getSessionId();
    json["marker_type"] = marker.getMarkerType();
    json["marker_id"] = marker.getMarkerId();
    json["event"] = marker.getEvent();
    json["label"] = marker.getLabel();
    json["emitted_at_us"] = marker.getEmittedAtUs();
    // Canonical time axis for markers is emitted_at_us (see the Timestamped
    // base, Phase 3); mirror it as `timestamp` so viewers share one accessor.
    json["timestamp"] = marker.getEmittedAtUs();
    nlohmann::json attributes = nlohmann::json::object();
    try {
        auto parsed = nlohmann::json::parse(marker.getAttributesJson());
        if (parsed.is_object()) {
            attributes = std::move(parsed);
        }
    } catch (const std::exception&) {
        // Malformed attributes: fall back to an empty object rather than drop
        // the whole marker.
    }
    json["attributes"] = std::move(attributes);
    return json;
}

struct TransformConfig {
    std::string kind;
    double cutoff_hz = 20.0;
    double low_cutoff_hz = 20.0;
    double high_cutoff_hz = 450.0;
    double notch_hz = 60.0;
    double notch_q = 30.0;
    std::string iir_method = "butterworth";
    uint32_t butterworth_order = 2;
    double biquad_q = std::sqrt(0.5);
    uint32_t harmonic_count = 0;
    uint32_t window_samples = 32;
    uint32_t step_samples = 16;
    double zc_threshold = 0.0;
    double ssc_threshold = 0.0;
    uint32_t ar_order = 4;
    std::string model_path{};
    std::string select_mode = "label";
    std::string selection{};
};

// Backward-compatible alias — the EMG-specific name is being retired (Phase 0
// of the visual-programming rework); the config is sensor-agnostic.
using EmgTransformConfig = TransformConfig;

std::string serializeTransformConfigJson(const TransformConfig& config)
{
    nlohmann::json json;
    if (config.kind == "rectify") {
        return json.dump();
    }
    if (config.kind == "lowpass_envelope") {
        json["cutoff_hz"] = config.cutoff_hz;
    } else if (config.kind == "bandpass_iir") {
        json["low_cutoff_hz"] = config.low_cutoff_hz;
        json["high_cutoff_hz"] = config.high_cutoff_hz;
        json["iir_method"] = config.iir_method;
        if (config.iir_method == "butterworth") {
            json["butterworth_order"] = config.butterworth_order;
        } else if (config.iir_method == "biquad") {
            json["biquad_q"] = config.biquad_q;
        }
    } else if (config.kind == "notch_iir") {
        json["notch_hz"] = config.notch_hz;
        json["notch_q"] = config.notch_q;
        json["harmonic_count"] = config.harmonic_count;
    } else if (config.kind == "rms_window") {
        json["window_samples"] = config.window_samples;
    } else if (config.kind == "sliding_window") {
        json["window_samples"] = config.window_samples;
        json["step_samples"] = config.step_samples;
    } else if (config.kind == "highpass_iir") {
        json["cutoff_hz"] = config.cutoff_hz;
        json["iir_method"] = config.iir_method;
        if (config.iir_method == "butterworth") {
            json["butterworth_order"] = config.butterworth_order;
        } else if (config.iir_method == "biquad") {
            json["biquad_q"] = config.biquad_q;
        }
    } else if (config.kind == "mav" || config.kind == "wl" || config.kind == "rms") {
        return json.dump();
    } else if (config.kind == "zc") {
        json["zc_threshold"] = config.zc_threshold;
    } else if (config.kind == "ssc") {
        json["ssc_threshold"] = config.ssc_threshold;
    } else if (config.kind == "ar_coeffs") {
        json["ar_order"] = config.ar_order;
    } else if (config.kind == "lda_classify" ||
               config.kind == "emg_gesture_classify") {
        json["model_path"] = config.model_path;
    } else if (config.kind == "channel_select") {
        json["select_mode"] = config.select_mode;
        json["selection"] = config.selection;
    }
    return json.dump();
}

nlohmann::json buildTransformCapabilityJson(
    const std::string& kind,
    const std::string& label,
    const std::string& description,
    const nlohmann::json& config_fields)
{
    nlohmann::json input_mappings = nlohmann::json::array();
    input_mappings.push_back(
        {{"id", "canonical_channel_frame"},
         {"label", "Canonical channel frame"},
         {"mode", "canonical_channel_frame"},
         {"required_descriptor_paths",
          nlohmann::json::array(
              {"device_id",
               "seq_no",
               "device_ts_us",
               "sample_rate_hz",
               "channels.0.samples.0"})}});
    for (const auto& mapping : getAlternateTransformInputMappings()) {
        nlohmann::json channels = nlohmann::json::array();
        for (const auto& channel : mapping.channels) {
            channels.push_back(
                {{"label", channel.label},
                 {"sample_array_path", channel.sampleArrayPath}});
        }

        nlohmann::json mapping_json{
            {"id", mapping.id},
            {"label", mapping.label},
            {"mode", "explicit_channel_paths"},
            {"schema_name", mapping.schemaName},
            {"required_descriptor_paths",
             nlohmann::json::array(
                 {mapping.seqNoPath, mapping.deviceTsUsPath})},
            {"seq_no_path", mapping.seqNoPath},
            {"device_ts_us_path", mapping.deviceTsUsPath},
            {"channels", channels}};
        if (mapping.sampleRateHzValue.has_value()) {
            mapping_json["sample_rate_hz_value"] = mapping.sampleRateHzValue.value();
        } else if (!mapping.sampleRateHzPath.empty()) {
            mapping_json["sample_rate_hz_path"] = mapping.sampleRateHzPath;
        }
        input_mappings.push_back(mapping_json);
    }

    nlohmann::json json;
    json["kind"] = kind;
    json["label"] = label;
    json["description"] = description;
    json["input_descriptor_paths"] = nlohmann::json::array(
        {"device_id",
         "seq_no",
         "device_ts_us",
         "sample_rate_hz",
         "channels.0.samples.0"});
    json["input_mappings"] = input_mappings;
    json["output_schema_name"] = nat::core::NatSignalFrameDataSchemaV1::name;
    json["config_fields"] = config_fields;
    return json;
}

nlohmann::json buildTransformCapabilitiesJson()
{
    nlohmann::json transforms = nlohmann::json::array();
    transforms.push_back(buildTransformCapabilityJson(
        "rectify",
        "Rectify",
        "Absolute-value mapping over a numeric channel frame",
        nlohmann::json::array()));
    transforms.push_back(buildTransformCapabilityJson(
        "lowpass_envelope",
        "Low-pass envelope",
        "Pure low-pass smoothing stage for numeric channel frames without implicit rectification",
        nlohmann::json::array(
            {{{"id", "cutoff_hz"},
              {"label", "Cutoff (Hz)"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", 5.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "bandpass_iir",
        "Band-pass IIR",
        "Pure band-pass IIR filter using cascaded high-pass and low-pass stages",
        nlohmann::json::array(
            {{{"id", "low_cutoff_hz"},
              {"label", "Low cutoff (Hz)"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", 20.0}},
             {{"id", "high_cutoff_hz"},
              {"label", "High cutoff (Hz)"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", 450.0}},
             {{"id", "iir_method"},
              {"label", "IIR method"},
              {"type", "enum"},
              {"required", true},
              {"options", nlohmann::json::array({"butterworth", "biquad"})},
              {"default_option", "butterworth"}},
             {{"id", "butterworth_order"},
              {"label", "Butterworth order"},
              {"type", "number"},
              {"required", true},
              {"min", 1.0},
              {"step", 1.0},
              {"default_value", 2.0}},
             {{"id", "biquad_q"},
              {"label", "Biquad Q"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", std::sqrt(0.5)}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "notch_iir",
        "Notch IIR",
        "Narrowband notch filtering with optional harmonic sections up to Nyquist",
        nlohmann::json::array(
            {{{"id", "notch_hz"},
              {"label", "Base notch (Hz)"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", 60.0}},
             {{"id", "notch_q"},
              {"label", "Notch Q"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", 30.0}},
             {{"id", "harmonic_count"},
              {"label", "Extra harmonics"},
              {"type", "number"},
              {"required", true},
              {"min", 0.0},
              {"step", 1.0},
              {"default_value", 0.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "rms_window",
        "RMS window",
        "Sliding RMS magnitude estimate over a fixed number of samples with prefix-window startup behavior",
        nlohmann::json::array(
            {{{"id", "window_samples"},
              {"label", "Window samples"},
              {"type", "number"},
              {"required", true},
              {"min", 1.0},
              {"step", 1.0},
              {"default_value", 32.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "sliding_window",
        "Sliding window",
        "Explicit overlapping sample windows emitted as derived channel frames",
        nlohmann::json::array(
            {{{"id", "window_samples"},
              {"label", "Window samples"},
              {"type", "number"},
              {"required", true},
              {"min", 1.0},
              {"step", 1.0},
              {"default_value", 128.0}},
             {{"id", "step_samples"},
              {"label", "Step samples"},
              {"type", "number"},
              {"required", true},
              {"min", 1.0},
              {"step", 1.0},
              {"default_value", 32.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "highpass_iir",
        "High-pass IIR",
        "Pure high-pass IIR filter with Butterworth or biquad design options",
        nlohmann::json::array(
            {{{"id", "cutoff_hz"},
              {"label", "Cutoff (Hz)"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", 20.0}},
             {{"id", "iir_method"},
              {"label", "IIR method"},
              {"type", "enum"},
              {"required", true},
              {"options", nlohmann::json::array({"butterworth", "biquad"})},
              {"default_option", "butterworth"}},
             {{"id", "butterworth_order"},
              {"label", "Butterworth order"},
              {"type", "number"},
              {"required", true},
              {"min", 1.0},
              {"step", 1.0},
              {"default_value", 2.0}},
             {{"id", "biquad_q"},
              {"label", "Biquad Q"},
              {"type", "number"},
              {"required", true},
              {"min", 0.1},
              {"step", 0.1},
              {"default_value", std::sqrt(0.5)}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "mav",
        "Mean Absolute Value",
        "Reduces one windowed channel frame (e.g. from Sliding window) to its mean absolute value per channel",
        nlohmann::json::array()));
    transforms.push_back(buildTransformCapabilityJson(
        "rms",
        "RMS (windowed)",
        "Reduces one windowed channel frame (e.g. from Sliding window) to its root-mean-square value per channel — unlike RMS window, this is a one-shot per-window reduction, not a continuous moving filter",
        nlohmann::json::array()));
    transforms.push_back(buildTransformCapabilityJson(
        "wl",
        "Waveform Length",
        "Reduces one windowed channel frame to the cumulative absolute difference between consecutive samples per channel",
        nlohmann::json::array()));
    transforms.push_back(buildTransformCapabilityJson(
        "zc",
        "Zero Crossings",
        "Counts zero-crossings per channel within one windowed channel frame, ignoring crossings below the threshold",
        nlohmann::json::array(
            {{{"id", "zc_threshold"},
              {"label", "Threshold"},
              {"type", "number"},
              {"required", true},
              {"min", 0.0},
              {"step", 0.01},
              {"default_value", 0.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "ssc",
        "Slope Sign Changes",
        "Counts slope-sign changes per channel within one windowed channel frame, ignoring changes below the threshold",
        nlohmann::json::array(
            {{{"id", "ssc_threshold"},
              {"label", "Threshold"},
              {"type", "number"},
              {"required", true},
              {"min", 0.0},
              {"step", 0.01},
              {"default_value", 0.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "ar_coeffs",
        "AR Coefficients",
        "Fits a Levinson-Durbin autoregressive model to one windowed channel frame, emitting ar_order coefficients per channel",
        nlohmann::json::array(
            {{{"id", "ar_order"},
              {"label", "AR order"},
              {"type", "number"},
              {"required", true},
              {"min", 1.0},
              {"step", 1.0},
              {"default_value", 4.0}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "lda_classify",
        "LDA Classify",
        "Scores a flat feature-vector frame (e.g. from a Combine node) against a diagonal-covariance LDA model loaded from disk, emitting a predicted class index and per-class confidences",
        nlohmann::json::array(
            {{{"id", "model_path"},
              {"label", "Model path"},
              {"type", "string"},
              {"required", true}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "emg_gesture_classify",
        "EMG Gesture Classify",
        "Self-contained EMG gesture classifier: consumes raw EMG frames and "
        "internally reproduces the training pipeline (sliding window -> DSP -> "
        "Hudgins features -> rest-calibration normalization -> LDA), driven "
        "entirely by a model bundle so live accuracy matches training. Point "
        "model_path at the emg-gesture-bundle.json written by the train node.",
        nlohmann::json::array(
            // Not required at author/validate time: it is auto-filled from the
            // train job and only needs to be non-empty at start (enforced in
            // parseEmgTransformConfig). This lets a Quick-Start graph validate
            // clean before the model is trained.
            {{{"id", "model_path"},
              {"label", "Model bundle path"},
              {"type", "string"},
              {"required", false}}})));
    transforms.push_back(buildTransformCapabilityJson(
        "channel_select",
        "Select / Split channels",
        "Passes through a subset of a multi-channel frame's channels — e.g. "
        "destructure a concatenated feature vector into per-family or per-source "
        "sub-vectors. Keep every channel, channels whose label contains a "
        "substring, or an explicit index range/list.",
        nlohmann::json::array(
            {{{"id", "select_mode"},
              {"label", "Select by"},
              {"type", "enum"},
              {"required", true},
              {"options", nlohmann::json::array({"label", "index", "all"})},
              {"default_option", "label"}},
             {{"id", "selection"},
              {"label", "Selection (label substring, or indices e.g. 0-8,12)"},
              {"type", "string"},
              {"required", false},
              {"default_value", "mav"}}})));
    return transforms;
}

// Full node catalog (Phase 1 of the visual-programming rework). The frontend
// palette and node inspector render purely from this — adding a node type is a
// data change here, never a TypeScript union edit. Each entry advertises:
//   node_type   unique id (for transforms this equals the transform_kind)
//   kind        the coarse structural kind the backend runtime validates
//               (stream_source | transform | viewer | sink | combine)
//   category    palette grouping (source | transform | viewer | sink)
//   runner      who executes it (kafka_source | cpp_dsp | frontend | kafka_sink)
//   config_fields  typed config schema (reused typed-field machinery)
//   input_ports / output_ports  typed port templates
// Transform entries also carry the existing input_mappings / output_schema_name
// so the create_transform path keeps working unchanged.
// Provenance ports (lineage/control wiring) are identified purely by an id
// prefix, matching the frontend (streamGraph.ts PROVENANCE_PORT_PREFIX). They
// are preserved through port normalization but excluded from data-port rules.
inline bool isProvenancePortId(const std::string& port_id)
{
    return port_id.rfind("prov_", 0) == 0;
}

// The dedicated provenance stubs each node kind exposes.
//
// `prov_source` (source->experiment) and `prov_experiment` (experiment->train)
// are RETIRED (experiment-history-snapshots-plan): the experiment now owns the
// whole graph, so every source in it is a recorded source and that lineage is
// implicit. A train node points at instances -- concrete datasets with
// materialized files -- instead of at an experiment node. Only the
// train->classify pair survives, because a model artifact is a real handoff
// between two nodes that nothing else expresses.
constexpr const char* kProvenancePortModels =
    "prov_models"; // train->classify (train output)
constexpr const char* kProvenancePortModel =
    "prov_model"; // train->classify (classify input)

nlohmann::json buildNodeCatalogJson()
{
    const auto singleInputPort = []() {
        return nlohmann::json::array(
            {{{"id", "in"}, {"label", "Input"}}});
    };
    const auto singleOutputPort = []() {
        return nlohmann::json::array(
            {{{"id", "out"}, {"label", "Output"}}});
    };
    // Provenance-port advertisement (lineage stubs; kind "provenance" so the
    // frontend renders/validates them distinctly from data ports).
    const auto provPort = [](const char* id, const char* label) {
        return nlohmann::json{{"id", id}, {"label", label}, {"kind", "provenance"}};
    };

    nlohmann::json nodes = nlohmann::json::array();

    // Stream source — a live Kafka stream selected by the user.
    nodes.push_back(
        {{"node_type", "stream_source"},
         {"kind", "stream_source"},
         {"category", "source"},
         {"runner", "kafka_source"},
         {"label", "Stream source"},
         {"description",
          "A live sensor/derived stream selected from the broker. Emits its "
          "records downstream; the port descriptor is the stream's own."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", nlohmann::json::array()},
         {"output_ports", singleOutputPort()},
         {"variadic_inputs", false}});

    // Compiled C++ DSP transforms — one catalog entry per transform capability.
    for (const auto& capability : buildTransformCapabilitiesJson()) {
        nlohmann::json node;
        node["node_type"] = capability.at("kind");
        node["kind"] = "transform";
        node["category"] = "transform";
        node["runner"] = "cpp_dsp";
        node["label"] = capability.at("label");
        node["description"] = capability.at("description");
        node["config_fields"] = capability.at("config_fields");
        node["input_mappings"] = capability.at("input_mappings");
        node["input_descriptor_paths"] = capability.at("input_descriptor_paths");
        node["output_schema_name"] = capability.at("output_schema_name");
        auto input_ports = singleInputPort();
        // A classifier transform (loads a model bundle) can receive a
        // train->classify provenance edge — advertise a prov_model input stub.
        // Detected by a model_path config field.
        for (const auto& field : capability.at("config_fields")) {
            if (field.value("id", std::string{}) == "model_path") {
                input_ports.push_back(provPort(kProvenancePortModel, "Model"));
                break;
            }
        }
        node["input_ports"] = input_ports;
        node["output_ports"] = singleOutputPort();
        node["variadic_inputs"] = false;
        nodes.push_back(node);
    }

    // Combine — fans in >=2 upstream frames into one flattened feature vector.
    nodes.push_back(
        {{"node_type", "combine"},
         {"kind", "combine"},
         {"category", "transform"},
         {"runner", "cpp_dsp"},
         {"label", "Combine"},
         {"description",
          "Fans in two or more upstream channel frames into one flattened "
          "feature-vector frame (e.g. several feature extractors into a model "
          "input)."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", singleInputPort()},
         {"output_ports", singleOutputPort()},
         {"variadic_inputs", true}});

    // Viewer — renders its upstream output on the canvas (runs in the browser).
    nodes.push_back(
        {{"node_type", "viewer"},
         {"kind", "viewer"},
         {"category", "viewer"},
         {"runner", "frontend"},
         {"label", "Viewer"},
         {"description",
          "Renders the upstream node's output live on the canvas. The renderer "
          "is chosen from the output descriptor's capabilities."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", singleInputPort()},
         {"output_ports", nlohmann::json::array()},
         {"variadic_inputs", false}});

    // Sink — persists/forwards its upstream output.
    nodes.push_back(
        {{"node_type", "sink"},
         {"kind", "sink"},
         {"category", "sink"},
         {"runner", "kafka_sink"},
         {"label", "Sink"},
         {"description",
          "Terminal node that consumes its upstream output (e.g. records it)."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", singleInputPort()},
         {"output_ports", nlohmann::json::array()},
         {"variadic_inputs", false}});

    // Markers — a config-less SOURCE of the bound experiment's cue/session
    // marker timeline (MarkerEventV1 on Marker/<experiment_id>).
    //
    // This is what is left on the canvas after the experiment became a
    // first-class object (experiment-history-snapshots-plan): markers are WIRED
    // data -- the marker viewer overlay, topic-aware combine's marker lane and
    // the export node's label join all consume them -- so the wiring stays, but
    // the protocol authoring, participant, notes and the Record button move to
    // experiment-level UI. The node has nothing to configure: it resolves its
    // topic from the graph's bound experiment_id, so it is correct by
    // construction rather than by the author retyping an id.
    //
    // The retired `experiment` kind is still PARSED (see from_json) so old
    // boards load, but it is no longer advertised here -- there is nothing to
    // author on it any more.
    const auto markersOutputPort = []() {
        return nlohmann::json::array(
            {{{"id", "markers"}, {"label", "Markers"}}});
    };
    nodes.push_back(
        {{"node_type", "markers"},
         {"kind", "markers"},
         {"category", "source"},
         {"runner", "kafka_source"},
         {"label", "Markers"},
         {"description",
          "The bound experiment's marker timeline (cue + session events) as a "
          "stream. Wire it into a viewer to see cues on the trace, into combine "
          "to carry markers alongside data, or into export to label each row. "
          "Nothing to configure — it follows whichever experiment owns this "
          "board; bind one from the board header."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", nlohmann::json::array()},
         {"output_ports", markersOutputPort()},
         {"variadic_inputs", false}});

    // Export — writes its inputs to a durable dataset file (Parquet) via a
    // control-plane job submitted client-side. Terminal like a sink, but
    // topic-aware and variadic: data inputs become the exported rows and a
    // `markers` input supplies both the session window and the cue label joined
    // onto each row. No stream output — its artifact is a file.
    nodes.push_back(
        {{"node_type", "export"},
         {"kind", "export"},
         {"category", "export"},
         {"runner", "control_plane"},
         {"label", "Export"},
         {"description",
          "Writes a labeled dataset file (Parquet) from its inputs. Wire a data "
          "stream in for the rows and a markers node in to scope the "
          "session window and label each row with the active cue. Run the export "
          "from the inspector; artifacts land in the server's export directory."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", singleInputPort()},
         {"output_ports", nlohmann::json::array()},
         {"variadic_inputs", true}});

    // Train — submits a control-plane train_validate job from a labeled dataset
    // (client-driven via the ML proxy); its output is a durable model artifact,
    // not a stream. Config (families/features/windowing/run+field selection)
    // lives on the node.
    nodes.push_back(
        {{"node_type", "train"},
         {"kind", "train"},
         {"category", "ml"},
         {"runner", "control_plane"},
         {"label", "Train"},
         {"description",
          "Trains a classifier on a recorded session dataset (model families + "
          "feature windowing), producing a durable model artifact a classify "
          "node can load. Pick the recorded runs in the inspector, and wire its "
          "models output into a classify node to serve them."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", nlohmann::json::array()},
         {"output_ports",
          nlohmann::json::array(
              {provPort(kProvenancePortModels, "Models")})},
         {"variadic_inputs", false}});

    return nodes;
}

struct StreamGraphPosition {
    double x = 0.0;
    double y = 0.0;
};

struct StreamGraphViewport {
    double x = 0.0;
    double y = 0.0;
    double zoom = 1.0;
};

struct StreamGraphNode {
    std::string id{};
    std::string kind{};
    std::string label{};
    StreamGraphPosition position{};
    std::vector<std::string> inputPortIds{};
    std::vector<std::string> outputPortIds{};
    std::optional<uint64_t> streamId{};
    std::optional<std::string> schemaName{};
    std::optional<std::string> transformKind{};
    std::optional<std::string> inputMappingId{};
    nlohmann::json config = nlohmann::json::object();
    std::optional<std::string> outputIdentifier{};
    std::optional<uint64_t> outputStreamId{};
};

struct StreamGraphEdge {
    std::string id{};
    std::string sourceNodeId{};
    std::string sourcePort{};
    std::string targetNodeId{};
    std::string targetPort{};
    // Topic-aware channels: StreamType strings ("Data"/"Marker"/"Meta") the user
    // has hidden on this link, so they don't reach the target node. Empty = the
    // whole channel flows (default). Lets a viewer/combine act on only part of a
    // "stream" channel (e.g. drop the markers, keep the data).
    std::vector<std::string> hiddenTopicTypes{};
    // Provenance edges (data-lineage / control wiring, e.g. source->experiment,
    // experiment->train, train->classify) are persisted with the graph but are
    // EXCLUDED from the executed data-flow: they resolve node configuration at
    // author/submit time, not at runtime. "data" (default) is a normal streaming
    // edge; "provenance" is a lineage edge the executor + validation ignore
    // (mirrors how param nodes are dropped in flattenGraph).
    std::string edgeKind{"data"};
};

// A provenance edge carries data lineage, not a stream; the graph executor and
// data-flow validation must skip it. Centralized so every edge-iteration site
// filters identically.
inline bool isProvenanceEdge(const StreamGraphEdge& edge)
{
    return edge.edgeKind == "provenance";
}

// A node kind whose output channel carries MARKER events rather than data
// frames. Topic-aware consumers (combine's marker lane, export's label join)
// classify their inputs by this, so it is centralized: the retired `experiment`
// kind is still a marker source on boards that predate the split, and a site
// that checked only one of the two names would silently treat the other's
// markers as an unresolvable data input.
inline bool isMarkerSourceKind(const std::string& kind)
{
    return kind == "markers" || kind == "experiment";
}

struct StreamGraphDefinition {
    int graphVersion = 1;
    std::string graphId{};
    std::string label{};
    std::string description{};
    uint64_t createdAtUs = 0;
    uint64_t updatedAtUs = 0;
    StreamGraphViewport viewport{};
    std::optional<std::string> selectedNodeId{};
    std::vector<StreamGraphNode> nodes{};
    std::vector<StreamGraphEdge> edges{};
    std::vector<std::string> notes{};
    // Opaque frontend-only metadata (Phase 7): the unflattened composite editor
    // tree. The backend stores and returns it verbatim (never interprets it) so
    // a saved composite graph reloads from the backend alone; the executed graph
    // is still the flattened `nodes`/`edges` above.
    nlohmann::json editorMetadata = nlohmann::json(nullptr);

    // --- Experiment history (experiment-history-snapshots-plan) --------------
    // A graph is one of three things:
    //   live graph  : experimentId set, instanceId empty            (editable)
    //   recording   : instanceId set, immutable=true, origin=recording
    //   fork        : instanceId set, immutable=false, origin=fork, forkedFrom set
    // A fork is a recording that happens to be editable -- same record shape,
    // same place in the tree, and it INHERITS `recording` so it points at the
    // very same materialized artifacts. Forking never copies data.
    // --- Workspace membership (TEC-NATKIT-56) --------------------------------
    // Which workspace this belongs to, or empty for "Unfiled". Membership lives
    // on the MEMBER and nowhere else: a workspace holding its own list of ids
    // would be a second copy of the same fact, and the two would drift the first
    // time something was deleted while a client held a stale list.
    //
    // Nullable on purpose -- everything that existed before workspaces reads as
    // Unfiled, so there is no migration and no store version bump.
    std::string workspaceId{};

    std::string experimentId{};
    std::string instanceId{};
    bool immutable = false;
    std::string origin{};       // "recording" | "fork" | "" (live graph)
    std::string forkedFrom{};   // parent instance_id, for fork-of-fork nesting
    // The captured session: session_id, window, recorded streams, artifact refs,
    // status. Opaque here -- the backend stores and returns it verbatim in this
    // phase; materialization interprets it later.
    nlohmann::json recording = nlohmann::json(nullptr);
};

struct StreamGraphDiagnostic {
    std::string severity{"error"};
    std::string code{};
    std::string message{};
};

struct StreamGraphValidationResult {
    bool valid = true;
    std::vector<StreamGraphDiagnostic> graphDiagnostics{};
    std::unordered_map<std::string, std::vector<StreamGraphDiagnostic>>
        nodeDiagnostics{};
    std::unordered_map<std::string, std::vector<StreamGraphDiagnostic>>
        edgeDiagnostics{};
};

// One topic carried by a node's output channel (Part A: a channel is a topic
// set, at most one per StreamType). `type` is the StreamType string
// ("Data"/"Marker"/"Meta"); `id` is the stableStreamId of the full topic.
struct StreamGraphOutputTopic {
    std::string type{};
    uint64_t id = 0;
    std::string schemaName{};
};

struct StreamGraphNodeRuntimeStatus {
    std::string state{"draft"};
    std::optional<uint64_t> outputStreamId{};
    std::optional<std::string> workerId{};
    std::optional<std::string> threadSlotId{};
    uint64_t framesProcessed = 0;
    uint64_t lastFrameAtUs = 0;
    std::optional<std::string> message{};
    // The node's output channel: an ordered topic set, at most one per type
    // (Part A). Empty for nodes with no output. Kept LAST so existing positional
    // aggregate initializers (7 fields) still compile; populated after
    // construction. When present it includes the DATA topic id equal to
    // outputStreamId for the one-topic (backward-compat) case.
    std::vector<StreamGraphOutputTopic> outputTopics{};
};

// Build one output-channel topic entry directly (used where no worker exists,
// e.g. an experiment's MARKER output, a raw source's DATA topic, or the combine
// merger's output). `type` becomes the StreamType string ("Data"/"Marker"/…).
inline StreamGraphOutputTopic makeChannelTopic(
    nat::core::StreamType type, uint64_t id, std::string schema_name)
{
    return StreamGraphOutputTopic{
        nat::core::toString(type), id, std::move(schema_name)};
}

struct StreamGraphRuntimeState {
    std::string graphId{};
    std::string activeRunId{};
    std::string runState{"stopped"};
    std::unordered_map<std::string, StreamGraphNodeRuntimeStatus> nodeStatuses{};
    std::vector<uint64_t> outputStreamIds{};
    // Set when this run's sources were repointed at a replay's scratch topics. The
    // replay stops the run it owns when it ends, and this is how it recognises it:
    // by then the user may have stopped the run and started a live one, which must
    // be left alone.
    std::string boundReplayId{};
};

struct LiveTransformWorkerSnapshot {
    uint64_t framesProcessed = 0;
    uint64_t lastFrameAtUs = 0;
    std::string workerId{};
    std::string threadSlotId{};
};

std::optional<LiveTransformWorkerSnapshot> getLiveTransformWorkerSnapshot(
    uint64_t output_stream_id);
// Checks the transform-worker registry first, then the combine-worker
// registry — a node's output_stream_id lives in exactly one of the two.
std::optional<LiveTransformWorkerSnapshot> getLiveGraphWorkerSnapshot(
    uint64_t output_stream_id);

void to_json(nlohmann::json& json, const StreamGraphPosition& value)
{
    json = {{"x", value.x}, {"y", value.y}};
}

void from_json(const nlohmann::json& json, StreamGraphPosition& value)
{
    value.x = json.at("x").get<double>();
    value.y = json.at("y").get<double>();
}

void to_json(nlohmann::json& json, const StreamGraphViewport& value)
{
    json = {{"x", value.x}, {"y", value.y}, {"zoom", value.zoom}};
}

void from_json(const nlohmann::json& json, StreamGraphViewport& value)
{
    value.x = json.value("x", 0.0);
    value.y = json.value("y", 0.0);
    value.zoom = json.value("zoom", 1.0);
}

void to_json(nlohmann::json& json, const StreamGraphNode& value)
{
    json = {
        {"id", value.id},
        {"kind", value.kind},
        {"label", value.label},
        {"position", value.position},
    };
    if (!value.inputPortIds.empty()) {
        json["input_port_ids"] = value.inputPortIds;
    }
    if (!value.outputPortIds.empty()) {
        json["output_port_ids"] = value.outputPortIds;
    }
    if (value.streamId.has_value()) {
        json["stream_id"] = std::to_string(value.streamId.value());
    }
    if (value.schemaName.has_value()) {
        json["schema_name"] = value.schemaName.value();
    }
    if (value.transformKind.has_value()) {
        json["transform_kind"] = value.transformKind.value();
    }
    if (value.inputMappingId.has_value()) {
        json["input_mapping_id"] = value.inputMappingId.value();
    }
    if (!value.config.is_null() && !value.config.empty()) {
        json["config"] = value.config;
    }
    if (value.outputIdentifier.has_value()) {
        json["output_identifier"] = value.outputIdentifier.value();
    }
    if (value.outputStreamId.has_value()) {
        json["output_stream_id"] = std::to_string(value.outputStreamId.value());
    }
}

void from_json(const nlohmann::json& json, StreamGraphNode& value)
{
    value.id = json.at("id").get<std::string>();
    value.kind = json.at("kind").get<std::string>();
    // Backward compat: the "session" node kind was renamed to "experiment"
    // (same recording semantics, now with a markers output). Old persisted
    // graphs still load.
    if (value.kind == "session") {
        value.kind = "experiment";
    }
    value.label = json.at("label").get<std::string>();
    value.position = json.at("position").get<StreamGraphPosition>();
    value.inputPortIds =
        json.value("input_port_ids", std::vector<std::string>{});
    value.outputPortIds =
        json.value("output_port_ids", std::vector<std::string>{});

    if (json.contains("stream_id")) {
        auto parsed = parseStreamId(json["stream_id"]);
        if (!parsed.has_value()) {
            throw std::runtime_error("stream_id must be a valid non-negative integer");
        }
        value.streamId = parsed.value();
    }
    if (json.contains("schema_name") && json["schema_name"].is_string()) {
        value.schemaName = json["schema_name"].get<std::string>();
    }
    if (json.contains("transform_kind") && json["transform_kind"].is_string()) {
        value.transformKind = json["transform_kind"].get<std::string>();
    }
    if (json.contains("input_mapping_id") && json["input_mapping_id"].is_string()) {
        value.inputMappingId = json["input_mapping_id"].get<std::string>();
    }
    if (json.contains("config")) {
        value.config = json["config"];
    }
    if (json.contains("output_identifier") && json["output_identifier"].is_string()) {
        value.outputIdentifier = json["output_identifier"].get<std::string>();
    }
    if (json.contains("output_stream_id")) {
        auto parsed = parseStreamId(json["output_stream_id"]);
        if (!parsed.has_value()) {
            throw std::runtime_error(
                "output_stream_id must be a valid non-negative integer");
        }
        value.outputStreamId = parsed.value();
    }
}

void to_json(nlohmann::json& json, const StreamGraphEdge& value)
{
    json = {
        {"id", value.id},
        {"source_node_id", value.sourceNodeId},
        {"source_port", value.sourcePort},
        {"target_node_id", value.targetNodeId},
        {"target_port", value.targetPort},
    };
    if (!value.hiddenTopicTypes.empty()) {
        json["hidden_topic_types"] = value.hiddenTopicTypes;
    }
    if (value.edgeKind != "data") {
        json["edge_kind"] = value.edgeKind;
    }
}

void from_json(const nlohmann::json& json, StreamGraphEdge& value)
{
    value.id = json.at("id").get<std::string>();
    value.sourceNodeId = json.at("source_node_id").get<std::string>();
    value.sourcePort = json.at("source_port").get<std::string>();
    value.targetNodeId = json.at("target_node_id").get<std::string>();
    value.targetPort = json.at("target_port").get<std::string>();
    if (json.contains("hidden_topic_types") &&
        json.at("hidden_topic_types").is_array()) {
        value.hiddenTopicTypes =
            json.at("hidden_topic_types").get<std::vector<std::string>>();
    }
    value.edgeKind = json.value("edge_kind", std::string{"data"});
}

void to_json(nlohmann::json& json, const StreamGraphDefinition& value)
{
    json = {
        {"graph_version", value.graphVersion},
        {"graph_id", value.graphId},
        {"label", value.label},
        {"description", value.description},
        {"created_at_us", value.createdAtUs},
        {"updated_at_us", value.updatedAtUs},
        {"nodes", value.nodes},
        {"edges", value.edges},
        {"notes", value.notes},
        {"ui",
         {{"viewport", value.viewport},
          {"selected_node_id",
           value.selectedNodeId.has_value()
               ? nlohmann::json(value.selectedNodeId.value())
               : nlohmann::json(nullptr)}}},
    };
    if (!value.editorMetadata.is_null()) {
        json["editor_metadata"] = value.editorMetadata;
    }
    // Emitted only when set, so a plain board's JSON is unchanged.
    if (!value.experimentId.empty()) {
        json["experiment_id"] = value.experimentId;
    }
    if (!value.workspaceId.empty()) {
        json["workspace_id"] = value.workspaceId;
    }
    if (!value.instanceId.empty()) {
        json["instance_id"] = value.instanceId;
        json["immutable"] = value.immutable;
    }
    if (!value.origin.empty()) {
        json["origin"] = value.origin;
    }
    if (!value.forkedFrom.empty()) {
        json["forked_from"] = value.forkedFrom;
    }
    if (!value.recording.is_null()) {
        json["recording"] = value.recording;
    }
}

void from_json(const nlohmann::json& json, StreamGraphDefinition& value)
{
    value.graphVersion = json.at("graph_version").get<int>();
    value.graphId = json.at("graph_id").get<std::string>();
    value.label = json.at("label").get<std::string>();
    value.description = json.value("description", std::string{});
    value.createdAtUs = json.value("created_at_us", static_cast<uint64_t>(0));
    value.updatedAtUs = json.value("updated_at_us", static_cast<uint64_t>(0));
    value.nodes = json.at("nodes").get<std::vector<StreamGraphNode>>();
    value.edges = json.value("edges", std::vector<StreamGraphEdge>{});
    // Migration: drop the retired provenance ports (prov_source,
    // prov_experiment) and every edge that referenced them. Both halves matter.
    // Without the edge half an old board loads with edges pointing at ports that
    // no longer exist and goes invalid on a diagnostic the author can't fix;
    // without the port half the stored port list still lists them, so a legacy
    // board keeps RENDERING a dead lineage port until something happens to
    // re-save it. Doing it here, at the parse boundary, means neither the
    // executor nor the editor ever sees them.
    const auto isRetiredPort = [](const std::string& port) {
        return port == "prov_source" || port == "prov_experiment";
    };
    value.edges.erase(
        std::remove_if(
            value.edges.begin(),
            value.edges.end(),
            [&isRetiredPort](const StreamGraphEdge& edge) {
                return isRetiredPort(edge.sourcePort) ||
                       isRetiredPort(edge.targetPort);
            }),
        value.edges.end());
    for (auto& node : value.nodes) {
        node.inputPortIds.erase(
            std::remove_if(node.inputPortIds.begin(), node.inputPortIds.end(),
                           isRetiredPort),
            node.inputPortIds.end());
        node.outputPortIds.erase(
            std::remove_if(node.outputPortIds.begin(), node.outputPortIds.end(),
                           isRetiredPort),
            node.outputPortIds.end());
    }
    value.notes = json.value("notes", std::vector<std::string>{});
    value.editorMetadata = json.value("editor_metadata", nlohmann::json(nullptr));
    value.experimentId = json.value("experiment_id", std::string{});
    value.workspaceId = json.value("workspace_id", std::string{});
    value.instanceId = json.value("instance_id", std::string{});
    value.immutable = json.value("immutable", false);
    value.origin = json.value("origin", std::string{});
    value.forkedFrom = json.value("forked_from", std::string{});
    value.recording = json.value("recording", nlohmann::json(nullptr));
    if (json.contains("ui") && json["ui"].is_object()) {
        const auto& ui = json["ui"];
        if (ui.contains("viewport")) {
            value.viewport = ui["viewport"].get<StreamGraphViewport>();
        }
        if (ui.contains("selected_node_id") && !ui["selected_node_id"].is_null()) {
            value.selectedNodeId = ui["selected_node_id"].get<std::string>();
        }
    }
}

void to_json(nlohmann::json& json, const StreamGraphDiagnostic& value)
{
    json = {
        {"severity", value.severity},
        {"code", value.code},
        {"message", value.message},
    };
}

void to_json(nlohmann::json& json, const StreamGraphNodeRuntimeStatus& value)
{
    json = {
        {"state", value.state},
        {"frames_processed", value.framesProcessed},
        {"last_frame_at_us", value.lastFrameAtUs},
    };
    if (value.outputStreamId.has_value()) {
        json["output_stream_id"] = std::to_string(value.outputStreamId.value());
    }
    if (!value.outputTopics.empty()) {
        nlohmann::json topics = nlohmann::json::array();
        for (const auto& topic : value.outputTopics) {
            topics.push_back({
                {"type", topic.type},
                {"id", std::to_string(topic.id)},
                {"schema", topic.schemaName},
            });
        }
        json["output_topics"] = std::move(topics);
    }
    if (value.workerId.has_value()) {
        json["worker_id"] = value.workerId.value();
    }
    if (value.threadSlotId.has_value()) {
        json["thread_slot_id"] = value.threadSlotId.value();
    }
    if (value.message.has_value()) {
        json["message"] = value.message.value();
    }
}

std::filesystem::path resolveStreamGraphStorePath()
{
    const char* store_path = std::getenv("NATKIT_STREAM_GRAPH_STORE");
    if (store_path != nullptr && store_path[0] != '\0') {
        return std::filesystem::path(store_path);
    }
    return std::filesystem::path("./data/stream_graphs.json");
}

std::unordered_map<std::string, StreamGraphDefinition> g_stream_graphs;
std::mutex g_stream_graph_mutex;
bool g_stream_graph_store_loaded = false;
std::string g_stream_graph_store_error{};
std::unordered_map<std::string, StreamGraphRuntimeState> g_stream_graph_runtime;

void addGraphDiagnostic(
    StreamGraphValidationResult& result,
    std::vector<StreamGraphDiagnostic>& sink,
    const std::string& code,
    const std::string& message,
    const std::string& severity = "error")
{
    sink.push_back(StreamGraphDiagnostic{severity, code, message});
    if (severity == "error") {
        result.valid = false;
    }
}

void addTopLevelGraphDiagnostic(
    StreamGraphValidationResult& result,
    const std::string& code,
    const std::string& message,
    const std::string& severity = "error")
{
    addGraphDiagnostic(result, result.graphDiagnostics, code, message, severity);
}

void normalizeGraphNodePorts(StreamGraphNode& node)
{
    // Provenance ports (prov_*) carry lineage, not data — they survive
    // normalization regardless of the kind's data-port rules. Collect the ones
    // the node already declares so they can be re-appended after the data ports
    // are reset, then ensure the kind's own dedicated stub is present.
    // Retired provenance ports are dropped rather than preserved: an old board
    // that still declares prov_source / prov_experiment loses those ports (and
    // with them the edges that targeted them) on its first normalization, which
    // is what retiring them means.
    const auto collectProvenance = [](const std::vector<std::string>& ports) {
        std::vector<std::string> kept;
        for (const auto& port : ports) {
            if (isProvenancePortId(port) && port != "prov_source" &&
                port != "prov_experiment") {
                kept.push_back(port);
            }
        }
        return kept;
    };
    const auto ensurePort = [](std::vector<std::string>& ports,
                               const std::string& id) {
        if (std::find(ports.begin(), ports.end(), id) == ports.end()) {
            ports.push_back(id);
        }
    };
    auto provInputs = collectProvenance(node.inputPortIds);
    auto provOutputs = collectProvenance(node.outputPortIds);

    if (node.kind == "stream_source") {
        node.outputPortIds = {"data"};
        node.inputPortIds.clear();
    } else if (node.kind == "transform") {
        node.inputPortIds = {"input"};
        node.outputPortIds = {"output"};
        // A classifier transform (one that loads a model bundle) can be the
        // target of a train->classify provenance edge, so it gets a prov_model
        // input stub. Detected by the presence of a model_path config field.
        if (node.config.is_object() && node.config.contains("model_path")) {
            ensurePort(provInputs, kProvenancePortModel);
        }
        if (node.config.is_null()) {
            node.config = nlohmann::json::object();
        }
    } else if (node.kind == "viewer" || node.kind == "sink") {
        node.inputPortIds = {"input"};
        node.outputPortIds.clear();
    } else if (node.kind == "markers" || node.kind == "experiment") {
        // A markers node is source-like: it republishes the bound experiment's
        // marker timeline, so it has no inputs and exposes exactly one output
        // port, `markers`. The retired `experiment` kind normalizes identically
        // (its protocol config is ignored; the experiment record owns it now), so
        // an old board keeps the same port shape and its edges stay valid.
        node.inputPortIds.clear();
        node.outputPortIds = {"markers"};
        if (node.config.is_null()) {
            node.config = nlohmann::json::object();
        }
    } else if (node.kind == "export") {
        // An export node is terminal and variadic: it fans in one or more data
        // inputs (the exported rows) plus optional experiment `markers` inputs
        // (session window + cue labels), and produces a file, not a stream. Keep
        // whatever data inputs the author wired (defaulting to one) and clear the
        // outputs, like a sink.
        if (node.inputPortIds.empty()) {
            node.inputPortIds = {"input"};
        } else {
            // Drop provenance ids from the data-port list; they are re-appended
            // below with the other preserved provenance ports.
            std::vector<std::string> dataInputs;
            for (const auto& port : node.inputPortIds) {
                if (!isProvenancePortId(port)) {
                    dataInputs.push_back(port);
                }
            }
            node.inputPortIds =
                dataInputs.empty() ? std::vector<std::string>{"input"} : dataInputs;
        }
        node.outputPortIds.clear();
        if (node.config.is_null()) {
            node.config = nlohmann::json::object();
        }
    } else if (node.kind == "train") {
        // A train node submits a control-plane job (client-driven); no DATA
        // stream output. It keeps a prov_models output stub (train->classify
        // lineage: the models it produced). The retired prov_experiment input is
        // dropped here, so an old board's experiment->train edge falls away with
        // the port it targeted.
        node.inputPortIds.clear();
        node.outputPortIds.clear();
        ensurePort(provOutputs, kProvenancePortModels);
        if (node.config.is_null()) {
            node.config = nlohmann::json::object();
        }
    }

    // Re-append the preserved/ensured provenance ports after the data ports.
    for (const auto& port : provInputs) {
        if (std::find(node.inputPortIds.begin(), node.inputPortIds.end(), port) ==
            node.inputPortIds.end()) {
            node.inputPortIds.push_back(port);
        }
    }
    for (const auto& port : provOutputs) {
        if (std::find(node.outputPortIds.begin(), node.outputPortIds.end(),
                      port) == node.outputPortIds.end()) {
            node.outputPortIds.push_back(port);
        }
    }
}

nlohmann::json makeGraphStatusJson(const StreamGraphDefinition& graph)
{
    nlohmann::json json;
    json["graph_id"] = graph.graphId;
    json["run_state"] = "stopped";
    json["active_run_id"] = nullptr;
    json["node_statuses"] = nlohmann::json::object();

    for (const auto& node : graph.nodes) {
        if (node.kind == "stream_source" && node.streamId.has_value()) {
            json["node_statuses"][node.id] = StreamGraphNodeRuntimeStatus{
                "stopped",
                node.streamId,
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>("Source stream is available to downstream nodes.")};
            continue;
        }
        if ((node.kind == "transform" || node.kind == "combine") &&
            node.outputStreamId.has_value()) {
            json["node_statuses"][node.id] = StreamGraphNodeRuntimeStatus{
                "stopped",
                node.outputStreamId,
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>("Last known derived stream from a previous graph run.")};
        }
    }

    const auto runtime_search = g_stream_graph_runtime.find(graph.graphId);
    if (runtime_search != g_stream_graph_runtime.end()) {
        const auto& runtime = runtime_search->second;
        auto node_statuses = runtime.nodeStatuses;

        for (auto& entry : node_statuses) {
            auto& status = entry.second;
            if (!status.outputStreamId.has_value() ||
                (!status.workerId.has_value() && !status.threadSlotId.has_value())) {
                continue;
            }

            const auto live_worker =
                getLiveGraphWorkerSnapshot(status.outputStreamId.value());
            if (!live_worker.has_value()) {
                if (status.state == "running" || status.state == "stalled" ||
                    status.state == "starting") {
                    status.state = runtime.runState == "stopped"
                        ? "stopped"
                        : "blocked";
                }
                continue;
            }

            status.framesProcessed = live_worker->framesProcessed;
            status.lastFrameAtUs = live_worker->lastFrameAtUs;
            status.workerId = live_worker->workerId;
            status.threadSlotId = live_worker->threadSlotId;
            status.state =
                classifyTransformWorkerStatus(1, status.lastFrameAtUs);
        }

        std::string derived_run_state = runtime.runState;
        bool has_running_node = false;
        bool has_stalled_node = false;
        bool has_error_node = false;
        bool has_starting_node = false;
        for (const auto& entry : node_statuses) {
            const auto& state = entry.second.state;
            if (state == "error" || state == "blocked") {
                has_error_node = true;
            } else if (state == "stalled") {
                has_stalled_node = true;
            } else if (state == "starting") {
                has_starting_node = true;
            } else if (state == "running" || state == "valid") {
                has_running_node = true;
            }
        }
        if (runtime.runState != "stopped") {
            if (has_error_node) {
                derived_run_state = "error";
            } else if (has_stalled_node) {
                derived_run_state = "stalled";
            } else if (has_starting_node) {
                derived_run_state = "starting";
            } else if (has_running_node) {
                derived_run_state = "running";
            } else {
                derived_run_state = "stopped";
            }
        }

        json["run_state"] = derived_run_state;
        json["active_run_id"] =
            runtime.activeRunId.empty() ? nlohmann::json(nullptr)
                                        : nlohmann::json(runtime.activeRunId);
        for (const auto& entry : node_statuses) {
            nlohmann::json node_json = entry.second;
            // Annotate the data edge's transport so an in-process fast-path edge
            // is as observable as a Kafka one. A live in-process producer has a
            // channel registered under its output stream id; its depth and drop
            // counts ride the same status surface Kafka transforms report.
            if (entry.second.outputStreamId.has_value()) {
                const auto channel =
                    nat::tools::InProcessChannelRegistry::global().find(
                        entry.second.outputStreamId.value());
                if (channel) {
                    const auto metrics = channel->metrics();
                    node_json["transport"] = "in_process";
                    node_json["channel"] = {
                        {"subscribers", metrics.subscriberCount},
                        {"published", metrics.published},
                        {"dropped", metrics.dropped},
                        {"depth", metrics.maxDepth},
                    };
                } else if (entry.second.state == "running" ||
                           entry.second.state == "stalled") {
                    node_json["transport"] = "kafka";
                }
            }
            json["node_statuses"][entry.first] = node_json;
        }
    }
    return json;
}

std::optional<nlohmann::json> findTransformCapabilityJsonByKind(
    const std::string& kind)
{
    for (const auto& capability : buildTransformCapabilitiesJson()) {
        if (capability.value("kind", std::string{}) == kind) {
            return capability;
        }
    }
    return std::nullopt;
}

bool descriptorSupportsRequestedInputMapping(
    const nat::core::DataSchemaDescriptor& descriptor,
    const std::string& requested_input_mapping_id)
{
    if (requested_input_mapping_id.empty() ||
        requested_input_mapping_id == "canonical_channel_frame") {
        return descriptorSupportsNumericChannelFrame(descriptor);
    }
    return findRequestedAlternateInputMapping(
               descriptor, requested_input_mapping_id)
        .has_value();
}

bool validateTransformConfigAgainstCapability(
    const nlohmann::json& capability,
    const nlohmann::json& config,
    StreamGraphValidationResult& result,
    const std::string& node_id)
{
    bool ok = true;
    auto& diagnostics = result.nodeDiagnostics[node_id];
    const auto config_fields =
        capability.value("config_fields", nlohmann::json::array());
    if (!config.is_object()) {
        addGraphDiagnostic(
            result,
            diagnostics,
            "invalid_config",
            "Transform config must be a JSON object.");
        return false;
    }

    for (const auto& field : config_fields) {
        const std::string field_id = field.value("id", std::string{});
        const bool required = field.value("required", false);
        if (field_id.empty()) {
            continue;
        }
        if (!config.contains(field_id)) {
            if (required) {
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "missing_config_field",
                    "Missing transform config field '" + field_id + "'.");
                ok = false;
            }
            continue;
        }

        const auto& value = config[field_id];
        const std::string type = field.value("type", std::string{});
        if (type == "number") {
            if (!value.is_number()) {
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "invalid_config_type",
                    "Transform config field '" + field_id + "' must be numeric.");
                ok = false;
                continue;
            }
            if (field.contains("min") &&
                value.get<double>() < field["min"].get<double>()) {
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "config_below_min",
                    "Transform config field '" + field_id +
                        "' is below the allowed minimum.");
                ok = false;
            }
        } else if (type == "enum") {
            if (!value.is_string()) {
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "invalid_config_type",
                    "Transform config field '" + field_id + "' must be a string.");
                ok = false;
                continue;
            }
            const auto options =
                field.value("options", std::vector<std::string>{});
            if (!options.empty() &&
                std::find(options.begin(), options.end(), value.get<std::string>()) ==
                    options.end()) {
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "invalid_config_option",
                    "Transform config field '" + field_id +
                        "' is not one of the supported options.");
                ok = false;
            }
        } else if (type == "string") {
            if (!value.is_string()) {
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "invalid_config_type",
                    "Transform config field '" + field_id + "' must be a string.");
                ok = false;
            } else if (required && value.get_ref<const std::string&>().empty()) {
                // Empty is allowed for optional string fields (e.g. an
                // emg_gesture_classify model_path that is filled after training);
                // the actual requirement is enforced at start time.
                addGraphDiagnostic(
                    result,
                    diagnostics,
                    "invalid_config_type",
                    "Transform config field '" + field_id +
                        "' must be a non-empty string.");
                ok = false;
            }
        }
    }
    return ok;
}

void ensureStreamGraphStoreLoadedLocked()
{
    if (g_stream_graph_store_loaded) {
        return;
    }

    g_stream_graph_store_loaded = true;
    g_stream_graph_store_error.clear();
    g_stream_graphs.clear();

    const auto store_path = resolveStreamGraphStorePath();
    if (!std::filesystem::exists(store_path)) {
        return;
    }

    std::ifstream input(store_path);
    if (!input) {
        g_stream_graph_store_error =
            "Failed to open stream graph store at " + store_path.string();
        return;
    }

    try {
        nlohmann::json json = nlohmann::json::parse(input);
        if (json.value("store_version", 0) != 1 || !json["graphs"].is_array()) {
            throw std::runtime_error(
                "Stream graph store must contain store_version=1 and a graphs array");
        }
        for (const auto& graph_json : json["graphs"]) {
            StreamGraphDefinition graph = graph_json.get<StreamGraphDefinition>();
            for (auto& node : graph.nodes) {
                normalizeGraphNodePorts(node);
            }
            g_stream_graphs[graph.graphId] = std::move(graph);
        }
    } catch (const std::exception& exception) {
        g_stream_graphs.clear();
        g_stream_graph_store_error =
            "Failed to parse stream graph store: " + std::string(exception.what());
    }
}

bool persistStreamGraphStoreLocked(std::string& error)
{
    const auto store_path = resolveStreamGraphStorePath();
    if (store_path.has_parent_path()) {
        std::filesystem::create_directories(store_path.parent_path());
    }

    nlohmann::json store_json;
    store_json["store_version"] = 1;
    store_json["graphs"] = nlohmann::json::array();
    for (const auto& entry : g_stream_graphs) {
        store_json["graphs"].push_back(entry.second);
    }

    const auto temp_path = store_path.string() + ".tmp";
    {
        std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
        if (!output) {
            error = "Failed to open temporary stream graph store file for write";
            return false;
        }
        output << store_json.dump(2);
        if (!output.good()) {
            error = "Failed while writing stream graph store";
            return false;
        }
    }

    std::error_code rename_error;
    std::filesystem::rename(temp_path, store_path, rename_error);
    if (rename_error) {
        std::filesystem::remove(store_path, rename_error);
        rename_error.clear();
        std::filesystem::rename(temp_path, store_path, rename_error);
    }
    if (rename_error) {
        error = "Failed to atomically replace stream graph store: " +
            rename_error.message();
        return false;
    }

    return true;
}

// --- Individual profiles (Phase 4) -----------------------------------------
//
// A Profile persists a person as a first-class object so they can walk up later
// and resume live classifying in one click. It is a thin pointer: the trained
// bundle path (placement-specific) is already baked into the referenced classify
// graph (graph_id) via the graph store, so "load profile" = load that graph and
// Start. The store mirrors the stream-graph store mechanics (atomic write,
// load-on-startup) under NATKIT_PROFILE_STORE.

struct Profile {
    std::string participantId;
    std::string displayName;
    // Which workspace's roster this participant is on, or empty for Unfiled
    // (TEC-NATKIT-56). ⚠️ ONE workspace, so the same person cannot yet appear in
    // two cohorts. Deliberate for now -- nothing asked for it, and a set here
    // would have to be reconciled on every workspace delete.
    std::string workspaceId;
    std::string modelPath;  // bundle path (reference/display; also baked into the graph)
    std::string graphId;    // the classify graph to reload
    std::string protocolId;
    std::string deviceId;   // channel binding
    std::vector<std::string> sessionIds;
    double bestAccuracy = 0.0;
    uint64_t createdAtUs = 0;
    uint64_t updatedAtUs = 0;
};

void to_json(nlohmann::json& json, const Profile& value)
{
    json = {
        {"participant_id", value.participantId},
        {"display_name", value.displayName},
        {"workspace_id", value.workspaceId},
        {"model_path", value.modelPath},
        {"graph_id", value.graphId},
        {"protocol_id", value.protocolId},
        {"device_id", value.deviceId},
        {"session_ids", value.sessionIds},
        {"best_accuracy", value.bestAccuracy},
        {"created_at_us", value.createdAtUs},
        {"updated_at_us", value.updatedAtUs},
    };
}

void from_json(const nlohmann::json& json, Profile& value)
{
    value.participantId = json.at("participant_id").get<std::string>();
    value.displayName = json.value("display_name", std::string{});
    value.workspaceId = json.value("workspace_id", std::string{});
    value.modelPath = json.value("model_path", std::string{});
    value.graphId = json.value("graph_id", std::string{});
    value.protocolId = json.value("protocol_id", std::string{});
    value.deviceId = json.value("device_id", std::string{});
    value.sessionIds = json.value("session_ids", std::vector<std::string>{});
    value.bestAccuracy = json.value("best_accuracy", 0.0);
    value.createdAtUs = json.value("created_at_us", static_cast<uint64_t>(0));
    value.updatedAtUs = json.value("updated_at_us", static_cast<uint64_t>(0));
}

std::filesystem::path resolveProfileStorePath()
{
    const char* store_path = std::getenv("NATKIT_PROFILE_STORE");
    if (store_path != nullptr && store_path[0] != '\0') {
        return std::filesystem::path(store_path);
    }
    return std::filesystem::path("./data/profiles.json");
}

std::unordered_map<std::string, Profile> g_profiles;
std::mutex g_profile_mutex;
bool g_profile_store_loaded = false;
std::string g_profile_store_error{};

void ensureProfileStoreLoadedLocked()
{
    if (g_profile_store_loaded) {
        return;
    }
    g_profile_store_loaded = true;
    g_profile_store_error.clear();
    g_profiles.clear();
    const auto store_path = resolveProfileStorePath();
    if (!std::filesystem::exists(store_path)) {
        return;
    }
    std::ifstream input(store_path);
    if (!input) {
        g_profile_store_error =
            "Failed to open profile store at " + store_path.string();
        return;
    }
    try {
        nlohmann::json json = nlohmann::json::parse(input);
        if (json.value("store_version", 0) != 1 || !json["profiles"].is_array()) {
            throw std::runtime_error(
                "Profile store must contain store_version=1 and a profiles array");
        }
        for (const auto& profile_json : json["profiles"]) {
            Profile profile = profile_json.get<Profile>();
            g_profiles[profile.participantId] = std::move(profile);
        }
    } catch (const std::exception& exception) {
        g_profiles.clear();
        g_profile_store_error =
            "Failed to parse profile store: " + std::string(exception.what());
    }
}

bool persistProfileStoreLocked(std::string& error)
{
    const auto store_path = resolveProfileStorePath();
    if (store_path.has_parent_path()) {
        std::filesystem::create_directories(store_path.parent_path());
    }
    nlohmann::json store_json;
    store_json["store_version"] = 1;
    store_json["profiles"] = nlohmann::json::array();
    for (const auto& entry : g_profiles) {
        store_json["profiles"].push_back(entry.second);
    }
    const auto temp_path = store_path.string() + ".tmp";
    {
        std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
        if (!output) {
            error = "Failed to open temporary profile store file for write";
            return false;
        }
        output << store_json.dump(2);
        if (!output.good()) {
            error = "Failed while writing profile store";
            return false;
        }
    }
    std::error_code rename_error;
    std::filesystem::rename(temp_path, store_path, rename_error);
    if (rename_error) {
        std::filesystem::remove(store_path, rename_error);
        rename_error.clear();
        std::filesystem::rename(temp_path, store_path, rename_error);
    }
    if (rename_error) {
        error = "Failed to atomically replace profile store: " + rename_error.message();
        return false;
    }
    return true;
}

// --- Experiments (experiment-history-snapshots-plan, Phase 1) ---------------
//
// An experiment stops being a NODE on the canvas and becomes a first-class
// stored object that OWNS a graph and (from Phase 2) a history of instances.
// What lived in the experiment node's config -- the protocol, participant and
// notes -- lives here instead; the canvas keeps only a config-less `markers`
// source node that resolves Marker/<experiment_id> from the graph's bound
// experiment.
//
// `protocol` is opaque json for the same reason `editor_metadata` is: the
// protocol shape (classes / repetitions / hold+rest timings) is authored and
// consumed entirely by the frontend, which already round-trips it through the
// node's generic config. Storing it verbatim keeps one owner of that shape.
//
// The binding to a board is 1:1 and lives on BOTH sides: the experiment names
// its `live_graph_id`, and that graph carries `experiment_id`. save_experiment
// is the sole writer of the pair (handleSaveStreamGraph deliberately re-pins
// experiment_id from the stored record), so the two can't drift.
struct Experiment {
    std::string experimentId;
    std::string label;
    // Which workspace this experiment belongs to, or empty for Unfiled
    // (TEC-NATKIT-56). Scoping the picker to this is the whole point of the
    // container: an experiment list that is every experiment ever made is not a
    // list anybody can pick from.
    std::string workspaceId;
    nlohmann::json protocol = nlohmann::json(nullptr);
    // DEPRECATED (TEC-NATKIT-55). The participant is a property of a RUN, not of
    // the procedure: one experiment records a whole cohort, so a single field here
    // could only ever name the most recent person. `start_experiment_instance`
    // carries it per run now and the instance snapshots it.
    //
    // Still read AND written verbatim on purpose: it is the only source the
    // one-time back-fill has for instances recorded before the snapshot existed,
    // and dropping it on the first save would destroy that source before the
    // migration ran. Nothing else may read it.
    std::string legacyParticipantId;
    std::string notes;
    std::string liveGraphId;  // the editable board this experiment records with
    uint64_t createdAtUs = 0;
    uint64_t updatedAtUs = 0;
};

void to_json(nlohmann::json& json, const Experiment& value)
{
    json = {
        {"experiment_id", value.experimentId},
        {"label", value.label},
        {"workspace_id", value.workspaceId},
        {"participant_id", value.legacyParticipantId},
        {"notes", value.notes},
        {"live_graph_id", value.liveGraphId},
        {"created_at_us", value.createdAtUs},
        {"updated_at_us", value.updatedAtUs},
    };
    json["protocol"] = value.protocol;
}

void from_json(const nlohmann::json& json, Experiment& value)
{
    value.experimentId = json.at("experiment_id").get<std::string>();
    value.label = json.value("label", std::string{});
    value.workspaceId = json.value("workspace_id", std::string{});
    value.protocol = json.value("protocol", nlohmann::json(nullptr));
    value.legacyParticipantId = json.value("participant_id", std::string{});
    value.notes = json.value("notes", std::string{});
    value.liveGraphId = json.value("live_graph_id", std::string{});
    value.createdAtUs = json.value("created_at_us", static_cast<uint64_t>(0));
    value.updatedAtUs = json.value("updated_at_us", static_cast<uint64_t>(0));
}

std::filesystem::path resolveExperimentStorePath()
{
    const char* store_path = std::getenv("NATKIT_EXPERIMENT_STORE");
    if (store_path != nullptr && store_path[0] != '\0') {
        return std::filesystem::path(store_path);
    }
    // The fallback is relative to the working directory, which in a container is
    // the ephemeral image layer -- fine for a local run, but it means an
    // experiment (and the history it owns) would not survive a container
    // recreate. Every compose file therefore points NATKIT_EXPERIMENT_STORE at
    // the same durable volume as the graph store.
    return std::filesystem::path("./data/experiments.json");
}

std::unordered_map<std::string, Experiment> g_experiments;
std::mutex g_experiment_mutex;
bool g_experiment_store_loaded = false;
std::string g_experiment_store_error{};

void ensureExperimentStoreLoadedLocked()
{
    if (g_experiment_store_loaded) {
        return;
    }
    g_experiment_store_loaded = true;
    g_experiment_store_error.clear();
    g_experiments.clear();
    const auto store_path = resolveExperimentStorePath();
    if (!std::filesystem::exists(store_path)) {
        return;
    }
    std::ifstream input(store_path);
    if (!input) {
        g_experiment_store_error =
            "Failed to open experiment store at " + store_path.string();
        return;
    }
    try {
        nlohmann::json json = nlohmann::json::parse(input);
        if (json.value("store_version", 0) != 1 || !json["experiments"].is_array()) {
            throw std::runtime_error(
                "Experiment store must contain store_version=1 and an experiments array");
        }
        for (const auto& experiment_json : json["experiments"]) {
            Experiment experiment = experiment_json.get<Experiment>();
            g_experiments[experiment.experimentId] = std::move(experiment);
        }
    } catch (const std::exception& exception) {
        g_experiments.clear();
        g_experiment_store_error =
            "Failed to parse experiment store: " + std::string(exception.what());
    }
}

// Apply the graph side of the 1:1 experiment<->board binding: stamp
// `experiment_id` onto the bound board and clear it from any OTHER live board
// that pointed at this experiment. Instances are left alone -- they belong to
// this experiment's history permanently, and re-binding the live board must not
// disown them.
//
// Called with g_experiment_mutex held and takes g_stream_graph_mutex, so the
// lock order is always experiment -> graph. Nothing acquires them the other way
// round (the graph handlers never touch the experiment store).
bool applyExperimentGraphBindingLocked(
    const std::string& experiment_id,
    const std::string& live_graph_id,
    // The bound experiment's workspace, stamped onto the board along with the
    // binding (TEC-NATKIT-56). A board carries its own workspaceId so an
    // unbound analysis board has somewhere to live, but once it is bound the
    // experiment's filing wins -- two halves of one binding sitting in different
    // workspaces is a state no picker could show honestly. This function is
    // already the sole writer of the pair, so it is the only place that needs to
    // know.
    const std::string& workspace_id,
    std::string& error)
{
    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    ensureStreamGraphStoreLoadedLocked();
    if (!g_stream_graph_store_error.empty()) {
        error = g_stream_graph_store_error;
        return false;
    }
    if (!live_graph_id.empty()) {
        const auto target = g_stream_graphs.find(live_graph_id);
        if (target == g_stream_graphs.end()) {
            error = "Unknown graph: " + live_graph_id;
            return false;
        }
        if (!target->second.instanceId.empty()) {
            error = "Graph '" + live_graph_id +
                    "' is instance " + target->second.instanceId +
                    ", not a live board. An experiment records with an editable "
                    "board; fork the instance if you want to edit it.";
            return false;
        }
    }

    bool changed = false;
    for (auto& entry : g_stream_graphs) {
        auto& graph = entry.second;
        if (!graph.instanceId.empty()) {
            continue;  // history: never re-parented by a binding change
        }
        const bool should_be_bound = !live_graph_id.empty() && entry.first == live_graph_id;
        if (should_be_bound && graph.workspaceId != workspace_id) {
            // Follows the experiment INCLUDING when the experiment is Unfiled:
            // dragging an experiment out of a workspace has to take its board
            // with it, or the board is orphaned in a workspace whose experiment
            // list no longer mentions it.
            graph.workspaceId = workspace_id;
            graph.updatedAtUs = nowUs();
            changed = true;
        }
        if (should_be_bound && graph.experimentId != experiment_id) {
            graph.experimentId = experiment_id;
            graph.updatedAtUs = nowUs();
            changed = true;
        } else if (!should_be_bound && graph.experimentId == experiment_id) {
            graph.experimentId.clear();
            graph.updatedAtUs = nowUs();
            changed = true;
        }
    }
    if (!changed) {
        return true;
    }
    return persistStreamGraphStoreLocked(error);
}

// One-time repair of instances recorded before the participant and protocol were
// snapshotted into the run (TEC-NATKIT-54). Their `recording` carries neither, so
// "whose run is this" could only be answered by reading the live experiment's
// editable fields -- and editing those silently re-attributed every past run.
//
// Recovers what the mapping can still tell us and marks it: a back-filled value
// is stamped `participant_backfilled` / `protocol_backfilled` so it can never be
// mistaken for one captured at record time. That distinction matters more than
// the value -- a recovered attribution is evidence about our bookkeeping, not
// about the session.
//
// ⚠️ This writes to instances that are SEALED (`immutable`). That is deliberate
// and is the one place allowed to: it adds attribution metadata only, touches no
// data file and invalidates no checksum. handleSaveStreamGraph still refuses to
// let a client do the same thing.
//
// Idempotent -- an instance that already has a participant_id is skipped, so a
// re-run cannot overwrite a real capture with a recovered one.
//
// Called with g_experiment_mutex held and takes g_stream_graph_mutex, keeping the
// lock order experiment -> graph like applyExperimentGraphBindingLocked.
void backfillInstanceParticipantsLocked()
{
    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    ensureStreamGraphStoreLoadedLocked();
    if (!g_stream_graph_store_error.empty()) {
        LOG_ERROR << "Participant back-fill skipped: " << g_stream_graph_store_error;
        return;
    }

    size_t repaired = 0;    // instances touched at all
    size_t filled = 0;      // ... of which a real participant was recovered
    size_t unrecorded = 0;  // ... of which nobody had ever entered one
    std::vector<std::string> unresolved;
    for (auto& entry : g_stream_graphs) {
        auto& graph = entry.second;
        if (graph.instanceId.empty() || graph.recording.is_null()) {
            continue;  // a live board has nothing recorded to attribute
        }
        if (graph.recording.contains("participant_id")) {
            continue;
        }
        const auto experiment = g_experiments.find(graph.experimentId);
        if (experiment == g_experiments.end()) {
            // The owning experiment is gone, so nothing can say who this was.
            // Named in the log rather than passed over: an instance that can
            // never be attributed is a permanent hole, and a silent one reads
            // as a clean migration.
            unresolved.push_back(graph.graphId);
            continue;
        }
        graph.recording["participant_id"] = experiment->second.legacyParticipantId;
        // THREE distinct states, and collapsing any two of them is how a data set
        // starts lying: captured at record time (neither flag), recovered from the
        // experiment record afterwards (`participant_backfilled`), or never
        // entered by anyone (`participant_unrecorded`). An empty string stamped
        // "back-filled" would claim an attribution that never existed, which is
        // worse than the missing field it replaced.
        if (experiment->second.legacyParticipantId.empty()) {
            graph.recording["participant_unrecorded"] = true;
            ++unrecorded;
        } else {
            graph.recording["participant_backfilled"] = true;
            ++filled;
        }
        if (!graph.recording.contains("protocol")) {
            graph.recording["protocol"] = experiment->second.protocol;
            graph.recording["protocol_backfilled"] = true;
        }
        graph.updatedAtUs = nowUs();
        ++repaired;
    }

    if (!unresolved.empty()) {
        LOG_WARN << "Participant back-fill could not attribute "
                 << unresolved.size()
                 << " instance(s) -- their experiment no longer exists: "
                 << nlohmann::json(unresolved).dump();
    }
    if (repaired == 0) {
        return;
    }
    std::string persist_error;
    if (!persistStreamGraphStoreLocked(persist_error)) {
        LOG_ERROR << "Participant back-fill could not be persisted, so it will be "
                     "retried next start: "
                  << persist_error;
        return;
    }
    // All three counts, every time. "Repaired 2" alone would read as two runs
    // attributed, when it can equally mean two runs proven unattributable.
    LOG_INFO << "Participant back-fill repaired " << repaired
             << " pre-existing instance(s): " << filled
             << " attributed from their experiment record, " << unrecorded
             << " had no participant ever entered";
}

bool persistExperimentStoreLocked(std::string& error)
{
    const auto store_path = resolveExperimentStorePath();
    if (store_path.has_parent_path()) {
        std::filesystem::create_directories(store_path.parent_path());
    }
    nlohmann::json store_json;
    store_json["store_version"] = 1;
    store_json["experiments"] = nlohmann::json::array();
    for (const auto& entry : g_experiments) {
        store_json["experiments"].push_back(entry.second);
    }
    const auto temp_path = store_path.string() + ".tmp";
    {
        std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
        if (!output) {
            error = "Failed to open temporary experiment store file for write";
            return false;
        }
        output << store_json.dump(2);
        if (!output.good()) {
            error = "Failed while writing experiment store";
            return false;
        }
    }
    std::error_code rename_error;
    std::filesystem::rename(temp_path, store_path, rename_error);
    if (rename_error) {
        std::filesystem::remove(store_path, rename_error);
        rename_error.clear();
        std::filesystem::rename(temp_path, store_path, rename_error);
    }
    if (rename_error) {
        error =
            "Failed to atomically replace experiment store: " + rename_error.message();
        return false;
    }
    return true;
}

// --- Workspaces (TEC-NATKIT-56) ------------------------------------------
//
// A selectable container, so that picking an experiment is not picking from every
// experiment ever made. The motivation is cohorts: one workspace per study, its
// experiments and boards and participant roster inside it.
//
// It is a CONTAINER and nothing more. The 1:1 experiment<->board binding is
// untouched, boards do not inherit the workspace's experiment, and protocols are
// not shared by reference -- all three were considered and declined (Zach,
// 2026-08-19), because each of them changes what an experiment IS rather than
// where it is filed.
//
// Membership lives on the MEMBER (`workspaceId` on graphs, experiments and
// profiles) and is deliberately NOT mirrored into a list here: two copies of the
// same fact drift the first time something is deleted while a client holds a
// stale list. So this record is identity and description only.
//
// Everything that predates workspaces has an empty workspaceId and reads as
// "Unfiled", which is why there is no migration and no store version bump.
struct Workspace {
    std::string workspaceId;
    std::string label;
    std::string notes;
    uint64_t createdAtUs = 0;
    uint64_t updatedAtUs = 0;
};

void to_json(nlohmann::json& json, const Workspace& value)
{
    json = {
        {"workspace_id", value.workspaceId},
        {"label", value.label},
        {"notes", value.notes},
        {"created_at_us", value.createdAtUs},
        {"updated_at_us", value.updatedAtUs},
    };
}

void from_json(const nlohmann::json& json, Workspace& value)
{
    value.workspaceId = json.at("workspace_id").get<std::string>();
    value.label = json.value("label", std::string{});
    value.notes = json.value("notes", std::string{});
    value.createdAtUs = json.value("created_at_us", static_cast<uint64_t>(0));
    value.updatedAtUs = json.value("updated_at_us", static_cast<uint64_t>(0));
}

std::filesystem::path resolveWorkspaceStorePath()
{
    const char* store_path = std::getenv("NATKIT_WORKSPACE_STORE");
    if (store_path != nullptr && store_path[0] != '\0') {
        return std::filesystem::path(store_path);
    }
    // Same durable-volume caveat as the experiment store: the relative fallback
    // is the ephemeral container layer, so compose must point this at the volume
    // the other stores live on or a workspace vanishes on recreate -- taking
    // nothing with it, but leaving every member Unfiled.
    return std::filesystem::path("./data/workspaces.json");
}

std::unordered_map<std::string, Workspace> g_workspaces;
std::mutex g_workspace_mutex;
bool g_workspace_store_loaded = false;
std::string g_workspace_store_error{};

void ensureWorkspaceStoreLoadedLocked()
{
    if (g_workspace_store_loaded) {
        return;
    }
    g_workspace_store_loaded = true;
    g_workspace_store_error.clear();
    g_workspaces.clear();
    const auto store_path = resolveWorkspaceStorePath();
    if (!std::filesystem::exists(store_path)) {
        return;  // no workspaces yet: everything is Unfiled, which is valid
    }
    std::ifstream input(store_path);
    if (!input) {
        g_workspace_store_error =
            "Failed to open workspace store at " + store_path.string();
        return;
    }
    try {
        nlohmann::json json = nlohmann::json::parse(input);
        if (json.value("store_version", 0) != 1 || !json["workspaces"].is_array()) {
            throw std::runtime_error(
                "Workspace store must contain store_version=1 and a workspaces array");
        }
        for (const auto& workspace_json : json["workspaces"]) {
            Workspace workspace = workspace_json.get<Workspace>();
            g_workspaces[workspace.workspaceId] = std::move(workspace);
        }
    } catch (const std::exception& exception) {
        g_workspaces.clear();
        g_workspace_store_error =
            "Failed to parse workspace store: " + std::string(exception.what());
    }
}

bool persistWorkspaceStoreLocked(std::string& error)
{
    const auto store_path = resolveWorkspaceStorePath();
    if (store_path.has_parent_path()) {
        std::filesystem::create_directories(store_path.parent_path());
    }
    nlohmann::json store_json;
    store_json["store_version"] = 1;
    store_json["workspaces"] = nlohmann::json::array();
    for (const auto& entry : g_workspaces) {
        store_json["workspaces"].push_back(entry.second);
    }
    const auto temp_path = store_path.string() + ".tmp";
    {
        std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
        if (!output) {
            error = "Failed to open temporary workspace store file for write";
            return false;
        }
        output << store_json.dump(2);
        if (!output.good()) {
            error = "Failed while writing workspace store";
            return false;
        }
    }
    std::error_code rename_error;
    std::filesystem::rename(temp_path, store_path, rename_error);
    if (rename_error) {
        std::filesystem::remove(store_path, rename_error);
        rename_error.clear();
        std::filesystem::rename(temp_path, store_path, rename_error);
    }
    if (rename_error) {
        error =
            "Failed to atomically replace workspace store: " + rename_error.message();
        return false;
    }
    return true;
}

// Un-file every member of a workspace that is going away.
//
// ⚠️ Deleting a workspace must NOT delete its contents. It is a filing cabinet,
// and the affordance that empties it reads to an operator like tidying up -- so
// destroying a cohort's recorded history from it would be the worst possible
// reading of a UI gesture. Experiments, boards and profiles survive with an empty
// workspaceId, which puts them in Unfiled where they can be re-filed.
//
// Instances are re-filed too, unlike the experiment<->board binding which leaves
// them alone: an instance's workspace is a filing location, not the attribution
// its own record now carries (TEC-NATKIT-54), so moving it loses nothing.
//
// Called with g_workspace_mutex held. Takes the experiment, graph and profile
// mutexes in that order, establishing the lock order
// workspace -> experiment -> graph -> profile. Nothing acquires them the other
// way round: the experiment and graph handlers never touch the workspace store.
bool unfileWorkspaceMembersLocked(const std::string& workspace_id, std::string& error)
{
    size_t experiments_unfiled = 0;
    size_t graphs_unfiled = 0;
    size_t profiles_unfiled = 0;

    {
        std::lock_guard<std::mutex> lock(g_experiment_mutex);
        ensureExperimentStoreLoadedLocked();
        if (!g_experiment_store_error.empty()) {
            error = g_experiment_store_error;
            return false;
        }
        for (auto& entry : g_experiments) {
            if (entry.second.workspaceId == workspace_id) {
                entry.second.workspaceId.clear();
                entry.second.updatedAtUs = nowUs();
                ++experiments_unfiled;
            }
        }
        if (experiments_unfiled > 0 && !persistExperimentStoreLocked(error)) {
            return false;
        }
    }
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            error = g_stream_graph_store_error;
            return false;
        }
        for (auto& entry : g_stream_graphs) {
            if (entry.second.workspaceId == workspace_id) {
                entry.second.workspaceId.clear();
                entry.second.updatedAtUs = nowUs();
                ++graphs_unfiled;
            }
        }
        if (graphs_unfiled > 0 && !persistStreamGraphStoreLocked(error)) {
            return false;
        }
    }
    {
        std::lock_guard<std::mutex> lock(g_profile_mutex);
        ensureProfileStoreLoadedLocked();
        if (!g_profile_store_error.empty()) {
            error = g_profile_store_error;
            return false;
        }
        for (auto& entry : g_profiles) {
            if (entry.second.workspaceId == workspace_id) {
                entry.second.workspaceId.clear();
                entry.second.updatedAtUs = nowUs();
                ++profiles_unfiled;
            }
        }
        if (profiles_unfiled > 0 && !persistProfileStoreLocked(error)) {
            return false;
        }
    }

    LOG_INFO << "Un-filed workspace " << workspace_id << ": "
             << experiments_unfiled << " experiment(s), " << graphs_unfiled
             << " board(s), " << profiles_unfiled << " profile(s) moved to Unfiled";
    return true;
}

// --- Instance artifacts (experiment-history-snapshots-plan, Phase 3) --------
//
// An instance is only "permanent" once its data has LEFT Kafka. The broker here
// keeps 168h of retention and its log dir is a container volume, so a snapshot
// backed by topics silently becomes an empty snapshot. Materialization writes
// Parquet (plus the markers sidecar) into this store, checksums it, and marks it
// read-only.
//
// Be clear about what each of those buys: the read-only mode bits stop an
// ACCIDENT (a stray write, a re-run pointed at the same path). They are not a
// guarantee -- this backend runs as root in its container, and root ignores mode
// bits entirely. The sha256 is the part that actually makes corruption
// detectable, which is why it is recorded per artifact and why
// verify_experiment_instance exists to check it.
std::filesystem::path resolveInstanceStorePath()
{
    const char* store_path = std::getenv("NATKIT_INSTANCE_STORE");
    if (store_path != nullptr && store_path[0] != '\0') {
        return std::filesystem::path(store_path);
    }
    return std::filesystem::path("./data/instances");
}

// <store>/<experiment_id>/<instance_id>/ — one directory per instance, so an
// instance's artifacts can be listed, checksummed and removed as a unit.
std::filesystem::path instanceArtifactDir(
    const std::string& experimentId, const std::string& instanceId)
{
    return resolveInstanceStorePath() / experimentId / instanceId;
}

// SHA-256 of a file, lowercase hex. Recorded per artifact so truncation or
// tampering is detectable at review time rather than at training time.
std::string sha256OfFile(const std::filesystem::path& path, std::string& error)
{
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        error = "Failed to open for checksum: " + path.string();
        return {};
    }
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> context(
        EVP_MD_CTX_new(), &EVP_MD_CTX_free);
    if (context == nullptr ||
        EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        error = "Failed to initialize SHA-256";
        return {};
    }
    std::vector<char> buffer(1 << 16);
    while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto read = input.gcount();
        if (read > 0 &&
            EVP_DigestUpdate(context.get(), buffer.data(),
                             static_cast<size_t>(read)) != 1) {
            error = "Failed while hashing " + path.string();
            return {};
        }
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_length = 0;
    if (EVP_DigestFinal_ex(context.get(), digest, &digest_length) != 1) {
        error = "Failed to finalize SHA-256";
        return {};
    }
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < digest_length; ++index) {
        hex << std::setw(2) << static_cast<int>(digest[index]);
    }
    return hex.str();
}

// Make an artifact read-only. An accident guard, not a security boundary (see
// above): root bypasses it, so the checksum is the real integrity record.
void markArtifactReadOnly(const std::filesystem::path& path)
{
    std::error_code error;
    std::filesystem::permissions(
        path,
        std::filesystem::perms::owner_read | std::filesystem::perms::group_read |
            std::filesystem::perms::others_read,
        std::filesystem::perm_options::replace,
        error);
}

// The next recording instance id for an experiment: run-0001, run-0002, ...
// Forks then suffix these (run-0001-a) via nextForkInstanceIdLocked, so an id
// reads as its own lineage. Caller holds g_stream_graph_mutex.
std::string nextRecordingInstanceIdLocked(const std::string& experimentId)
{
    int highest = 0;
    for (const auto& entry : g_stream_graphs) {
        const auto& graph = entry.second;
        if (graph.experimentId != experimentId || graph.instanceId.empty()) {
            continue;
        }
        // Only plain run-NNNN ids count; a fork's run-0001-a must not bump the
        // recording counter.
        if (graph.instanceId.rfind("run-", 0) != 0) {
            continue;
        }
        const auto suffix = graph.instanceId.substr(4);
        if (suffix.empty() ||
            !std::all_of(suffix.begin(), suffix.end(),
                         [](unsigned char c) { return std::isdigit(c) != 0; })) {
            continue;
        }
        highest = std::max(highest, std::stoi(suffix));
    }
    std::ostringstream id;
    id << "run-" << std::setw(4) << std::setfill('0') << (highest + 1);
    return id.str();
}

}  // namespace (reopened below)

// --- Active replays (experiment-history-snapshots-plan, Phase 5) ------------
//
// A replay streams an instance's Parquet back onto a SCRATCH Kafka topic, so
// everything downstream (transform workers, viewers, combine, the marker overlay)
// works unchanged — from the graph's point of view it is just another live stream.
// The registry exists so a replay can be stopped and its scratch topics deleted:
// they are ephemeral by construction, and leaking one per replay would accrete
// topics on a broker whose storage we now care about.
struct ActiveReplay {
    std::string replayId;
    std::string graphId;
    natkit::tools::ReplayPlan plan;
    std::shared_ptr<std::atomic<bool>> cancelled;
    natkit::tools::ReplayProgress progress{};
    bool paced = true;
    double speed = 1.0;
};

std::unordered_map<std::string, ActiveReplay> g_active_replays;
std::mutex g_replay_mutex;

std::string sanitizeTopicIdentifier(const std::string& value)
{
    std::string out;
    out.reserve(value.size());
    for (const char character : value) {
        const bool allowed = (character >= 'A' && character <= 'Z') ||
                             (character >= 'a' && character <= 'z') ||
                             (character >= '0' && character <= '9') ||
                             character == '-' || character == '_';
        out.push_back(allowed ? character : '-');
    }
    if (out.empty() || !std::isalnum(static_cast<unsigned char>(out.front()))) {
        out.insert(out.begin(), 'r');
    }
    return out;
}

// One shape for a replay's state, used by the reply, the broadcasts and the list.
nlohmann::json makeReplayStateJson(const std::string& replayId,
                                   const std::string& graphId,
                                   const natkit::tools::ReplayPlan& plan,
                                   const natkit::tools::ReplayProgress& progress,
                                   const std::string& state)
{
    nlohmann::json bindings = nlohmann::json::array();
    for (const auto& binding : plan.bindings) {
        bindings.push_back({
            // The client maps "the source node that read stream X" to the scratch
            // topic, and start_stream_graph does the same server-side.
            {"original_stream_id", std::to_string(binding.originalStreamId)},
            {"replay_stream_id", std::to_string(binding.replayStreamId)},
            {"topic", binding.topic},
            {"frame_count", binding.frameCount},
            {"channel_labels", binding.channelLabels},
        });
    }
    nlohmann::json json;
    json["replay_id"] = replayId;
    json["graph_id"] = graphId;
    json["state"] = state;
    json["bindings"] = std::move(bindings);
    json["marker_stream_id"] = std::to_string(plan.markerStreamId);
    json["marker_count"] = plan.markerCount;
    json["total_frames"] = plan.totalFrames;
    json["first_ts_us"] = plan.firstTsUs;
    json["last_ts_us"] = plan.lastTsUs;
    json["frames_published"] = progress.framesPublished;
    json["markers_published"] = progress.markersPublished;
    json["last_published_ts_us"] = progress.lastTsUs;
    if (!progress.error.empty()) {
        json["error"] = progress.error;
    }
    return json;
}

// Artifact lookup for the download endpoint. Defined outside the anonymous
// namespace so NatKitBackend can call it; it reads the same store everything else
// does, under the same lock.
bool natkitLookupInstanceArtifact(const std::string& graphId,
                                  const std::string& artifactName,
                                  std::string& directoryOut,
                                  bool& listedOut)
{
    listedOut = false;
    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    ensureStreamGraphStoreLoadedLocked();
    const auto stored = g_stream_graphs.find(graphId);
    if (stored == g_stream_graphs.end() || stored->second.instanceId.empty()) {
        return false;
    }
    const auto artifacts =
        stored->second.recording.value("artifacts", nlohmann::json::object());
    directoryOut = artifacts.value(
        "directory",
        instanceArtifactDir(stored->second.experimentId,
                            stored->second.instanceId).string());
    for (const auto& data : artifacts.value("data", nlohmann::json::array())) {
        if (data.value("path", std::string{}) == artifactName) {
            listedOut = true;
            return true;
        }
    }
    if (artifacts.value("markers", std::string{}) == artifactName) {
        listedOut = true;
    }
    return true;
}

namespace {

StreamGraphValidationResult validateStreamGraphDefinition(
    const StreamGraphDefinition& graph,
    std::shared_ptr<nat::kafka::BrokerManager> broker_manager,
    // Stream ids that are known-good WITHOUT asking the broker. Replay binds a
    // source to a scratch topic the replay itself owns and is about to publish to;
    // the broker may not have observed that topic yet (auto-creation happens on
    // first produce, and metadata takes a moment to propagate), so asking it
    // "does this topic exist?" fails a graph that is perfectly well formed. The
    // replay plan is the authority for those ids, not the broker.
    const std::unordered_set<uint64_t>& assumed_source_streams = {})
{
    StreamGraphValidationResult result;
    if (graph.graphVersion != 1) {
        addTopLevelGraphDiagnostic(
            result,
            "unsupported_graph_version",
            "graph_version must equal 1.");
    }
    if (graph.graphId.empty()) {
        addTopLevelGraphDiagnostic(
            result,
            "missing_graph_id",
            "graph_id is required.");
    }
    if (graph.label.empty()) {
        addTopLevelGraphDiagnostic(
            result,
            "missing_label",
            "Graph label is required.");
    }

    std::unordered_set<std::string> node_ids;
    std::unordered_set<std::string> edge_ids;
    std::unordered_map<std::string, std::string> transform_node_by_output_identifier;
    std::unordered_map<std::string, const StreamGraphNode*> nodes_by_id;
    std::unordered_map<std::string, size_t> indegree;
    std::unordered_map<std::string, std::vector<std::string>> adjacency;
    std::unordered_map<std::string, std::optional<std::string>>
        node_output_schema_names;

    for (const auto& node : graph.nodes) {
        if (node.id.empty()) {
            addTopLevelGraphDiagnostic(
                result, "missing_node_id", "Every graph node must have an id.");
            continue;
        }
        if (!node_ids.insert(node.id).second) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "duplicate_node_id",
                "Node ids must be unique.");
        }
        nodes_by_id[node.id] = &node;
        indegree[node.id] = 0;

        if (node.kind != "stream_source" && node.kind != "transform" &&
            node.kind != "viewer" && node.kind != "sink" &&
            node.kind != "combine" && node.kind != "markers" &&
            node.kind != "experiment" && node.kind != "train" &&
            node.kind != "export") {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "unknown_node_kind",
                "Unsupported node kind '" + node.kind + "'.");
        }

        if (node.label.empty()) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "missing_node_label",
                "Node label is required.");
        }

        if (node.kind == "stream_source") {
            if (!node.streamId.has_value()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "missing_stream_id",
                    "stream_source nodes require stream_id.");
            }
            if ((node.outputPortIds.empty() ? 1U : node.outputPortIds.size()) != 1U) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_source_ports",
                    "stream_source nodes must expose exactly one output port in V1.");
            }
        } else if (node.kind == "transform") {
            if (!node.transformKind.has_value()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "missing_transform_kind",
                    "Transform nodes require transform_kind.");
            }
            if (!node.outputIdentifier.has_value() ||
                node.outputIdentifier->empty()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "missing_output_identifier",
                    "Transform nodes require output_identifier.");
            } else if (!isValidTopicIdentifier(node.outputIdentifier.value())) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_output_identifier",
                    "output_identifier must match ^[A-Za-z0-9][A-Za-z0-9_-]*$.");
            } else {
                const auto duplicate_search = transform_node_by_output_identifier.find(
                    node.outputIdentifier.value());
                if (duplicate_search != transform_node_by_output_identifier.end()) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "duplicate_output_identifier",
                        "Transform output_identifier values must be unique within a graph.");
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[duplicate_search->second],
                        "duplicate_output_identifier",
                        "Transform output_identifier values must be unique within a graph.");
                } else {
                    transform_node_by_output_identifier.emplace(
                        node.outputIdentifier.value(), node.id);
                }
            }
        } else if (node.kind == "viewer" || node.kind == "sink") {
            if ((node.inputPortIds.empty() ? 1U : node.inputPortIds.size()) != 1U) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_utility_ports",
                    node.kind + " nodes must expose exactly one input port in V1.");
            }
            if (!node.outputPortIds.empty()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_utility_output_ports",
                    node.kind + " nodes do not expose output ports in V1.");
            }
        } else if (node.kind == "combine") {
            if (!node.outputIdentifier.has_value() ||
                node.outputIdentifier->empty()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "missing_output_identifier",
                    "combine nodes require output_identifier.");
            } else if (!isValidTopicIdentifier(node.outputIdentifier.value())) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_output_identifier",
                    "output_identifier must match ^[A-Za-z0-9][A-Za-z0-9_-]*$.");
            }
            if (!node.outputPortIds.empty() && node.outputPortIds.size() != 1U) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_combine_output_ports",
                    "combine nodes must expose exactly one output port in V1.");
            }
        } else if (node.kind == "markers" || node.kind == "experiment") {
            // A markers node exposes exactly one output port, `markers` (the
            // MarkerEventV1 stream for the bound experiment). Any other
            // output-port shape is invalid.
            if (node.outputPortIds.size() != 1U ||
                node.outputPortIds.front() != "markers") {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_markers_output_ports",
                    "markers nodes expose exactly one output port, 'markers'.");
            }
            // The topic comes from the graph's bound experiment, not from the
            // node, so an unbound board has nothing for this node to republish.
            // A warning, not an error: the board is still startable and every
            // other branch runs -- binding an experiment is a header action, and
            // blocking the whole graph on it would be out of proportion.
            //
            // A legacy `experiment` node still resolves from its own config, so it
            // is only unbound when THAT is empty too -- warning about it otherwise
            // would flag a board that works.
            const bool node_carries_own_id =
                node.kind == "experiment" && node.config.is_object() &&
                isValidTopicIdentifier(
                    node.config.value("experiment_id", std::string{}));
            if (graph.experimentId.empty() && !node_carries_own_id) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "unbound_experiment",
                    "No experiment is bound to this board, so there is no marker "
                    "timeline to republish. Bind one from the board header.",
                    "warning");
            }
        } else if (node.kind == "export") {
            // An export node is terminal: its artifact is a file, so a data
            // output port is invalid (provenance stubs are fine). Input count is
            // checked in the descriptor pass below, where marker inputs can be
            // told apart from data inputs.
            const bool export_has_data_output = std::any_of(
                node.outputPortIds.begin(),
                node.outputPortIds.end(),
                [](const std::string& port) {
                    return !isProvenancePortId(port);
                });
            if (export_has_data_output) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_export_output_ports",
                    "export nodes do not expose data output ports.");
            }
        } else if (node.kind == "train") {
            // A train node produces no DATA stream, but may expose provenance
            // output stubs (prov_models: the models it trained, for a
            // train->classify lineage edge). Only a non-provenance output port
            // is invalid.
            const bool has_data_output = std::any_of(
                node.outputPortIds.begin(),
                node.outputPortIds.end(),
                [](const std::string& port) {
                    return !isProvenancePortId(port);
                });
            if (has_data_output) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_train_output_ports",
                    "train nodes do not expose data output ports.");
            }
        }
    }

    for (const auto& edge : graph.edges) {
        if (edge.id.empty()) {
            addTopLevelGraphDiagnostic(
                result, "missing_edge_id", "Every graph edge must have an id.");
            continue;
        }
        if (!edge_ids.insert(edge.id).second) {
            addGraphDiagnostic(
                result,
                result.edgeDiagnostics[edge.id],
                "duplicate_edge_id",
                "Edge ids must be unique.");
        }

        const auto source_search = nodes_by_id.find(edge.sourceNodeId);
        const auto target_search = nodes_by_id.find(edge.targetNodeId);
        if (source_search == nodes_by_id.end()) {
            addGraphDiagnostic(
                result,
                result.edgeDiagnostics[edge.id],
                "missing_source_node",
                "Edge source node does not exist.");
            continue;
        }
        if (target_search == nodes_by_id.end()) {
            addGraphDiagnostic(
                result,
                result.edgeDiagnostics[edge.id],
                "missing_target_node",
                "Edge target node does not exist.");
            continue;
        }

        const auto& source_node = *source_search->second;
        const auto& target_node = *target_search->second;
        const auto source_ports =
            source_node.outputPortIds.empty()
                ? std::vector<std::string>{"data"}
                : source_node.outputPortIds;
        const auto target_ports =
            target_node.inputPortIds.empty()
                ? std::vector<std::string>{"input"}
                : target_node.inputPortIds;

        if (std::find(source_ports.begin(), source_ports.end(), edge.sourcePort) ==
            source_ports.end()) {
            addGraphDiagnostic(
                result,
                result.edgeDiagnostics[edge.id],
                "missing_source_port",
                "Edge source port does not exist on its node.");
        }
        if (std::find(target_ports.begin(), target_ports.end(), edge.targetPort) ==
            target_ports.end()) {
            addGraphDiagnostic(
                result,
                result.edgeDiagnostics[edge.id],
                "missing_target_port",
                "Edge target port does not exist on its node.");
        }
        // Provenance edges carry lineage, not data — they never join the data
        // DAG, so they don't contribute to topo ordering or indegree. (Their
        // endpoints/ports are still validated above.)
        if (isProvenanceEdge(edge)) {
            continue;
        }
        adjacency[edge.sourceNodeId].push_back(edge.targetNodeId);
        indegree[edge.targetNodeId] += 1;
    }

    std::deque<std::string> ready;
    for (const auto& entry : indegree) {
        if (entry.second == 0) {
            ready.push_back(entry.first);
        }
    }

    std::vector<std::string> topo_order;
    while (!ready.empty()) {
        const std::string node_id = ready.front();
        ready.pop_front();
        topo_order.push_back(node_id);
        for (const auto& downstream_id : adjacency[node_id]) {
            auto search = indegree.find(downstream_id);
            if (search == indegree.end() || search->second == 0) {
                continue;
            }
            search->second -= 1;
            if (search->second == 0) {
                ready.push_back(downstream_id);
            }
        }
    }

    if (topo_order.size() != graph.nodes.size()) {
        addTopLevelGraphDiagnostic(
            result,
            "cycle_detected",
            "Graph contains a cycle; V1 graphs must be acyclic.");
    }

    std::unordered_map<
        std::string,
        std::shared_ptr<const nat::core::DataSchemaDescriptor>>
        resolved_output_descriptors;

    for (const auto& node_id : topo_order) {
        const auto node_search = nodes_by_id.find(node_id);
        if (node_search == nodes_by_id.end()) {
            continue;
        }
        const auto& node = *node_search->second;
        if (node.kind == "stream_source") {
            if (!broker_manager || !node.streamId.has_value()) {
                continue;
            }
            // A replay-bound source resolves from the replay, not the broker: its
            // topic carries the canonical channel frame by construction.
            if (assumed_source_streams.count(node.streamId.value()) > 0) {
                auto assumed_descriptor =
                    nat::core::DataSchemaDescriptorRegistry::getDefault()
                        .findBySchemaName(
                            nat::core::NatSignalFrameDataSchemaV1::name);
                if (assumed_descriptor.has_value()) {
                    resolved_output_descriptors[node.id] = assumed_descriptor.value();
                    node_output_schema_names[node.id] =
                        std::string(nat::core::NatSignalFrameDataSchemaV1::name);
                }
                continue;
            }
            const auto source_topic =
                findTransformSourceTopicForStream(broker_manager, node.streamId.value());
            if (source_topic == nullptr) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "missing_stream_topic",
                    "Could not locate a compatible DATA topic for this source stream.");
                continue;
            }
            auto descriptor_maybe =
                nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                    source_topic->schemaName);
            if (!descriptor_maybe.has_value()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "missing_stream_descriptor",
                    "No schema descriptor is available for the source stream.");
                continue;
            }
            resolved_output_descriptors[node.id] = descriptor_maybe.value();
            node_output_schema_names[node.id] = source_topic->schemaName;
            continue;
        }

        if (node.kind == "combine") {
            const auto input_count = std::count_if(
                graph.edges.begin(),
                graph.edges.end(),
                [&node](const StreamGraphEdge& edge) {
                    return edge.targetNodeId == node.id &&
                       !isProvenanceEdge(edge);
                });
            if (input_count < 2) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "too_few_inputs",
                    "combine nodes require at least two connected inputs.");
            }

            // Topic-aware combine (Part B): combine is a per-type merger, so each
            // input is classified as a data input (numeric channel frame — merged
            // by concat) or a marker input (an experiment's `markers` output —
            // interleaved). A marker input no longer errors; the output carries
            // one topic per type present across the inputs.
            bool all_inputs_resolved = true;
            bool has_data_input = false;
            bool has_marker_input = false;
            for (const auto& edge : graph.edges) {
                if (edge.targetNodeId != node.id || isProvenanceEdge(edge)) {
                    continue;
                }
                const auto src_search = nodes_by_id.find(edge.sourceNodeId);
                const bool source_is_marker =
                    src_search != nodes_by_id.end() &&
                    src_search->second != nullptr &&
                    isMarkerSourceKind(src_search->second->kind);
                if (source_is_marker) {
                    has_marker_input = true;
                    continue;
                }
                const auto descriptor_search =
                    resolved_output_descriptors.find(edge.sourceNodeId);
                if (descriptor_search != resolved_output_descriptors.end() &&
                    descriptor_search->second != nullptr &&
                    descriptorSupportsNumericChannelFrame(*descriptor_search->second)) {
                    has_data_input = true;
                    continue;
                }
                all_inputs_resolved = false;
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "unresolved_input_descriptor",
                    "Upstream descriptor could not be resolved for one of combine's inputs.");
            }

            // The combine output channel: a DATA descriptor when any data input is
            // present (so a downstream data consumer resolves it). A markers-only
            // combine carries only a marker topic (no numeric descriptor).
            if (all_inputs_resolved && has_data_input) {
                auto output_descriptor_maybe =
                    nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                        nat::core::NatSignalFrameDataSchemaV1::name);
                if (output_descriptor_maybe.has_value()) {
                    resolved_output_descriptors[node.id] = output_descriptor_maybe.value();
                    node_output_schema_names[node.id] =
                        std::string(nat::core::NatSignalFrameDataSchemaV1::name);
                }
            }
            (void)has_marker_input;
            continue;
        }

        if (node.kind != "transform") {
            if (node.kind == "viewer" || node.kind == "sink") {
                const auto input_count = std::count_if(
                    graph.edges.begin(),
                    graph.edges.end(),
                    [&node](const StreamGraphEdge& edge) {
                        return edge.targetNodeId == node.id &&
                       !isProvenanceEdge(edge);
                    });
                if (input_count == 0) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "missing_input",
                        node.kind + " node input is not connected.");
                } else if (input_count > 1) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "too_many_inputs",
                        node.kind + " nodes support exactly one input in V1.");
                }
            } else if (node.kind == "export") {
                // An export node is variadic and topic-aware: it needs at least
                // one connected input, and at least one of those must be a DATA
                // input (the exported rows). A markers-only export would have
                // nothing to write.
                bool export_has_data_input = false;
                std::size_t export_input_count = 0;
                for (const auto& edge : graph.edges) {
                    if (edge.targetNodeId != node.id || isProvenanceEdge(edge)) {
                        continue;
                    }
                    ++export_input_count;
                    const auto src_search = nodes_by_id.find(edge.sourceNodeId);
                    const bool source_is_marker =
                        src_search != nodes_by_id.end() &&
                        src_search->second != nullptr &&
                        isMarkerSourceKind(src_search->second->kind);
                    if (!source_is_marker) {
                        export_has_data_input = true;
                    }
                }
                if (export_input_count == 0) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "missing_input",
                        "export node input is not connected.");
                } else if (!export_has_data_input) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "missing_export_data_input",
                        "export nodes require at least one data input; wire a "
                        "stream in alongside the experiment markers.");
                }
            } else if (node.kind == "markers" || node.kind == "experiment") {
                // A markers node is source-like: it republishes the bound
                // experiment's marker timeline and takes no inputs. Any inbound
                // edge is invalid.
                const auto input_count = std::count_if(
                    graph.edges.begin(),
                    graph.edges.end(),
                    [&node](const StreamGraphEdge& edge) {
                        return edge.targetNodeId == node.id &&
                       !isProvenanceEdge(edge);
                    });
                if (input_count > 0) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "invalid_markers_input",
                        "markers nodes take no inputs (they are a source of the "
                        "experiment's marker timeline).");
                }
            }
            continue;
        }

        const auto input_count = std::count_if(
            graph.edges.begin(),
            graph.edges.end(),
            [&node](const StreamGraphEdge& edge) {
                return edge.targetNodeId == node.id &&
                       !isProvenanceEdge(edge);
            });
        if (input_count == 0) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "missing_input",
                "Transform node input is not connected.");
        } else if (input_count > 1) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "too_many_inputs",
                "Transform nodes support exactly one input in V1.");
        }

        if (!node.transformKind.has_value()) {
            continue;
        }
        const auto capability_maybe =
            findTransformCapabilityJsonByKind(node.transformKind.value());
        if (!capability_maybe.has_value()) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "unknown_transform_kind",
                "Transform kind '" + node.transformKind.value() +
                    "' is not supported by the backend.");
            continue;
        }

        validateTransformConfigAgainstCapability(
            capability_maybe.value(), node.config, result, node.id);

        const auto upstream_edge_search = std::find_if(
            graph.edges.begin(),
            graph.edges.end(),
            [&node](const StreamGraphEdge& edge) {
                return edge.targetNodeId == node.id &&
                       !isProvenanceEdge(edge);
            });
        if (upstream_edge_search == graph.edges.end()) {
            continue;
        }

        const auto descriptor_search =
            resolved_output_descriptors.find(upstream_edge_search->sourceNodeId);
        if (descriptor_search == resolved_output_descriptors.end() ||
            descriptor_search->second == nullptr) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "unresolved_input_descriptor",
                "Upstream descriptor could not be resolved for this transform input.");
            continue;
        }

        const auto requested_mapping_id = node.inputMappingId.value_or(
            std::string("canonical_channel_frame"));
        if (!descriptorSupportsRequestedInputMapping(
                *descriptor_search->second, requested_mapping_id)) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "incompatible_input_mapping",
                "input_mapping_id is not compatible with the upstream node output.");
        }

        const std::string output_schema_name =
            capability_maybe->value(
                "output_schema_name",
                std::string(nat::core::NatSignalFrameDataSchemaV1::name));
        auto output_descriptor_maybe =
            nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                output_schema_name);
        if (!output_descriptor_maybe.has_value()) {
            addGraphDiagnostic(
                result,
                result.nodeDiagnostics[node.id],
                "missing_output_descriptor",
                "No schema descriptor is available for the transform output.");
            continue;
        }

        resolved_output_descriptors[node.id] = output_descriptor_maybe.value();
        node_output_schema_names[node.id] = output_schema_name;
    }

    return result;
}

std::vector<std::string> topologicallySortStreamGraph(
    const StreamGraphDefinition& graph)
{
    std::unordered_map<std::string, size_t> indegree{};
    std::unordered_map<std::string, std::vector<std::string>> adjacency{};
    for (const auto& node : graph.nodes) {
        indegree[node.id] = 0;
    }
    for (const auto& edge : graph.edges) {
        adjacency[edge.sourceNodeId].push_back(edge.targetNodeId);
        indegree[edge.targetNodeId] += 1;
    }

    std::deque<std::string> ready{};
    for (const auto& entry : indegree) {
        if (entry.second == 0) {
            ready.push_back(entry.first);
        }
    }

    std::vector<std::string> order{};
    while (!ready.empty()) {
        const std::string node_id = ready.front();
        ready.pop_front();
        order.push_back(node_id);
        for (const auto& downstream_id : adjacency[node_id]) {
            auto search = indegree.find(downstream_id);
            if (search == indegree.end() || search->second == 0) {
                continue;
            }
            search->second -= 1;
            if (search->second == 0) {
                ready.push_back(downstream_id);
            }
        }
    }

    return order;
}

struct OnePoleLowPassFilter {
    bool initialized = false;
    double y = 0.0;

    double process(double x, double alpha)
    {
        if (!initialized) {
            y = x;
            initialized = true;
            return y;
        }
        y += alpha * (x - y);
        return y;
    }
};

struct OnePoleHighPassFilter {
    bool initialized = false;
    double prev_x = 0.0;
    double prev_y = 0.0;

    double process(double x, double alpha)
    {
        if (!initialized) {
            prev_x = x;
            prev_y = 0.0;
            initialized = true;
            return 0.0;
        }

        const double y = alpha * (prev_y + x - prev_x);
        prev_x = x;
        prev_y = y;
        return y;
    }
};

struct BiquadCoefficients {
    double b0 = 0.0;
    double b1 = 0.0;
    double b2 = 0.0;
    double a1 = 0.0;
    double a2 = 0.0;
};

struct BiquadFilterState {
    double x1 = 0.0;
    double x2 = 0.0;
    double y1 = 0.0;
    double y2 = 0.0;

    double process(double x0, const BiquadCoefficients& c)
    {
        const double y0 =
            c.b0 * x0 + c.b1 * x1 + c.b2 * x2 - c.a1 * y1 - c.a2 * y2;
        x2 = x1;
        x1 = x0;
        y2 = y1;
        y1 = y0;
        return y0;
    }
};

double normalizeButterworthCutoff(double cutoff_hz, double sample_rate_hz)
{
    if (cutoff_hz <= 0.0 || sample_rate_hz <= 0.0) {
        return 0.0;
    }
    const double nyquist = sample_rate_hz / 2.0;
    if (cutoff_hz >= nyquist) {
        return nyquist * 0.999;
    }
    return cutoff_hz;
}

BiquadCoefficients designHighPassBiquadCoefficients(
    double sample_rate_hz,
    double cutoff_hz,
    double q)
{
    const double bounded_cutoff =
        normalizeButterworthCutoff(cutoff_hz, sample_rate_hz);
    const double w0 = 2.0 * kPi * bounded_cutoff / sample_rate_hz;
    const double alpha = std::sin(w0) / (2.0 * q);
    const double cos_w0 = std::cos(w0);
    const double a0 = 1.0 + alpha;
    return {
        (1.0 + cos_w0) / (2.0 * a0),
        -(1.0 + cos_w0) / a0,
        (1.0 + cos_w0) / (2.0 * a0),
        (-2.0 * cos_w0) / a0,
        (1.0 - alpha) / a0};
}

BiquadCoefficients designLowPassBiquadCoefficients(
    double sample_rate_hz,
    double cutoff_hz,
    double q)
{
    const double bounded_cutoff =
        normalizeButterworthCutoff(cutoff_hz, sample_rate_hz);
    const double w0 = 2.0 * kPi * bounded_cutoff / sample_rate_hz;
    const double alpha = std::sin(w0) / (2.0 * q);
    const double cos_w0 = std::cos(w0);
    const double a0 = 1.0 + alpha;
    return {
        (1.0 - cos_w0) / (2.0 * a0),
        (1.0 - cos_w0) / a0,
        (1.0 - cos_w0) / (2.0 * a0),
        (-2.0 * cos_w0) / a0,
        (1.0 - alpha) / a0};
}

BiquadCoefficients designNotchBiquadCoefficients(
    double sample_rate_hz,
    double notch_hz,
    double q)
{
    const double bounded_cutoff =
        normalizeButterworthCutoff(notch_hz, sample_rate_hz);
    const double w0 = 2.0 * kPi * bounded_cutoff / sample_rate_hz;
    const double alpha = std::sin(w0) / (2.0 * q);
    const double cos_w0 = std::cos(w0);
    const double a0 = 1.0 + alpha;
    return {
        1.0 / a0,
        (-2.0 * cos_w0) / a0,
        1.0 / a0,
        (-2.0 * cos_w0) / a0,
        (1.0 - alpha) / a0};
}

std::vector<BiquadCoefficients> buildButterworthHighPassSections(
    double sample_rate_hz,
    double cutoff_hz,
    uint32_t order)
{
    std::vector<BiquadCoefficients> sections{};
    if (order < 2U) {
        return sections;
    }
    sections.reserve(order / 2U);
    const uint32_t section_count = order / 2U;
    for (uint32_t section_index = 0; section_index < section_count; ++section_index) {
        const double q = 1.0 /
            (2.0 *
             std::cos(kPi * (2.0 * static_cast<double>(section_index) + 1.0) /
                      (2.0 * static_cast<double>(order))));
        sections.push_back(
            designHighPassBiquadCoefficients(sample_rate_hz, cutoff_hz, q));
    }
    return sections;
}

std::vector<BiquadCoefficients> buildButterworthLowPassSections(
    double sample_rate_hz,
    double cutoff_hz,
    uint32_t order)
{
    std::vector<BiquadCoefficients> sections{};
    if (order < 2U) {
        return sections;
    }
    sections.reserve(order / 2U);
    const uint32_t section_count = order / 2U;
    for (uint32_t section_index = 0; section_index < section_count; ++section_index) {
        const double q = 1.0 /
            (2.0 *
             std::cos(kPi * (2.0 * static_cast<double>(section_index) + 1.0) /
                      (2.0 * static_cast<double>(order))));
        sections.push_back(
            designLowPassBiquadCoefficients(sample_rate_hz, cutoff_hz, q));
    }
    return sections;
}

double computeHighPassAlpha(double cutoff_hz, double sample_rate_hz);
double computeLowPassAlpha(double cutoff_hz, double sample_rate_hz);

struct HighPassIirFilterState {
    double configured_sample_rate_hz = 0.0;
    double configured_cutoff_hz = 0.0;
    uint32_t configured_order = 0;
    double configured_q = 0.0;
    std::string configured_method{};
    bool use_first_order = false;
    OnePoleHighPassFilter first_order{};
    std::vector<BiquadCoefficients> coefficients{};
    std::vector<BiquadFilterState> sections{};

    void configure(
        double cutoff_hz,
        const EmgTransformConfig& config,
        double sample_rate_hz)
    {
        if (configured_sample_rate_hz == sample_rate_hz &&
            configured_cutoff_hz == cutoff_hz &&
            configured_order == config.butterworth_order &&
            configured_q == config.biquad_q &&
            configured_method == config.iir_method) {
            return;
        }

        configured_sample_rate_hz = sample_rate_hz;
        configured_cutoff_hz = cutoff_hz;
        configured_order = config.butterworth_order;
        configured_q = config.biquad_q;
        configured_method = config.iir_method;

        use_first_order = false;
        first_order = {};
        coefficients.clear();
        sections.clear();

        if (config.iir_method == "biquad") {
            coefficients.push_back(designHighPassBiquadCoefficients(
                sample_rate_hz, cutoff_hz, config.biquad_q));
            sections.resize(1);
            return;
        }

        use_first_order = (config.butterworth_order % 2U) == 1U;
        coefficients = buildButterworthHighPassSections(
            sample_rate_hz, cutoff_hz, config.butterworth_order);
        sections.resize(coefficients.size());
    }

    double process(
        double input,
        double cutoff_hz,
        const EmgTransformConfig& config,
        double sample_rate_hz)
    {
        configure(cutoff_hz, config, sample_rate_hz);
        double output = input;
        if (use_first_order) {
            output = first_order.process(
                output, computeHighPassAlpha(cutoff_hz, sample_rate_hz));
        }
        for (size_t section_index = 0; section_index < sections.size(); ++section_index) {
            output = sections[section_index].process(
                output, coefficients[section_index]);
        }
        return output;
    }
};

struct LowPassIirFilterState {
    double configured_sample_rate_hz = 0.0;
    double configured_cutoff_hz = 0.0;
    uint32_t configured_order = 0;
    double configured_q = 0.0;
    std::string configured_method{};
    bool use_first_order = false;
    OnePoleLowPassFilter first_order{};
    std::vector<BiquadCoefficients> coefficients{};
    std::vector<BiquadFilterState> sections{};

    void configure(
        double cutoff_hz,
        const EmgTransformConfig& config,
        double sample_rate_hz)
    {
        if (configured_sample_rate_hz == sample_rate_hz &&
            configured_cutoff_hz == cutoff_hz &&
            configured_order == config.butterworth_order &&
            configured_q == config.biquad_q &&
            configured_method == config.iir_method) {
            return;
        }

        configured_sample_rate_hz = sample_rate_hz;
        configured_cutoff_hz = cutoff_hz;
        configured_order = config.butterworth_order;
        configured_q = config.biquad_q;
        configured_method = config.iir_method;

        use_first_order = false;
        first_order = {};
        coefficients.clear();
        sections.clear();

        if (config.iir_method == "biquad") {
            coefficients.push_back(designLowPassBiquadCoefficients(
                sample_rate_hz, cutoff_hz, config.biquad_q));
            sections.resize(1);
            return;
        }

        use_first_order = (config.butterworth_order % 2U) == 1U;
        coefficients = buildButterworthLowPassSections(
            sample_rate_hz, cutoff_hz, config.butterworth_order);
        sections.resize(coefficients.size());
    }

    double process(
        double input,
        double cutoff_hz,
        const EmgTransformConfig& config,
        double sample_rate_hz)
    {
        configure(cutoff_hz, config, sample_rate_hz);
        double output = input;
        if (use_first_order) {
            output = first_order.process(
                output, computeLowPassAlpha(cutoff_hz, sample_rate_hz));
        }
        for (size_t section_index = 0; section_index < sections.size(); ++section_index) {
            output = sections[section_index].process(
                output, coefficients[section_index]);
        }
        return output;
    }
};

struct BandPassIirFilterState {
    HighPassIirFilterState highpass{};
    LowPassIirFilterState lowpass{};

    double process(double input, const EmgTransformConfig& config, double sample_rate_hz)
    {
        const double highpassed =
            highpass.process(
                input, config.low_cutoff_hz, config, sample_rate_hz);
        return lowpass.process(
            highpassed, config.high_cutoff_hz, config, sample_rate_hz);
    }
};

struct NotchIirFilterState {
    double configured_sample_rate_hz = 0.0;
    double configured_notch_hz = 0.0;
    double configured_q = 0.0;
    uint32_t configured_harmonic_count = 0;
    std::vector<BiquadCoefficients> coefficients{};
    std::vector<BiquadFilterState> sections{};

    void configure(const EmgTransformConfig& config, double sample_rate_hz)
    {
        if (configured_sample_rate_hz == sample_rate_hz &&
            configured_notch_hz == config.notch_hz &&
            configured_q == config.notch_q &&
            configured_harmonic_count == config.harmonic_count) {
            return;
        }

        configured_sample_rate_hz = sample_rate_hz;
        configured_notch_hz = config.notch_hz;
        configured_q = config.notch_q;
        configured_harmonic_count = config.harmonic_count;
        coefficients.clear();
        sections.clear();

        if (sample_rate_hz <= 0.0 || config.notch_hz <= 0.0 || config.notch_q <= 0.0) {
            return;
        }

        const double nyquist = sample_rate_hz / 2.0;
        for (uint32_t harmonic_index = 0; harmonic_index <= config.harmonic_count;
             ++harmonic_index) {
            const double frequency =
                config.notch_hz * static_cast<double>(harmonic_index + 1U);
            if (frequency >= nyquist) {
                break;
            }
            coefficients.push_back(designNotchBiquadCoefficients(
                sample_rate_hz, frequency, config.notch_q));
        }
        sections.resize(coefficients.size());
    }

    double process(double input, const EmgTransformConfig& config, double sample_rate_hz)
    {
        configure(config, sample_rate_hz);
        double output = input;
        for (size_t section_index = 0; section_index < sections.size(); ++section_index) {
            output = sections[section_index].process(
                output, coefficients[section_index]);
        }
        return output;
    }
};

struct RmsWindowState {
    uint32_t configured_window_samples = 0;
    std::deque<double> window{};
    double sum_squares = 0.0;

    double process(double input, uint32_t window_samples)
    {
        if (configured_window_samples != window_samples) {
            configured_window_samples = window_samples;
            window.clear();
            sum_squares = 0.0;
        }

        if (window_samples == 0U) {
            return 0.0;
        }

        const double squared = input * input;
        window.push_back(squared);
        sum_squares += squared;
        while (window.size() > static_cast<size_t>(window_samples)) {
            sum_squares -= window.front();
            window.pop_front();
        }

        const double denom = static_cast<double>(window.size());
        if (denom <= 0.0) {
            return 0.0;
        }
        return std::sqrt(std::max(0.0, sum_squares / denom));
    }
};

struct SlidingWindowTransformState {
    uint32_t configured_window_samples = 0;
    uint32_t configured_step_samples = 0;
    std::vector<std::deque<float>> channel_windows{};
    std::vector<std::string> channel_labels{};
    uint64_t total_samples_seen = 0;
    uint64_t samples_since_last_emit = 0;
    uint64_t output_seq_no = 0;

    void reset(uint32_t window_samples, uint32_t step_samples, size_t channel_count)
    {
        configured_window_samples = window_samples;
        configured_step_samples = step_samples;
        channel_windows.assign(channel_count, {});
        total_samples_seen = 0;
        samples_since_last_emit = 0;
        output_seq_no = 0;
    }
};

struct EmgTransformChannelState {
    OnePoleHighPassFilter highpass;
    OnePoleLowPassFilter lowpass;
    OnePoleLowPassFilter envelope;
    HighPassIirFilterState iir;
    BandPassIirFilterState bandpass_iir;
    NotchIirFilterState notch_iir;
    RmsWindowState rms_window;
};

double computeLowPassAlpha(double cutoff_hz, double sample_rate_hz)
{
    if (cutoff_hz <= 0.0 || sample_rate_hz <= 0.0) {
        return 1.0;
    }

    const double dt = 1.0 / sample_rate_hz;
    const double rc = 1.0 / (2.0 * kPi * cutoff_hz);
    return dt / (rc + dt);
}

double computeHighPassAlpha(double cutoff_hz, double sample_rate_hz)
{
    if (cutoff_hz <= 0.0 || sample_rate_hz <= 0.0) {
        return 0.0;
    }

    const double dt = 1.0 / sample_rate_hz;
    const double rc = 1.0 / (2.0 * kPi * cutoff_hz);
    return rc / (rc + dt);
}

std::string buildDerivedChannelLabel(
    const std::string& label,
    const std::string& transform_kind,
    size_t channel_index)
{
    const std::string base = label.empty()
        ? "ch" + std::to_string(channel_index + 1)
        : label;
    if (transform_kind == "rectify") {
        return base + ".rect";
    }
    if (transform_kind == "lowpass_envelope") {
        return base + ".env";
    }
    if (transform_kind == "bandpass_iir") {
        return base + ".bp";
    }
    if (transform_kind == "notch_iir") {
        return base + ".notch";
    }
    if (transform_kind == "rms_window") {
        return base + ".rms";
    }
    if (transform_kind == "sliding_window") {
        return base + ".win";
    }
    if (transform_kind == "highpass_iir") {
        return base + ".hp";
    }
    if (transform_kind == "mav") {
        return base + ".mav";
    }
    if (transform_kind == "rms") {
        return base + ".rmsw";
    }
    if (transform_kind == "wl") {
        return base + ".wl";
    }
    if (transform_kind == "zc") {
        return base + ".zc";
    }
    if (transform_kind == "ssc") {
        return base + ".ssc";
    }
    if (transform_kind == "ar_coeffs") {
        return base + ".ar";
    }
    return base + ".env";
}

std::optional<EmgTransformConfig> parseEmgTransformConfig(const nlohmann::json& json)
{
    EmgTransformConfig config{};
    config.kind = json.value("transform_kind", std::string{});
    if (config.kind != "rectify" &&
        config.kind != "lowpass_envelope" &&
        config.kind != "bandpass_iir" &&
        config.kind != "notch_iir" &&
        config.kind != "rms_window" &&
        config.kind != "sliding_window" &&
        config.kind != "highpass_iir" &&
        config.kind != "mav" &&
        config.kind != "rms" &&
        config.kind != "wl" &&
        config.kind != "zc" &&
        config.kind != "ssc" &&
        config.kind != "ar_coeffs" &&
        config.kind != "lda_classify" &&
        config.kind != "emg_gesture_classify" &&
        config.kind != "channel_select") {
        return std::nullopt;
    }

    const auto parseNumber = [](const nlohmann::json& value) -> std::optional<double> {
        try {
            if (value.is_number()) {
                return value.get<double>();
            }
            if (value.is_string()) {
                const auto& text = value.get_ref<const std::string&>();
                if (text.empty()) {
                    return std::nullopt;
                }
                std::size_t parsed_chars = 0;
                const double parsed = std::stod(text, &parsed_chars);
                if (parsed_chars != text.size()) {
                    return std::nullopt;
                }
                return parsed;
            }
        } catch (const std::exception&) {
        }
        return std::nullopt;
    };
    const auto parseUnsigned = [&](const nlohmann::json& value) -> std::optional<uint32_t> {
        const auto parsed = parseNumber(value);
        if (!parsed.has_value() ||
            !std::isfinite(parsed.value()) ||
            parsed.value() < 0.0) {
            return std::nullopt;
        }
        const double rounded = std::round(parsed.value());
        if (std::fabs(parsed.value() - rounded) > 1e-6 ||
            rounded > static_cast<double>(std::numeric_limits<uint32_t>::max())) {
            return std::nullopt;
        }
        return static_cast<uint32_t>(rounded);
    };
    const auto parseString = [](const nlohmann::json& value) -> std::optional<std::string> {
        if (value.is_string()) {
            return value.get<std::string>();
        }
        return std::nullopt;
    };

    if (json.contains("config") && json["config"].is_object()) {
        const auto& values = json["config"];
        if (values.contains("cutoff_hz")) {
            const auto parsed = parseNumber(values["cutoff_hz"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.cutoff_hz = parsed.value();
        }
        if (values.contains("low_cutoff_hz")) {
            const auto parsed = parseNumber(values["low_cutoff_hz"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.low_cutoff_hz = parsed.value();
        }
        if (values.contains("high_cutoff_hz")) {
            const auto parsed = parseNumber(values["high_cutoff_hz"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.high_cutoff_hz = parsed.value();
        }
        if (values.contains("notch_hz")) {
            const auto parsed = parseNumber(values["notch_hz"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.notch_hz = parsed.value();
        }
        if (values.contains("notch_q")) {
            const auto parsed = parseNumber(values["notch_q"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.notch_q = parsed.value();
        }
        if (values.contains("iir_method")) {
            const auto parsed = parseString(values["iir_method"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.iir_method = parsed.value();
        }
        if (values.contains("butterworth_order")) {
            const auto parsed = parseUnsigned(values["butterworth_order"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.butterworth_order = parsed.value();
        }
        if (values.contains("biquad_q")) {
            const auto parsed = parseNumber(values["biquad_q"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.biquad_q = parsed.value();
        }
        if (values.contains("harmonic_count")) {
            const auto parsed = parseUnsigned(values["harmonic_count"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.harmonic_count = parsed.value();
        }
        if (values.contains("window_samples")) {
            const auto parsed = parseUnsigned(values["window_samples"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.window_samples = parsed.value();
        }
        if (values.contains("step_samples")) {
            const auto parsed = parseUnsigned(values["step_samples"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.step_samples = parsed.value();
        }
        if (values.contains("zc_threshold")) {
            const auto parsed = parseNumber(values["zc_threshold"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.zc_threshold = parsed.value();
        }
        if (values.contains("ssc_threshold")) {
            const auto parsed = parseNumber(values["ssc_threshold"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.ssc_threshold = parsed.value();
        }
        if (values.contains("ar_order")) {
            const auto parsed = parseUnsigned(values["ar_order"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.ar_order = parsed.value();
        }
        if (values.contains("model_path")) {
            const auto parsed = parseString(values["model_path"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.model_path = parsed.value();
        }
        if (values.contains("select_mode")) {
            const auto parsed = parseString(values["select_mode"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.select_mode = parsed.value();
        }
        if (values.contains("selection")) {
            const auto parsed = parseString(values["selection"]);
            if (!parsed.has_value()) {
                return std::nullopt;
            }
            config.selection = parsed.value();
        }
    }

    if (!std::isfinite(config.low_cutoff_hz) || config.low_cutoff_hz <= 0.0 ||
        !std::isfinite(config.high_cutoff_hz) || config.high_cutoff_hz <= 0.0 ||
        !std::isfinite(config.notch_hz) || config.notch_hz <= 0.0 ||
        !std::isfinite(config.notch_q) || config.notch_q <= 0.0) {
        if (config.kind == "bandpass_iir" || config.kind == "notch_iir") {
            return std::nullopt;
        }
    }
    if ((config.kind == "highpass_iir" || config.kind == "lowpass_envelope") &&
        (!std::isfinite(config.cutoff_hz) || config.cutoff_hz <= 0.0)) {
        return std::nullopt;
    }
    if ((config.kind == "highpass_iir" || config.kind == "bandpass_iir") &&
        (config.iir_method.empty() ||
         !std::isfinite(config.biquad_q) || config.biquad_q <= 0.0)) {
        return std::nullopt;
    }
    if (config.kind == "rms_window" && config.window_samples == 0U) {
        return std::nullopt;
    }
    if (config.kind == "sliding_window" &&
        (config.window_samples == 0U || config.step_samples == 0U)) {
        return std::nullopt;
    }
    if (config.kind == "zc" &&
        (!std::isfinite(config.zc_threshold) || config.zc_threshold < 0.0)) {
        return std::nullopt;
    }
    if (config.kind == "ssc" &&
        (!std::isfinite(config.ssc_threshold) || config.ssc_threshold < 0.0)) {
        return std::nullopt;
    }
    if (config.kind == "ar_coeffs" &&
        (config.ar_order == 0U || config.ar_order > 32U)) {
        return std::nullopt;
    }
    if (config.kind == "channel_select" &&
        config.select_mode != "label" && config.select_mode != "index" &&
        config.select_mode != "all") {
        return std::nullopt;
    }
    if (config.kind == "lda_classify") {
        if (config.model_path.empty()) {
            return std::nullopt;
        }
        std::error_code exists_error;
        if (!std::filesystem::exists(config.model_path, exists_error) || exists_error) {
            return std::nullopt;
        }
    }
    // emg_gesture_classify intentionally accepts an empty / not-yet-present
    // model_path: it starts idle (emits nothing) until a trained bundle exists.
    // A Quick-Start graph can then run (view the raw signal, record a session)
    // before training, and go live via reactive restart once the train job
    // auto-fills the bundle path — no "failed to start" on an untrained graph.
    if (config.kind == "bandpass_iir" &&
        config.low_cutoff_hz >= config.high_cutoff_hz) {
        return std::nullopt;
    }
    if (config.kind == "highpass_iir" || config.kind == "bandpass_iir") {
        if (config.iir_method != "butterworth" && config.iir_method != "biquad") {
            return std::nullopt;
        }
        if (config.iir_method == "butterworth") {
            if (config.butterworth_order == 0 || config.butterworth_order > 8U) {
                return std::nullopt;
            }
        }
    }

    return config;
}

std::shared_ptr<nat::core::BasicTopicInformation> findTransformSourceTopicForStream(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    uint64_t stream_id)
{
    if (!broker_manager) {
        return nullptr;
    }

    auto raw_streams = broker_manager->getAllStreams();
    for (auto& stream : raw_streams) {
        if (stream->getId() != stream_id) {
            continue;
        }

        auto data_topics = stream->getTopicsByType(nat::core::StreamType::DATA);
        std::shared_ptr<nat::core::BasicTopicInformation> best_topic = nullptr;
        int best_priority = -1;
        for (const auto& topic : data_topics) {
            // Accept JSON or Binary data topics as transform sources. (The IMU
            // stream is binary; the backend decodes it via the registered decoder
            // and the descriptor-compatibility check below still gates whether the
            // topic can actually feed a transform.)
            if (!topic ||
                (topic->serializationType != nat::core::SerializationType::Json &&
                 topic->serializationType != nat::core::SerializationType::Binary)) {
                continue;
            }
            auto descriptor_maybe =
                nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                    topic->schemaName);
            if (!descriptor_maybe.has_value() ||
                !(descriptorSupportsNumericChannelFrame(*descriptor_maybe.value()) ||
                  findCompatibleAlternateInputMapping(*descriptor_maybe.value())
                      .has_value())) {
                continue;
            }

            const int priority = getDataTopicPriority(*topic);
            if (best_topic == nullptr || priority > best_priority) {
                best_topic =
                    std::make_shared<nat::core::BasicTopicInformation>(*topic);
                best_priority = priority;
            }
        }
        return best_topic;
    }

    return nullptr;
}

struct LdaClassifierModel {
    std::vector<std::string> labels{};
    std::unordered_map<std::string, double> priors{};
    std::unordered_map<std::string, std::vector<double>> means{};
    std::vector<double> variances{};
};

// Loads the JSON format produced by natVR's LdaModel.save_json (see
// natVR/src/natvr/model.py): a diagonal-covariance Gaussian LDA model with
// per-label means/priors and a shared per-feature variance vector.
std::optional<LdaClassifierModel> loadLdaClassifierModel(const std::string& path)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        return std::nullopt;
    }

    nlohmann::json json;
    try {
        file >> json;
    } catch (const std::exception&) {
        return std::nullopt;
    }
    if (json.value("model_type", std::string{}) != "lda") {
        return std::nullopt;
    }

    LdaClassifierModel model;
    try {
        for (const auto& label : json.at("labels")) {
            model.labels.push_back(label.get<std::string>());
        }
        for (const auto& entry : json.at("priors").items()) {
            model.priors[entry.key()] = entry.value().get<double>();
        }
        for (const auto& entry : json.at("means").items()) {
            std::vector<double> mean_values{};
            for (const auto& value : entry.value()) {
                mean_values.push_back(value.get<double>());
            }
            model.means[entry.key()] = std::move(mean_values);
        }
        for (const auto& value : json.at("variances")) {
            model.variances.push_back(value.get<double>());
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }

    if (model.labels.empty() || model.variances.empty()) {
        return std::nullopt;
    }
    for (const auto& label : model.labels) {
        const auto means_search = model.means.find(label);
        if (means_search == model.means.end() ||
            means_search->second.size() != model.variances.size() ||
            model.priors.find(label) == model.priors.end()) {
            return std::nullopt;
        }
    }
    for (const auto variance : model.variances) {
        if (!(variance > 0.0)) {
            return std::nullopt;
        }
    }
    return model;
}

// --- Self-contained EMG gesture classifier (train/serve parity) ------------
//
// The emg_gesture_classify transform consumes RAW EMG frames and reproduces the
// natVR training feature pipeline end to end from a single self-describing
// bundle (natVR/src/natvr/model_bundle.py): sliding window -> DSP (60 Hz notch
// chain + 20 Hz high-pass, applied fresh per window) -> Hudgins features in the
// canonical order -> per-channel rest-calibration normalization -> diagonal LDA
// score. Because the same bundle drives windowing, DSP, feature order, and
// normalization, live features land in the exact space the model was fit in --
// parity holds by construction, unlike a hand-wired feature pipeline.

// RBJ biquad, ported 1:1 from natVR/src/natvr/dsp.py (design_notch /
// design_highpass + BiquadFilter). Direct-form I; each window is filtered from
// zero state, exactly as featurize.window_feature_vectors does per window.
struct RbjBiquad {
    double b0 = 1.0, b1 = 0.0, b2 = 0.0, a1 = 0.0, a2 = 0.0;
};

inline RbjBiquad designNotchBiquad(double sample_rate_hz, double frequency_hz, double q)
{
    const double w0 = 2.0 * kPi * frequency_hz / sample_rate_hz;
    const double alpha = std::sin(w0) / (2.0 * q);
    const double cos_w0 = std::cos(w0);
    const double a0 = 1.0 + alpha;
    return RbjBiquad{
        1.0 / a0,
        (-2.0 * cos_w0) / a0,
        1.0 / a0,
        (-2.0 * cos_w0) / a0,
        (1.0 - alpha) / a0};
}

inline RbjBiquad designHighpassBiquad(double sample_rate_hz, double cutoff_hz, double q)
{
    const double w0 = 2.0 * kPi * cutoff_hz / sample_rate_hz;
    const double alpha = std::sin(w0) / (2.0 * q);
    const double cos_w0 = std::cos(w0);
    const double a0 = 1.0 + alpha;
    return RbjBiquad{
        ((1.0 + cos_w0) / 2.0) / a0,
        (-(1.0 + cos_w0)) / a0,
        ((1.0 + cos_w0) / 2.0) / a0,
        (-2.0 * cos_w0) / a0,
        (1.0 - alpha) / a0};
}

inline std::vector<double> applyBiquadFresh(
    const RbjBiquad& c, const std::vector<double>& input)
{
    std::vector<double> output;
    output.reserve(input.size());
    double x1 = 0.0, x2 = 0.0, y1 = 0.0, y2 = 0.0;
    for (const double x0 : input) {
        const double y0 =
            c.b0 * x0 + c.b1 * x1 + c.b2 * x2 - c.a1 * y1 - c.a2 * y2;
        output.push_back(y0);
        x2 = x1;
        x1 = x0;
        y2 = y1;
        y1 = y0;
    }
    return output;
}

struct EmgGestureBundle {
    uint32_t sampleRateHz = 0;
    uint32_t windowSamples = 0;
    uint32_t hopSamples = 0;
    uint32_t channelCount = 0;
    double notchBaseHz = 60.0;
    uint32_t notchHarmonics = 3;
    double highpassHz = 20.0;  // <= 0 disables the high-pass stage
    bool rectify = false;
    double zcThreshold = 0.0;
    double sscThreshold = 0.0;
    std::vector<int> selectedChannelIndexes{};  // empty => all incoming channels
    std::vector<double> restMeanRms{};
    std::vector<double> scaleRms{};
    LdaClassifierModel lda{};
};

// Applies the training DSP chain to one window (fresh filter state per stage),
// matching natVR dsp.preprocess_channel with the training-time parameters.
inline std::vector<double> preprocessGestureWindow(
    const std::vector<double>& window, const EmgGestureBundle& bundle)
{
    std::vector<double> processed = window;
    // Notch chain over base * {1..harmonics}, skipping any >= Nyquist
    // (matches dsp.apply_notch_chain; q=30 is the dsp default).
    for (uint32_t harmonic = 1; harmonic <= bundle.notchHarmonics; ++harmonic) {
        const double frequency = bundle.notchBaseHz * static_cast<double>(harmonic);
        if (frequency >= static_cast<double>(bundle.sampleRateHz) / 2.0) {
            break;
        }
        processed = applyBiquadFresh(
            designNotchBiquad(bundle.sampleRateHz, frequency, 30.0), processed);
    }
    if (bundle.highpassHz > 0.0) {
        processed = applyBiquadFresh(
            designHighpassBiquad(bundle.sampleRateHz, bundle.highpassHz, std::sqrt(0.5)),
            processed);
    }
    if (bundle.rectify) {
        for (double& value : processed) {
            value = std::abs(value);
        }
    }
    return processed;
}

// Loads the bundle produced by natVR's model_bundle.write_model_bundle.
std::optional<EmgGestureBundle> loadEmgGestureBundle(const std::string& path)
{
    std::ifstream file(path);
    if (!file.is_open()) {
        return std::nullopt;
    }
    nlohmann::json json;
    try {
        file >> json;
    } catch (const std::exception&) {
        return std::nullopt;
    }

    EmgGestureBundle bundle;
    try {
        // feature_order must be the canonical Hudgins order this node hard-codes.
        static const std::vector<std::string> kExpectedOrder = {
            "mav", "rms", "zero_crossings", "slope_sign_changes", "waveform_length"};
        if (json.contains("feature_order")) {
            std::vector<std::string> order;
            for (const auto& value : json.at("feature_order")) {
                order.push_back(value.get<std::string>());
            }
            if (order != kExpectedOrder) {
                return std::nullopt;
            }
        }
        bundle.sampleRateHz = json.value("sample_rate_hz", 0U);
        bundle.windowSamples = json.value("window_samples", 0U);
        bundle.hopSamples = json.value("hop_samples", 0U);
        bundle.channelCount = json.value("channel_count", 0U);
        bundle.zcThreshold = json.value("zc_threshold", 0.0);
        bundle.sscThreshold = json.value("ssc_threshold", 0.0);
        if (json.contains("dsp")) {
            const auto& dsp = json.at("dsp");
            bundle.notchBaseHz = dsp.value("notch_base_hz", 60.0);
            bundle.notchHarmonics = dsp.value("notch_harmonics", 3U);
            bundle.rectify = dsp.value("rectify", false);
            if (dsp.contains("highpass_hz")) {
                bundle.highpassHz = dsp.at("highpass_hz").is_null()
                    ? 0.0
                    : dsp.at("highpass_hz").get<double>();
            }
        }
        if (json.contains("selected_channel_indexes")) {
            for (const auto& value : json.at("selected_channel_indexes")) {
                bundle.selectedChannelIndexes.push_back(value.get<int>());
            }
        }
        const auto& calibration = json.at("calibration");
        for (const auto& value : calibration.at("rest_mean_rms")) {
            bundle.restMeanRms.push_back(value.get<double>());
        }
        for (const auto& value : calibration.at("scale_rms")) {
            bundle.scaleRms.push_back(value.get<double>());
        }
        const auto& lda = json.at("lda");
        if (lda.value("model_type", std::string{}) != "lda") {
            return std::nullopt;
        }
        for (const auto& label : lda.at("labels")) {
            bundle.lda.labels.push_back(label.get<std::string>());
        }
        for (const auto& entry : lda.at("priors").items()) {
            bundle.lda.priors[entry.key()] = entry.value().get<double>();
        }
        for (const auto& entry : lda.at("means").items()) {
            std::vector<double> mean_values;
            for (const auto& value : entry.value()) {
                mean_values.push_back(value.get<double>());
            }
            bundle.lda.means[entry.key()] = std::move(mean_values);
        }
        for (const auto& value : lda.at("variances")) {
            bundle.lda.variances.push_back(value.get<double>());
        }
    } catch (const std::exception&) {
        return std::nullopt;
    }

    // Structural validation: the bundle must be internally consistent so the
    // live feature vector length matches the model by construction.
    if (bundle.sampleRateHz == 0U || bundle.windowSamples == 0U ||
        bundle.hopSamples == 0U || bundle.channelCount == 0U) {
        return std::nullopt;
    }
    if (bundle.restMeanRms.size() != bundle.channelCount ||
        bundle.scaleRms.size() != bundle.channelCount) {
        return std::nullopt;
    }
    if (bundle.lda.labels.empty() || bundle.lda.variances.empty()) {
        return std::nullopt;
    }
    if (bundle.lda.variances.size() != static_cast<size_t>(bundle.channelCount) * 5U) {
        return std::nullopt;
    }
    for (const auto& label : bundle.lda.labels) {
        const auto means_search = bundle.lda.means.find(label);
        if (means_search == bundle.lda.means.end() ||
            means_search->second.size() != bundle.lda.variances.size() ||
            bundle.lda.priors.find(label) == bundle.lda.priors.end()) {
            return std::nullopt;
        }
    }
    for (const auto variance : bundle.lda.variances) {
        if (!(variance > 0.0)) {
            return std::nullopt;
        }
    }
    if (!bundle.selectedChannelIndexes.empty() &&
        bundle.selectedChannelIndexes.size() != bundle.channelCount) {
        return std::nullopt;
    }
    return bundle;
}

// Rolling per-channel window state for the streaming classifier. Mirrors
// SlidingWindowTransformState: emits a window every hop_samples once the first
// window_samples have accumulated, so the live window set matches training's
// leading windows in steady state.
struct EmgGestureClassifyState {
    uint32_t configured_window_samples = 0;
    uint32_t configured_hop_samples = 0;
    std::vector<std::deque<double>> channel_windows{};
    uint64_t total_samples_seen = 0;
    uint64_t samples_since_last_emit = 0;
    uint64_t output_seq_no = 0;

    void reset(uint32_t window_samples, uint32_t hop_samples, size_t channel_count)
    {
        configured_window_samples = window_samples;
        configured_hop_samples = hop_samples;
        channel_windows.assign(channel_count, {});
        total_samples_seen = 0;
        samples_since_last_emit = 0;
        output_seq_no = 0;
    }
};

class TransformWorker {
public:
    TransformWorker(
        uint64_t source_stream_id,
        const std::string& output_identifier,
        const EmgTransformConfig& config,
        const std::string& requested_input_mapping_id,
        size_t slot_index,
        const std::shared_ptr<nat::core::BasicTopicInformation>& source_topic,
        const std::shared_ptr<nat::core::BasicTopicInformation>& output_topic,
        std::unique_ptr<nat::core::TopicMessenger>&& source_messenger,
        std::unique_ptr<nat::core::TopicMessenger>&& output_messenger)
        : sourceStreamId(source_stream_id),
          outputIdentifier(output_identifier),
          config(config),
          inputMappingId("canonical_channel_frame"),
          slotIndex(slot_index),
          sourceTopic(source_topic),
          outputTopic(output_topic),
          sourceMessenger(std::move(source_messenger)),
          outputMessenger(std::move(output_messenger))
    {
        const auto descriptor =
            nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                sourceTopic->schemaName);
        if (descriptor.has_value()) {
            descriptorMaybe = descriptor.value();
            if (requested_input_mapping_id == "canonical_channel_frame") {
                inputMappingId = "canonical_channel_frame";
            } else if (!requested_input_mapping_id.empty()) {
                alternateInputMapping = findRequestedAlternateInputMapping(
                    *descriptor.value(), requested_input_mapping_id);
                if (alternateInputMapping.has_value()) {
                    inputMappingId = alternateInputMapping->id;
                }
            } else if (!descriptorSupportsNumericChannelFrame(*descriptor.value())) {
                alternateInputMapping =
                    findCompatibleAlternateInputMapping(*descriptor.value());
                if (alternateInputMapping.has_value()) {
                    inputMappingId = alternateInputMapping->id;
                }
            }
        }
    }

    ~TransformWorker()
    {
        stop();
    }

    void start()
    {
        if (workerThread.joinable()) {
            return;
        }
        active = true;
        startedAtUs.store(nowUs());
        workerThread = std::thread(&TransformWorker::run, this);
    }

    void stop()
    {
        active = false;
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }

    uint64_t getOutputStreamId() const
    {
        return outputTopic ? outputTopic->id : 0;
    }

    std::string getOutputTopic() const
    {
        return outputTopic ? outputTopic->toTopicString() : std::string{};
    }

    std::shared_ptr<nat::core::BasicTopicInformation> getOutputTopicInfo() const
    {
        return outputTopic;
    }

    uint64_t getSourceStreamId() const
    {
        return sourceStreamId;
    }

    const std::string& getOutputIdentifier() const
    {
        return outputIdentifier;
    }

    const std::string& getTransformKind() const
    {
        return config.kind;
    }

    const std::string& getInputMappingId() const
    {
        return inputMappingId;
    }

    size_t getSlotIndex() const
    {
        return slotIndex;
    }

    std::string getThreadSlotId() const
    {
        return buildTransformSlotId(slotIndex);
    }

    uint64_t getStartedAtUs() const
    {
        return startedAtUs.load();
    }

    uint64_t getLastFrameAtUs() const
    {
        return lastFrameAtUs.load();
    }

    uint64_t getFramesProcessed() const
    {
        return framesProcessed.load();
    }

private:
    uint64_t sourceStreamId;
    std::string outputIdentifier;
    EmgTransformConfig config;
    size_t slotIndex;
    std::shared_ptr<nat::core::BasicTopicInformation> sourceTopic;
    std::shared_ptr<nat::core::BasicTopicInformation> outputTopic;
    std::unique_ptr<nat::core::TopicMessenger> sourceMessenger;
    std::unique_ptr<nat::core::TopicMessenger> outputMessenger;
    std::optional<std::shared_ptr<const nat::core::DataSchemaDescriptor>>
        descriptorMaybe{};
    std::optional<TransformInputMappingDefinition> alternateInputMapping{};
    std::string inputMappingId{"canonical_channel_frame"};
    std::vector<EmgTransformChannelState> channelStates;
    SlidingWindowTransformState slidingWindowState;
    std::optional<LdaClassifierModel> ldaModel{};
    bool ldaModelLoadAttempted = false;
    std::optional<EmgGestureBundle> gestureBundle{};
    bool gestureBundleLoadAttempted = false;
    EmgGestureClassifyState gestureClassifyState;
    std::atomic<bool> active{false};
    std::atomic<uint64_t> startedAtUs{0};
    std::atomic<uint64_t> lastFrameAtUs{0};
    std::atomic<uint64_t> framesProcessed{0};
    std::thread workerThread;

    void ensureChannelStates(size_t channel_count)
    {
        if (channelStates.size() < channel_count) {
            channelStates.resize(channel_count);
        }
    }

    double processValue(
        EmgTransformChannelState& state,
        double raw_value,
        double sample_rate_hz)
    {
        if (config.kind == "highpass_iir") {
            return state.iir.process(
                raw_value, config.cutoff_hz, config, sample_rate_hz);
        }
        if (config.kind == "rectify") {
            return std::abs(raw_value);
        }
        if (config.kind == "lowpass_envelope") {
            const double env_alpha =
                computeLowPassAlpha(config.cutoff_hz, sample_rate_hz);
            return state.envelope.process(raw_value, env_alpha);
        }
        if (config.kind == "bandpass_iir") {
            return state.bandpass_iir.process(raw_value, config, sample_rate_hz);
        }
        if (config.kind == "notch_iir") {
            return state.notch_iir.process(raw_value, config, sample_rate_hz);
        }
        if (config.kind == "rms_window") {
            return state.rms_window.process(raw_value, config.window_samples);
        }
        return raw_value;
    }

    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformFrames(
        const NormalizedNumericChannelFrame& record)
    {
        if (config.kind == "sliding_window") {
            return transformSlidingWindows(record);
        }
        if (config.kind == "mav" || config.kind == "rms" || config.kind == "wl" ||
            config.kind == "zc" || config.kind == "ssc") {
            return transformWindowReduction(record);
        }
        if (config.kind == "ar_coeffs") {
            return transformArCoefficients(record);
        }
        if (config.kind == "lda_classify") {
            return transformLdaClassify(record);
        }
        if (config.kind == "emg_gesture_classify") {
            return transformEmgGestureClassify(record);
        }
        if (config.kind == "channel_select") {
            return transformChannelSelect(record);
        }

        const size_t channel_count = record.channelLabels.size();
        const size_t samples_per_channel = record.samplesPerChannel;
        const double sample_rate_hz = static_cast<double>(record.sampleRateHz);
        const auto& source_samples = record.samples;
        ensureChannelStates(channel_count);

        std::vector<float> transformed_samples{};
        transformed_samples.reserve(channel_count * samples_per_channel);

        for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
            const size_t offset = channel_index * samples_per_channel;
            const size_t channel_samples =
                offset < source_samples.size()
                    ? std::min(samples_per_channel, source_samples.size() - offset)
                    : 0;

            for (size_t sample_index = 0; sample_index < channel_samples; ++sample_index) {
                const double raw_value =
                    static_cast<double>(source_samples[offset + sample_index]);
                transformed_samples.push_back(static_cast<float>(processValue(
                    channelStates[channel_index], raw_value, sample_rate_hz)));
            }
            for (size_t sample_index = channel_samples; sample_index < samples_per_channel;
                 ++sample_index) {
                transformed_samples.push_back(0.0f);
            }
        }

        std::vector<std::string> transformed_labels{};
        transformed_labels.reserve(channel_count);
        const auto& channel_labels = record.channelLabels;
        for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
            const std::string label =
                channel_index < channel_labels.size()
                    ? channel_labels[channel_index]
                    : std::string{};
            transformed_labels.push_back(buildDerivedChannelLabel(
                label, config.kind, channel_index));
        }

        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};
        output.emplace_back(
            record.deviceId,
            record.seqNo,
            record.deviceTsUs,
            record.sampleRateHz,
            transformed_labels,
            transformed_samples,
            static_cast<uint32_t>(samples_per_channel));
        return output;
    }

    // Structural (stateless) transform: emit a subset of the input frame's
    // channels unchanged. "all" keeps everything (identity); "label" keeps
    // channels whose label contains the selection substring (e.g. "mav" to pull
    // just the MAV features out of a concatenated feature vector); "index" keeps
    // an explicit comma-separated list of indices and a-b ranges (e.g. "0-8,12").
    // Labels are preserved so downstream nodes/viewers keep meaningful names.
    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformChannelSelect(
        const NormalizedNumericChannelFrame& record)
    {
        const size_t channel_count = record.channelLabels.size();
        const size_t samples_per_channel = record.samplesPerChannel;

        std::vector<size_t> selected{};
        if (config.select_mode == "index" && !config.selection.empty()) {
            std::stringstream token_stream(config.selection);
            std::string token;
            while (std::getline(token_stream, token, ',')) {
                const auto first = token.find_first_not_of(" \t");
                if (first == std::string::npos) {
                    continue;
                }
                const auto last = token.find_last_not_of(" \t");
                token = token.substr(first, last - first + 1);
                const auto dash = token.find('-');
                try {
                    if (dash != std::string::npos) {
                        const long lo = std::stol(token.substr(0, dash));
                        const long hi = std::stol(token.substr(dash + 1));
                        for (long i = std::max<long>(0, lo);
                             i <= hi && i < static_cast<long>(channel_count);
                             ++i) {
                            selected.push_back(static_cast<size_t>(i));
                        }
                    } else {
                        const long idx = std::stol(token);
                        if (idx >= 0 && idx < static_cast<long>(channel_count)) {
                            selected.push_back(static_cast<size_t>(idx));
                        }
                    }
                } catch (const std::exception&) {
                    // Skip malformed tokens rather than dropping the frame.
                }
            }
        } else if (config.select_mode == "label" && !config.selection.empty()) {
            for (size_t i = 0; i < channel_count; ++i) {
                if (record.channelLabels[i].find(config.selection) !=
                    std::string::npos) {
                    selected.push_back(i);
                }
            }
        } else {
            // "all", or an empty selection: pass every channel through.
            for (size_t i = 0; i < channel_count; ++i) {
                selected.push_back(i);
            }
        }

        std::vector<std::string> out_labels{};
        std::vector<float> out_samples{};
        out_labels.reserve(selected.size());
        out_samples.reserve(selected.size() * samples_per_channel);
        std::vector<bool> emitted(channel_count, false);
        for (const size_t channel_index : selected) {
            if (channel_index >= channel_count || emitted[channel_index]) {
                continue;
            }
            emitted[channel_index] = true;
            out_labels.push_back(record.channelLabels[channel_index]);
            const size_t offset = channel_index * samples_per_channel;
            for (size_t sample_index = 0; sample_index < samples_per_channel;
                 ++sample_index) {
                const size_t flat = offset + sample_index;
                out_samples.push_back(
                    flat < record.samples.size() ? record.samples[flat] : 0.0f);
            }
        }

        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};
        output.emplace_back(
            record.deviceId,
            record.seqNo,
            record.deviceTsUs,
            record.sampleRateHz,
            out_labels,
            out_samples,
            static_cast<uint32_t>(samples_per_channel));
        return output;
    }

    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformSlidingWindows(
        const NormalizedNumericChannelFrame& record)
    {
        const size_t channel_count = record.channelLabels.size();
        const size_t samples_per_channel = record.samplesPerChannel;
        const auto& source_samples = record.samples;
        const uint32_t window_samples = config.window_samples;
        const uint32_t step_samples = config.step_samples;
        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};

        if (window_samples == 0U || step_samples == 0U || channel_count == 0U) {
            return output;
        }

        if (slidingWindowState.configured_window_samples != window_samples ||
            slidingWindowState.configured_step_samples != step_samples ||
            slidingWindowState.channel_windows.size() != channel_count) {
            slidingWindowState.reset(window_samples, step_samples, channel_count);
        }

        if (slidingWindowState.channel_labels.size() != channel_count) {
            slidingWindowState.channel_labels.clear();
            slidingWindowState.channel_labels.reserve(channel_count);
            for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
                const std::string label =
                    channel_index < record.channelLabels.size()
                        ? record.channelLabels[channel_index]
                        : std::string{};
                slidingWindowState.channel_labels.push_back(buildDerivedChannelLabel(
                    label, config.kind, channel_index));
            }
        }

        const double sample_period_us = record.sampleRateHz > 0
            ? 1000000.0 / static_cast<double>(record.sampleRateHz)
            : 0.0;

        for (size_t sample_index = 0; sample_index < samples_per_channel; ++sample_index) {
            for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
                const size_t offset = channel_index * samples_per_channel + sample_index;
                const float value =
                    offset < source_samples.size() ? source_samples[offset] : 0.0f;
                auto& window = slidingWindowState.channel_windows[channel_index];
                window.push_back(value);
                while (window.size() > static_cast<size_t>(window_samples)) {
                    window.pop_front();
                }
            }

            ++slidingWindowState.total_samples_seen;
            ++slidingWindowState.samples_since_last_emit;
            if (slidingWindowState.total_samples_seen < static_cast<uint64_t>(window_samples) ||
                slidingWindowState.samples_since_last_emit < static_cast<uint64_t>(step_samples)) {
                continue;
            }

            slidingWindowState.samples_since_last_emit = 0;
            std::vector<float> window_samples_flat{};
            window_samples_flat.reserve(channel_count * static_cast<size_t>(window_samples));
            for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
                const auto& window = slidingWindowState.channel_windows[channel_index];
                if (window.size() != static_cast<size_t>(window_samples)) {
                    return output;
                }
                window_samples_flat.insert(
                    window_samples_flat.end(), window.begin(), window.end());
            }

            const uint64_t output_ts_us = sample_period_us > 0.0
                ? record.deviceTsUs + static_cast<uint64_t>(
                      std::llround(sample_period_us * static_cast<double>(sample_index)))
                : record.deviceTsUs;
            output.emplace_back(
                record.deviceId,
                slidingWindowState.output_seq_no++,
                output_ts_us,
                record.sampleRateHz,
                slidingWindowState.channel_labels,
                window_samples_flat,
                window_samples);
        }

        return output;
    }

    // Reduces one already-windowed channel frame (samplesPerChannel samples
    // per channel, typically the output of a sliding_window node) down to a
    // single scalar per channel: mav/wl/zc/ssc are all Hudgins-style
    // per-window features, ported from natVR/src/natvr/features.py.
    double reduceWindow(const std::vector<float>& samples, size_t offset, size_t count)
    {
        if (count == 0U) {
            return 0.0;
        }
        if (config.kind == "mav") {
            double sum = 0.0;
            for (size_t i = 0; i < count; ++i) {
                sum += std::abs(static_cast<double>(samples[offset + i]));
            }
            return sum / static_cast<double>(count);
        }
        if (config.kind == "rms") {
            double sum_squares = 0.0;
            for (size_t i = 0; i < count; ++i) {
                const double value = static_cast<double>(samples[offset + i]);
                sum_squares += value * value;
            }
            return std::sqrt(sum_squares / static_cast<double>(count));
        }
        if (config.kind == "wl") {
            double sum = 0.0;
            for (size_t i = 1; i < count; ++i) {
                sum += std::abs(
                    static_cast<double>(samples[offset + i]) -
                    static_cast<double>(samples[offset + i - 1]));
            }
            return sum;
        }
        if (config.kind == "zc") {
            uint32_t crossings = 0;
            for (size_t i = 1; i < count; ++i) {
                const double prev = static_cast<double>(samples[offset + i - 1]);
                const double curr = static_cast<double>(samples[offset + i]);
                const bool crosses =
                    (prev >= 0.0 && curr < 0.0) || (prev < 0.0 && curr >= 0.0);
                if (crosses && std::abs(prev - curr) >= config.zc_threshold) {
                    ++crossings;
                }
            }
            return static_cast<double>(crossings);
        }
        if (config.kind == "ssc") {
            uint32_t changes = 0;
            for (size_t i = 1; i + 1 < count; ++i) {
                const double prev = static_cast<double>(samples[offset + i - 1]);
                const double curr = static_cast<double>(samples[offset + i]);
                const double next = static_cast<double>(samples[offset + i + 1]);
                const double diff1 = curr - prev;
                const double diff2 = curr - next;
                if (diff1 * diff2 > 0.0 &&
                    std::max(std::abs(diff1), std::abs(diff2)) >= config.ssc_threshold) {
                    ++changes;
                }
            }
            return static_cast<double>(changes);
        }
        return 0.0;
    }

    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformWindowReduction(
        const NormalizedNumericChannelFrame& record)
    {
        const size_t channel_count = record.channelLabels.size();
        const size_t samples_per_channel = record.samplesPerChannel;
        const auto& source_samples = record.samples;
        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};

        if (channel_count == 0U || samples_per_channel == 0U) {
            return output;
        }

        std::vector<float> reduced_samples{};
        reduced_samples.reserve(channel_count);
        for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
            const size_t offset = channel_index * samples_per_channel;
            const size_t channel_samples =
                offset < source_samples.size()
                    ? std::min(samples_per_channel, source_samples.size() - offset)
                    : 0;
            reduced_samples.push_back(
                static_cast<float>(reduceWindow(source_samples, offset, channel_samples)));
        }

        std::vector<std::string> transformed_labels{};
        transformed_labels.reserve(channel_count);
        const auto& channel_labels = record.channelLabels;
        for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
            const std::string label =
                channel_index < channel_labels.size()
                    ? channel_labels[channel_index]
                    : std::string{};
            transformed_labels.push_back(buildDerivedChannelLabel(
                label, config.kind, channel_index));
        }

        output.emplace_back(
            record.deviceId,
            record.seqNo,
            record.deviceTsUs,
            record.sampleRateHz,
            transformed_labels,
            reduced_samples,
            static_cast<uint32_t>(1));
        return output;
    }

    // Levinson-Durbin recursion over one channel's window, returning `order`
    // AR coefficients (a[1..order] of the standard all-pole model). Returns
    // all-zero coefficients if the window is too short or degenerate.
    std::vector<double> computeArCoefficients(
        const std::vector<float>& samples, size_t offset, size_t count, uint32_t order)
    {
        std::vector<double> coeffs(order, 0.0);
        if (order == 0U || count <= static_cast<size_t>(order)) {
            return coeffs;
        }

        std::vector<double> autocorr(order + 1U, 0.0);
        for (uint32_t lag = 0; lag <= order; ++lag) {
            double sum = 0.0;
            for (size_t i = 0; i + lag < count; ++i) {
                sum += static_cast<double>(samples[offset + i]) *
                    static_cast<double>(samples[offset + i + lag]);
            }
            autocorr[lag] = sum;
        }
        if (autocorr[0] <= 0.0) {
            return coeffs;
        }

        std::vector<double> a(order + 1U, 0.0);
        a[0] = 1.0;
        double error = autocorr[0];
        for (uint32_t i = 1; i <= order; ++i) {
            double acc = autocorr[i];
            for (uint32_t j = 1; j < i; ++j) {
                acc += a[j] * autocorr[i - j];
            }
            const double reflection = error > 0.0 ? (-acc / error) : 0.0;

            std::vector<double> updated = a;
            updated[i] = reflection;
            for (uint32_t j = 1; j < i; ++j) {
                updated[j] = a[j] + reflection * a[i - j];
            }
            a = updated;

            error *= (1.0 - reflection * reflection);
            if (error <= 0.0) {
                error = 1e-12;
            }
        }

        for (uint32_t i = 0; i < order; ++i) {
            coeffs[i] = a[i + 1U];
        }
        return coeffs;
    }

    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformArCoefficients(
        const NormalizedNumericChannelFrame& record)
    {
        const size_t channel_count = record.channelLabels.size();
        const size_t samples_per_channel = record.samplesPerChannel;
        const auto& source_samples = record.samples;
        const uint32_t order = config.ar_order;
        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};

        if (channel_count == 0U || samples_per_channel == 0U || order == 0U) {
            return output;
        }

        std::vector<float> coeff_samples{};
        coeff_samples.reserve(channel_count * static_cast<size_t>(order));
        for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
            const size_t offset = channel_index * samples_per_channel;
            const size_t channel_samples =
                offset < source_samples.size()
                    ? std::min(samples_per_channel, source_samples.size() - offset)
                    : 0;
            const auto coeffs =
                computeArCoefficients(source_samples, offset, channel_samples, order);
            for (uint32_t coeff_index = 0; coeff_index < order; ++coeff_index) {
                coeff_samples.push_back(static_cast<float>(coeffs[coeff_index]));
            }
        }

        std::vector<std::string> transformed_labels{};
        transformed_labels.reserve(channel_count);
        const auto& channel_labels = record.channelLabels;
        for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
            const std::string label =
                channel_index < channel_labels.size()
                    ? channel_labels[channel_index]
                    : std::string{};
            transformed_labels.push_back(buildDerivedChannelLabel(
                label, config.kind, channel_index));
        }

        output.emplace_back(
            record.deviceId,
            record.seqNo,
            record.deviceTsUs,
            record.sampleRateHz,
            transformed_labels,
            coeff_samples,
            order);
        return output;
    }

    // Treats the entire incoming frame as one flat feature vector (one value
    // per channel; samplesPerChannel is expected to be 1, as produced by a
    // Combine node) and scores it against a diagonal-covariance Gaussian LDA
    // model, matching natVR/src/natvr/model.py's LdaModel.score_samples.
    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformLdaClassify(
        const NormalizedNumericChannelFrame& record)
    {
        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};

        if (!ldaModelLoadAttempted) {
            ldaModelLoadAttempted = true;
            ldaModel = loadLdaClassifierModel(config.model_path);
            if (!ldaModel.has_value()) {
                LOG_ERROR << "StreamViewer: lda_classify could not load model at "
                          << config.model_path;
            }
        }
        if (!ldaModel.has_value()) {
            return output;
        }

        std::vector<double> features{};
        features.reserve(record.channelLabels.size());
        for (size_t channel_index = 0; channel_index < record.channelLabels.size();
             ++channel_index) {
            const size_t offset = channel_index * record.samplesPerChannel;
            features.push_back(
                offset < record.samples.size()
                    ? static_cast<double>(record.samples[offset])
                    : 0.0);
        }
        if (features.size() != ldaModel->variances.size()) {
            return output;
        }

        std::vector<double> scores{};
        scores.reserve(ldaModel->labels.size());
        for (const auto& label : ldaModel->labels) {
            const auto& mean_vec = ldaModel->means.at(label);
            double score = std::log(std::max(ldaModel->priors.at(label), 1e-12));
            for (size_t feature_index = 0; feature_index < features.size(); ++feature_index) {
                const double variance = ldaModel->variances[feature_index];
                const double diff = features[feature_index] - mean_vec[feature_index];
                score -= 0.5 * diff * diff / variance;
            }
            scores.push_back(score);
        }

        size_t best_index = 0;
        for (size_t i = 1; i < scores.size(); ++i) {
            if (scores[i] > scores[best_index]) {
                best_index = i;
            }
        }
        const double max_score = scores[best_index];
        std::vector<double> exps(scores.size());
        double denom = 0.0;
        for (size_t i = 0; i < scores.size(); ++i) {
            exps[i] = std::exp(scores[i] - max_score);
            denom += exps[i];
        }
        if (denom <= 0.0) {
            denom = 1.0;
        }

        std::vector<std::string> labels{"predicted_class"};
        std::vector<float> samples{static_cast<float>(best_index)};
        for (size_t i = 0; i < ldaModel->labels.size(); ++i) {
            labels.push_back("confidence." + ldaModel->labels[i]);
            samples.push_back(static_cast<float>(exps[i] / denom));
        }

        output.emplace_back(
            record.deviceId,
            record.seqNo,
            record.deviceTsUs,
            record.sampleRateHz,
            labels,
            samples,
            static_cast<uint32_t>(1));
        return output;
    }

    // Self-contained gesture classifier: raw EMG frames in, predicted_class +
    // per-class confidences out, reproducing the training feature pipeline from
    // the bundle so live accuracy matches reported validation accuracy.
    std::vector<nat::core::NatSignalFrameDataSchemaV1> transformEmgGestureClassify(
        const NormalizedNumericChannelFrame& record)
    {
        std::vector<nat::core::NatSignalFrameDataSchemaV1> output{};

        if (!gestureBundleLoadAttempted) {
            gestureBundleLoadAttempted = true;
            // An empty path is the normal "not trained yet" state — stay idle
            // silently. Only a non-empty path that fails to load is an error.
            if (!config.model_path.empty()) {
                gestureBundle = loadEmgGestureBundle(config.model_path);
                if (!gestureBundle.has_value()) {
                    LOG_ERROR
                        << "StreamViewer: emg_gesture_classify could not load bundle at "
                        << config.model_path;
                }
            }
        }
        if (!gestureBundle.has_value()) {
            return output;  // idle until a valid bundle is present
        }
        const EmgGestureBundle& bundle = *gestureBundle;

        const size_t incoming_channels = record.channelLabels.size();
        const size_t samples_per_channel = record.samplesPerChannel;
        if (incoming_channels == 0U || samples_per_channel == 0U) {
            return output;
        }

        // Resolve which incoming channels feed the model (must match training's
        // channel selection). Empty selection => all incoming channels in order.
        std::vector<size_t> channel_indexes;
        if (bundle.selectedChannelIndexes.empty()) {
            for (size_t c = 0; c < incoming_channels; ++c) {
                channel_indexes.push_back(c);
            }
        } else {
            for (const int idx : bundle.selectedChannelIndexes) {
                if (idx < 0 || static_cast<size_t>(idx) >= incoming_channels) {
                    return output;
                }
                channel_indexes.push_back(static_cast<size_t>(idx));
            }
        }
        if (channel_indexes.size() != bundle.channelCount) {
            return output;
        }

        const uint32_t window_samples = bundle.windowSamples;
        const uint32_t hop_samples = bundle.hopSamples;
        if (gestureClassifyState.configured_window_samples != window_samples ||
            gestureClassifyState.configured_hop_samples != hop_samples ||
            gestureClassifyState.channel_windows.size() != channel_indexes.size()) {
            gestureClassifyState.reset(
                window_samples, hop_samples, channel_indexes.size());
        }

        const double sample_period_us = record.sampleRateHz > 0
            ? 1000000.0 / static_cast<double>(record.sampleRateHz)
            : 0.0;

        for (size_t sample_index = 0; sample_index < samples_per_channel; ++sample_index) {
            for (size_t c = 0; c < channel_indexes.size(); ++c) {
                const size_t offset =
                    channel_indexes[c] * samples_per_channel + sample_index;
                const double value = offset < record.samples.size()
                    ? static_cast<double>(record.samples[offset])
                    : 0.0;
                auto& window = gestureClassifyState.channel_windows[c];
                window.push_back(value);
                while (window.size() > static_cast<size_t>(window_samples)) {
                    window.pop_front();
                }
            }

            ++gestureClassifyState.total_samples_seen;
            ++gestureClassifyState.samples_since_last_emit;
            if (gestureClassifyState.total_samples_seen <
                    static_cast<uint64_t>(window_samples) ||
                gestureClassifyState.samples_since_last_emit <
                    static_cast<uint64_t>(hop_samples)) {
                continue;
            }
            gestureClassifyState.samples_since_last_emit = 0;

            // Build the flat, normalized feature vector in the canonical order:
            // [mav, rms, zc, ssc, wl] per channel, channel-major.
            std::vector<double> features;
            features.reserve(channel_indexes.size() * 5U);
            bool window_ready = true;
            for (size_t c = 0; c < channel_indexes.size(); ++c) {
                const auto& window = gestureClassifyState.channel_windows[c];
                if (window.size() != static_cast<size_t>(window_samples)) {
                    window_ready = false;
                    break;
                }
                const std::vector<double> raw(window.begin(), window.end());
                const std::vector<double> s = preprocessGestureWindow(raw, bundle);

                // Hudgins features (match natVR/src/natvr/features.py exactly).
                const size_t n = s.size();
                double mav = 0.0;
                double sum_squares = 0.0;
                double wl = 0.0;
                for (size_t i = 0; i < n; ++i) {
                    mav += std::abs(s[i]);
                    sum_squares += s[i] * s[i];
                }
                for (size_t i = 1; i < n; ++i) {
                    wl += std::abs(s[i] - s[i - 1]);
                }
                mav = n > 0 ? mav / static_cast<double>(n) : 0.0;
                const double rms =
                    n > 0 ? std::sqrt(sum_squares / static_cast<double>(n)) : 0.0;
                uint32_t zc = 0;
                for (size_t i = 1; i < n; ++i) {
                    const double prev = s[i - 1];
                    const double curr = s[i];
                    const bool crosses =
                        (prev >= 0.0 && curr < 0.0) || (prev < 0.0 && curr >= 0.0);
                    if (crosses && std::abs(prev - curr) >= bundle.zcThreshold) {
                        ++zc;
                    }
                }
                uint32_t ssc = 0;
                for (size_t i = 1; i + 1 < n; ++i) {
                    const double diff1 = s[i] - s[i - 1];
                    const double diff2 = s[i] - s[i + 1];
                    if (diff1 * diff2 > 0.0 &&
                        std::max(std::abs(diff1), std::abs(diff2)) >=
                            bundle.sscThreshold) {
                        ++ssc;
                    }
                }

                // Rest-calibration normalization (match calibration.normalize_feature_vector).
                const double scale = bundle.scaleRms[c];
                const double rest = bundle.restMeanRms[c];
                features.push_back(mav / scale);
                features.push_back((rms - rest) / scale);
                features.push_back(static_cast<double>(zc));
                features.push_back(static_cast<double>(ssc));
                features.push_back(wl / scale);
            }
            if (!window_ready || features.size() != bundle.lda.variances.size()) {
                continue;
            }

            // Diagonal-covariance Gaussian LDA scoring + softmax (match model.py).
            const auto& lda = bundle.lda;
            std::vector<double> scores;
            scores.reserve(lda.labels.size());
            for (const auto& label : lda.labels) {
                const auto& mean_vec = lda.means.at(label);
                double score = std::log(std::max(lda.priors.at(label), 1e-12));
                for (size_t f = 0; f < features.size(); ++f) {
                    const double diff = features[f] - mean_vec[f];
                    score -= 0.5 * diff * diff / lda.variances[f];
                }
                scores.push_back(score);
            }
            size_t best_index = 0;
            for (size_t i = 1; i < scores.size(); ++i) {
                if (scores[i] > scores[best_index]) {
                    best_index = i;
                }
            }
            const double max_score = scores[best_index];
            std::vector<double> exps(scores.size());
            double denom = 0.0;
            for (size_t i = 0; i < scores.size(); ++i) {
                exps[i] = std::exp(scores[i] - max_score);
                denom += exps[i];
            }
            if (denom <= 0.0) {
                denom = 1.0;
            }

            std::vector<std::string> labels{"predicted_class"};
            std::vector<float> out_samples{static_cast<float>(best_index)};
            for (size_t i = 0; i < lda.labels.size(); ++i) {
                labels.push_back("confidence." + lda.labels[i]);
                out_samples.push_back(static_cast<float>(exps[i] / denom));
            }

            const uint64_t output_ts_us = sample_period_us > 0.0
                ? record.deviceTsUs + static_cast<uint64_t>(std::llround(
                      sample_period_us * static_cast<double>(sample_index)))
                : record.deviceTsUs;
            output.emplace_back(
                record.deviceId,
                gestureClassifyState.output_seq_no++,
                output_ts_us,
                record.sampleRateHz,
                labels,
                out_samples,
                static_cast<uint32_t>(1));
        }
        return output;
    }

    void run()
    {
        LOG_INFO << "StreamViewer: Starting transform worker source="
                 << sourceStreamId << " output=" << getOutputTopic()
                 << " kind=" << config.kind;

        while (active.load()) {
            try {
                const auto message_maybe = sourceMessenger->tryGetNexMessage();
                if (!message_maybe.has_value()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    continue;
                }

                std::unique_ptr<nat::core::Schema> message =
                    std::move(message_maybe.value());
                if (!descriptorMaybe.has_value() || !descriptorMaybe.value()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    continue;
                }

                const auto normalized = tryNormalizeNumericChannelFrame(
                    *message,
                    *descriptorMaybe.value(),
                    alternateInputMapping,
                    sourceStreamId);
                if (!normalized.has_value()) {
                    continue;
                }

                const auto transformed_records = transformFrames(normalized.value());
                if (transformed_records.empty()) {
                    continue;
                }

                for (const auto& transformed_record : transformed_records) {
                    outputMessenger->sendMessage(transformed_record);
                }
                framesProcessed.fetch_add(transformed_records.size());
                lastFrameAtUs.store(nowUs());
            } catch (const std::exception& ex) {
                LOG_ERROR << "StreamViewer: transform worker error: "
                          << ex.what();
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
        }

        LOG_INFO << "StreamViewer: Stopped transform worker output="
                 << getOutputTopic();
    }
};

std::mutex g_transform_mutex;
std::unordered_map<uint64_t, std::shared_ptr<TransformWorker>>
    g_transform_workers;
const size_t g_transform_slot_capacity = resolveTransformSlotCapacity();

std::optional<LiveTransformWorkerSnapshot> getLiveTransformWorkerSnapshot(
    uint64_t output_stream_id)
{
    std::lock_guard<std::mutex> lock(g_transform_mutex);
    const auto search = g_transform_workers.find(output_stream_id);
    if (search == g_transform_workers.end() || !search->second) {
        return std::nullopt;
    }

    return LiveTransformWorkerSnapshot{
        search->second->getFramesProcessed(),
        search->second->getLastFrameAtUs(),
        "natkit-local-transform-worker",
        search->second->getThreadSlotId(),
    };
}

std::optional<size_t> findAvailableTransformSlotIndex()
{
    std::vector<bool> used_slots(g_transform_slot_capacity, false);
    for (const auto& entry : g_transform_workers) {
        const auto& worker = entry.second;
        if (!worker) {
            continue;
        }
        const size_t slot_index = worker->getSlotIndex();
        if (slot_index < used_slots.size()) {
            used_slots[slot_index] = true;
        }
    }
    for (size_t slot_index = 0; slot_index < used_slots.size(); ++slot_index) {
        if (!used_slots[slot_index]) {
            return slot_index;
        }
    }
    return std::nullopt;
}

// Resolves a graph-internal node's output DATA topic from the in-process worker
// registries. Defined after the combine registry below; forward-declared here so
// createTransformWorker() (which precedes that registry) can fall back to it.
std::shared_ptr<nat::core::BasicTopicInformation>
findGraphInternalOutputTopicForStream(uint64_t stream_id);

// Buffering policy for graph in-process channels. Graph edges carry real-time
// signal frames, so a full channel drops its oldest frame (matches the
// live-monitoring semantics a viewer expects) rather than blocking the producer.
// Capacity is per frame, not per sample, so rate-changing stages need no special
// case. Tunable via NATKIT_INPROCESS_CHANNEL_CAPACITY for overloaded pipelines.
nat::tools::InProcessChannelPolicy graphInProcessChannelPolicy()
{
    nat::tools::InProcessChannelPolicy policy;
    policy.overflow = nat::tools::InProcessOverflowPolicy::DropOldest;
    if (const char* capacity_env =
            std::getenv("NATKIT_INPROCESS_CHANNEL_CAPACITY")) {
        try {
            const long parsed = std::stol(capacity_env);
            if (parsed > 0) {
                policy.capacity = static_cast<size_t>(parsed);
            }
        } catch (const std::exception&) {
            // Ignore a malformed override; keep the default capacity.
        }
    }
    return policy;
}

// Mint an input (source) messenger for a graph worker: in-process when the
// upstream producer's output is a private, colocated edge, else Kafka. Both sides
// rendezvous on the deterministic channel id == topic id, so a channel can be
// swapped for Kafka without touching node code.
std::unique_ptr<nat::core::TopicMessenger> makeGraphSourceMessenger(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    const std::shared_ptr<nat::core::BasicTopicInformation>& source_topic,
    bool in_process,
    int64_t start_offset = -1)
{
    if (in_process) {
        // In-process channels are live (a re-run's chain produces into them);
        // they can't seek Kafka history, so the offset only applies to a Kafka
        // root source. (Phase 5.)
        return nat::tools::createInProcessMessenger(
            source_topic,
            broker_manager->getRegistry(),
            nat::tools::InProcessRole::Consumer,
            nat::tools::InProcessChannelRegistry::global(),
            graphInProcessChannelPolicy());
    }
    return broker_manager->createMessenger(source_topic, start_offset);
}

std::unique_ptr<nat::core::TopicMessenger> makeGraphOutputMessenger(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    const std::shared_ptr<nat::core::BasicTopicInformation>& output_topic,
    bool in_process)
{
    if (in_process) {
        return nat::tools::createInProcessMessenger(
            output_topic,
            broker_manager->getRegistry(),
            nat::tools::InProcessRole::Producer,
            nat::tools::InProcessChannelRegistry::global(),
            graphInProcessChannelPolicy());
    }
    return broker_manager->createMessenger(output_topic);
}

struct CreateTransformWorkerResult {
    bool ok = false;
    bool alreadyExists = false;
    std::string error{};
    uint64_t sourceStreamId = 0;
    uint64_t outputStreamId = 0;
    std::string outputIdentifier{};
    std::string transformKind{};
    std::string inputMappingId{};
    std::string topic{};
    std::string workerId{"natkit-local-transform-worker"};
    std::string threadSlotId{};
    size_t slotCapacity = 0;
    size_t activeCount = 0;
};

CreateTransformWorkerResult createTransformWorker(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    uint64_t source_stream_id,
    const std::string& output_identifier,
    const EmgTransformConfig& config,
    const std::string& requested_input_mapping_id,
    const std::optional<std::string>& graph_id = std::nullopt,
    const std::optional<std::string>& graph_run_id = std::nullopt,
    const std::optional<std::string>& graph_node_id = std::nullopt,
    bool input_in_process = false,
    bool output_in_process = false,
    int64_t source_start_offset = -1)
{
    CreateTransformWorkerResult result;
    result.sourceStreamId = source_stream_id;
    result.outputIdentifier = output_identifier;
    result.transformKind = config.kind;
    result.slotCapacity = g_transform_slot_capacity;

    if (!broker_manager) {
        result.error = "Broker manager not available";
        return result;
    }

    // Kafka discovery for hardware sources; deterministic in-process fallback for
    // an upstream graph node that exists but has not yet produced its first frame
    // (so its DATA topic is not in broker metadata). See resolveGraphSourceTopic.
    auto source_topic = nat::tools::resolveGraphSourceTopic(
        source_stream_id,
        [&](uint64_t id) {
            return findTransformSourceTopicForStream(broker_manager, id);
        },
        [](uint64_t id) { return findGraphInternalOutputTopicForStream(id); });
    if (source_topic == nullptr) {
        result.error =
            "Could not locate a compatible JSON numeric channel topic for source_stream_id";
        return result;
    }

    const auto output_topic = createTopicInfo(
        nat::core::StreamType::DATA,
        "transform",
        output_identifier,
        nat::core::NatSignalFrameDataSchemaV1::name);
    if (output_topic == nullptr) {
        result.error = "Failed to create Kafka topic information for transform";
        return result;
    }
    const auto meta_topic = createTopicInfo(
        nat::core::StreamType::META,
        "transform",
        output_identifier,
        nat::core::MetaRecord::name);
    if (meta_topic == nullptr) {
        result.error =
            "Failed to create Kafka topic information for transform metadata";
        return result;
    }

    auto descriptor_maybe =
        nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
            source_topic->schemaName);
    if (!descriptor_maybe.has_value()) {
        result.error = "No descriptor is available for the source transform input";
        return result;
    }

    if (!requested_input_mapping_id.empty()) {
        const bool canonical_requested =
            requested_input_mapping_id == "canonical_channel_frame";
        const bool canonical_supported =
            descriptorSupportsNumericChannelFrame(*descriptor_maybe.value());
        const bool alternate_supported =
            findRequestedAlternateInputMapping(
                *descriptor_maybe.value(), requested_input_mapping_id)
                .has_value();
        if (!(canonical_requested && canonical_supported) && !alternate_supported) {
            result.error =
                "input_mapping_id is not compatible with the selected source stream";
            return result;
        }
    }

    const uint64_t output_stream_id = output_topic->id;
    result.outputStreamId = output_stream_id;
    result.topic = output_topic->toTopicString();

    {
        std::lock_guard<std::mutex> lock(g_transform_mutex);
        result.activeCount = g_transform_workers.size();
        const auto search = g_transform_workers.find(output_stream_id);
        if (search != g_transform_workers.end()) {
            result.ok = true;
            result.alreadyExists = true;
            result.inputMappingId = search->second->getInputMappingId();
            result.threadSlotId = search->second->getThreadSlotId();
            return result;
        }
        if (result.activeCount >= g_transform_slot_capacity) {
            result.error =
                "Transform worker capacity reached; stop an existing transform or raise NATKIT_TRANSFORM_THREADS";
            return result;
        }
    }

    auto source_messenger = makeGraphSourceMessenger(
        broker_manager, source_topic, input_in_process, source_start_offset);
    auto output_messenger =
        makeGraphOutputMessenger(broker_manager, output_topic, output_in_process);
    // Provenance metadata is always published on Kafka so lineage stays
    // discoverable regardless of the data edge's transport.
    auto meta_messenger = broker_manager->createMessenger(meta_topic);
    std::shared_ptr<TransformWorker> worker;
    {
        std::lock_guard<std::mutex> lock(g_transform_mutex);
        const auto duplicate = g_transform_workers.find(output_stream_id);
        if (duplicate != g_transform_workers.end()) {
            result.ok = true;
            result.alreadyExists = true;
            result.activeCount = g_transform_workers.size();
            result.inputMappingId = duplicate->second->getInputMappingId();
            result.threadSlotId = duplicate->second->getThreadSlotId();
            return result;
        }

        const auto slot_index = findAvailableTransformSlotIndex();
        if (!slot_index.has_value()) {
            result.error =
                "Transform worker capacity reached; no slot is currently available";
            return result;
        }

        worker = std::make_shared<TransformWorker>(
            source_stream_id,
            output_identifier,
            config,
            requested_input_mapping_id,
            slot_index.value(),
            source_topic,
            output_topic,
            std::move(source_messenger),
            std::move(output_messenger));
        g_transform_workers.emplace(output_stream_id, worker);
        result.activeCount = g_transform_workers.size();
    }
    worker->start();

    nlohmann::json config_json = nlohmann::json::parse(serializeTransformConfigJson(config));
    if (graph_id.has_value()) {
        config_json["graph_id"] = graph_id.value();
    }
    if (graph_run_id.has_value()) {
        config_json["graph_run_id"] = graph_run_id.value();
    }
    if (graph_node_id.has_value()) {
        config_json["graph_node_id"] = graph_node_id.value();
    }

    const auto created_at_us = nowUs();
    const nat::core::TransformProvenanceRecord provenance_record(
        output_identifier,
        output_stream_id,
        nat::core::NatSignalFrameDataSchemaV1::name,
        output_topic->toTopicString(),
        source_stream_id,
        source_topic->schemaName,
        source_topic->toTopicString(),
        config.kind,
        worker->getInputMappingId(),
        config_json.dump(),
        created_at_us);
    meta_messenger->sendMessage(provenance_record);

    result.ok = true;
    result.inputMappingId = worker->getInputMappingId();
    result.threadSlotId = worker->getThreadSlotId();
    return result;
}

bool stopTransformWorkerByOutputStreamId(
    uint64_t output_stream_id,
    size_t& remaining_active_count)
{
    std::shared_ptr<TransformWorker> worker;
    {
        std::lock_guard<std::mutex> lock(g_transform_mutex);
        const auto search = g_transform_workers.find(output_stream_id);
        if (search == g_transform_workers.end()) {
            remaining_active_count = g_transform_workers.size();
            return false;
        }
        worker = search->second;
        g_transform_workers.erase(search);
        remaining_active_count = g_transform_workers.size();
    }

    if (worker) {
        worker->stop();
    }
    return true;
}

// A `combine` node fans in N ≥ 2 upstream streams into one. Each upstream is
// expected to already be frame-cadence-aligned with the others (e.g. several
// feature-extraction transforms all deriving from the same sliding_window),
// so alignment is a simple per-input FIFO: once every input has ≥1 queued
// frame, pop one from each and concatenate. This is not general time-sync —
// mismatched cadences will silently misalign.
struct CombineInputState {
    uint64_t sourceStreamId = 0;
    std::shared_ptr<nat::core::BasicTopicInformation> sourceTopic;
    std::unique_ptr<nat::core::TopicMessenger> sourceMessenger;
    std::optional<std::shared_ptr<const nat::core::DataSchemaDescriptor>>
        descriptorMaybe{};
    std::deque<NormalizedNumericChannelFrame> queue{};
};

// Topic-aware channels (Part B): a combine input can carry a MARKER topic. The
// marker lane merges/interleaves MarkerEventV1 records from all marker inputs
// into one Marker/<out> topic — no numeric transform, no descriptor needed.
struct CombineMarkerInputState {
    uint64_t sourceStreamId = 0;
    std::shared_ptr<nat::core::BasicTopicInformation> sourceTopic;
    std::unique_ptr<nat::core::TopicMessenger> sourceMessenger;
};

class CombineWorker {
public:
    CombineWorker(
        const std::string& output_identifier,
        size_t slot_index,
        std::vector<CombineInputState>&& inputs,
        std::vector<CombineMarkerInputState>&& marker_inputs,
        const std::shared_ptr<nat::core::BasicTopicInformation>& output_topic,
        std::unique_ptr<nat::core::TopicMessenger>&& output_messenger,
        const std::shared_ptr<nat::core::BasicTopicInformation>& marker_output_topic,
        std::unique_ptr<nat::core::TopicMessenger>&& marker_output_messenger)
        : outputIdentifier(output_identifier),
          slotIndex(slot_index),
          inputs(std::move(inputs)),
          markerInputs(std::move(marker_inputs)),
          outputTopic(output_topic),
          outputMessenger(std::move(output_messenger)),
          markerOutputTopic(marker_output_topic),
          markerOutputMessenger(std::move(marker_output_messenger))
    {
    }

    ~CombineWorker()
    {
        stop();
    }

    void start()
    {
        if (workerThread.joinable()) {
            return;
        }
        active = true;
        startedAtUs.store(nowUs());
        workerThread = std::thread(&CombineWorker::run, this);
    }

    void stop()
    {
        active = false;
        if (workerThread.joinable()) {
            workerThread.join();
        }
    }

    uint64_t getOutputStreamId() const
    {
        // Data and marker outputs share one channel id (stableStreamId is keyed
        // on namespace:identifier, not the topic type), so either topic yields it.
        if (outputTopic) return outputTopic->id;
        if (markerOutputTopic) return markerOutputTopic->id;
        return 0;
    }

    std::string getOutputTopic() const
    {
        if (outputTopic) return outputTopic->toTopicString();
        if (markerOutputTopic) return markerOutputTopic->toTopicString();
        return std::string{};
    }

    std::shared_ptr<nat::core::BasicTopicInformation> getOutputTopicInfo() const
    {
        return outputTopic;
    }

    // The MARKER topic of a "stream"/markers combine output (null for data-only),
    // used to resolve the in-memory marker topic before it materializes in Kafka.
    std::shared_ptr<nat::core::BasicTopicInformation> getMarkerOutputTopicInfo() const
    {
        return markerOutputTopic;
    }

    const std::string& getOutputIdentifier() const
    {
        return outputIdentifier;
    }

    size_t getSlotIndex() const
    {
        return slotIndex;
    }

    std::string getThreadSlotId() const
    {
        std::ostringstream stream;
        stream << "natkit-local-combine-worker:slot-";
        stream << std::setw(2) << std::setfill('0') << (slotIndex + 1);
        return stream.str();
    }

    uint64_t getStartedAtUs() const
    {
        return startedAtUs.load();
    }

    uint64_t getLastFrameAtUs() const
    {
        return lastFrameAtUs.load();
    }

    uint64_t getFramesProcessed() const
    {
        return framesProcessed.load();
    }

private:
    static constexpr size_t kMaxQueuedFramesPerInput = 64;
    // Two frames align if their device_ts_us differ by <= this (Phase 5). ~half
    // a typical windowed-feature cadence; tolerant enough for jittered live
    // frames, tight enough that replay pairs the right frames across streams.
    static constexpr uint64_t kAlignToleranceUs = 50'000;  // 50 ms

    std::string outputIdentifier;
    size_t slotIndex;
    std::vector<CombineInputState> inputs;
    std::vector<CombineMarkerInputState> markerInputs;
    std::shared_ptr<nat::core::BasicTopicInformation> outputTopic;
    std::unique_ptr<nat::core::TopicMessenger> outputMessenger;
    std::shared_ptr<nat::core::BasicTopicInformation> markerOutputTopic;
    std::unique_ptr<nat::core::TopicMessenger> markerOutputMessenger;
    uint64_t outputSeqNo = 0;
    std::atomic<bool> active{false};
    std::atomic<uint64_t> startedAtUs{0};
    std::atomic<uint64_t> lastFrameAtUs{0};
    std::atomic<uint64_t> framesProcessed{0};
    std::thread workerThread;

    // Flattens every input frame down to one scalar per (channel, sample)
    // pair and concatenates them all into a single samplesPerChannel==1
    // output frame — a flat feature vector regardless of how many samples
    // per channel each individual input carried (e.g. mixing mav's 1-per-
    // channel output with ar_coeffs' ar_order-per-channel output).
    nat::core::NatSignalFrameDataSchemaV1 concatenate(
        const std::vector<NormalizedNumericChannelFrame>& frames)
    {
        std::vector<std::string> labels{};
        std::vector<float> samples{};
        for (size_t input_index = 0; input_index < frames.size(); ++input_index) {
            const auto& frame = frames[input_index];
            const size_t channel_count = frame.channelLabels.size();
            const size_t samples_per_channel = frame.samplesPerChannel;
            for (size_t channel_index = 0; channel_index < channel_count; ++channel_index) {
                const std::string channel_label =
                    channel_index < frame.channelLabels.size()
                        ? frame.channelLabels[channel_index]
                        : ("ch" + std::to_string(channel_index + 1));
                const size_t offset = channel_index * samples_per_channel;
                for (size_t sample_index = 0; sample_index < samples_per_channel;
                     ++sample_index) {
                    const std::string label = samples_per_channel > 1
                        ? "in" + std::to_string(input_index + 1) + "." + channel_label +
                              "." + std::to_string(sample_index)
                        : "in" + std::to_string(input_index + 1) + "." + channel_label;
                    labels.push_back(label);
                    const float value = offset + sample_index < frame.samples.size()
                        ? frame.samples[offset + sample_index]
                        : 0.0f;
                    samples.push_back(value);
                }
            }
        }

        const auto& lead = frames.front();
        return nat::core::NatSignalFrameDataSchemaV1(
            lead.deviceId,
            outputSeqNo++,
            lead.deviceTsUs,
            lead.sampleRateHz,
            labels,
            samples,
            static_cast<uint32_t>(1));
    }

    // Pass a single data input through unchanged (Part B: data+markers → "stream"
    // is "no cross-transform"). Preserves the original channel labels AND
    // samples-per-channel, so a raw waveform stays a waveform instead of being
    // flattened into a one-sample-per-channel feature vector by concatenate().
    nat::core::NatSignalFrameDataSchemaV1 passThrough(
        const NormalizedNumericChannelFrame& frame)
    {
        return nat::core::NatSignalFrameDataSchemaV1(
            frame.deviceId,
            outputSeqNo++,
            frame.deviceTsUs,
            frame.sampleRateHz,
            frame.channelLabels,
            frame.samples,
            static_cast<uint32_t>(frame.samplesPerChannel));
    }

    void run()
    {
        LOG_INFO << "StreamViewer: Starting combine worker inputs=" << inputs.size()
                 << " output=" << getOutputTopic();

        while (active.load()) {
            try {
                bool made_progress = false;
                for (auto& input : inputs) {
                    const auto message_maybe = input.sourceMessenger->tryGetNexMessage();
                    if (!message_maybe.has_value()) {
                        continue;
                    }
                    made_progress = true;
                    std::unique_ptr<nat::core::Schema> message =
                        std::move(message_maybe.value());
                    if (!input.descriptorMaybe.has_value() || !input.descriptorMaybe.value()) {
                        continue;
                    }
                    const auto normalized = tryNormalizeNumericChannelFrame(
                        *message,
                        *input.descriptorMaybe.value(),
                        std::nullopt,
                        input.sourceStreamId);
                    if (!normalized.has_value()) {
                        continue;
                    }
                    input.queue.push_back(normalized.value());
                    while (input.queue.size() > kMaxQueuedFramesPerInput) {
                        input.queue.pop_front();
                    }
                }

                // Marker lane (Part B): forward every MarkerEventV1 from each
                // marker input straight to Marker/<out>. Markers are low-volume
                // and discrete; consumers order globally by emitted_at_us, so we
                // interleave by arrival (no cross-input alignment needed) and
                // never mix them with the numeric data lane.
                for (auto& marker_input : markerInputs) {
                    const auto marker_maybe =
                        marker_input.sourceMessenger->tryGetNexMessage();
                    if (!marker_maybe.has_value()) {
                        continue;
                    }
                    made_progress = true;
                    std::unique_ptr<nat::core::Schema> record =
                        std::move(marker_maybe.value());
                    if (record == nullptr || markerOutputMessenger == nullptr) {
                        continue;
                    }
                    if (dynamic_cast<nat::core::MarkerEventV1*>(record.get()) ==
                        nullptr) {
                        continue;  // not a marker event; skip defensively
                    }
                    markerOutputMessenger->sendMessage(*record);
                    framesProcessed.fetch_add(1);
                    lastFrameAtUs.store(nowUs());
                }

                bool all_ready = !inputs.empty();
                for (const auto& input : inputs) {
                    if (input.queue.empty()) {
                        all_ready = false;
                        break;
                    }
                }

                if (all_ready) {
                    // Timestamp-aligned combine (Phase 5, Part E): align inputs
                    // by device_ts_us, not arrival order. FIFO ("pop the front
                    // of each") silently misaligns mismatched cadences and is
                    // wrong for replay; here we emit one concatenated frame per
                    // aligned timestamp group. Each queue is time-ordered, so the
                    // front is the oldest unconsumed frame per input.
                    //
                    // target = the newest of the per-input fronts: every input
                    // must have reached at least this time to align here.
                    uint64_t target_ts = 0;
                    for (const auto& input : inputs) {
                        target_ts =
                            std::max(target_ts, input.queue.front().deviceTsUs);
                    }
                    // Drop unmatchably-old frames (a faster input's frames with
                    // no counterpart near target), keeping at least one.
                    for (auto& input : inputs) {
                        while (input.queue.size() > 1 &&
                               input.queue.front().deviceTsUs +
                                       kAlignToleranceUs <
                                   target_ts) {
                            input.queue.pop_front();
                        }
                    }
                    // Aligned only if every input's front is within tolerance of
                    // the target; otherwise wait for a lagging input to catch up.
                    bool aligned_ready = true;
                    for (const auto& input : inputs) {
                        const uint64_t ts = input.queue.front().deviceTsUs;
                        const uint64_t diff =
                            ts > target_ts ? ts - target_ts : target_ts - ts;
                        if (diff > kAlignToleranceUs) {
                            aligned_ready = false;
                            break;
                        }
                    }
                    if (aligned_ready) {
                        std::vector<NormalizedNumericChannelFrame> aligned{};
                        aligned.reserve(inputs.size());
                        for (auto& input : inputs) {
                            aligned.push_back(input.queue.front());
                            input.queue.pop_front();
                        }
                        // One data input (e.g. data+markers "stream"): pass it
                        // through so the waveform is preserved. Two or more: concat
                        // into a feature vector (the genuine numeric merge).
                        outputMessenger->sendMessage(
                            aligned.size() == 1 ? passThrough(aligned.front())
                                                : concatenate(aligned));
                        framesProcessed.fetch_add(1);
                        lastFrameAtUs.store(nowUs());
                        made_progress = true;
                    }
                }

                if (!made_progress) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                }
            } catch (const std::exception& ex) {
                LOG_ERROR << "StreamViewer: combine worker error: " << ex.what();
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
        }

        LOG_INFO << "StreamViewer: Stopped combine worker output=" << getOutputTopic();
    }
};

std::mutex g_combine_mutex;
std::unordered_map<uint64_t, std::shared_ptr<CombineWorker>> g_combine_workers;

// Forward-declared above createTransformWorker. When a graph node is started in
// the same topological pass as its upstream, the upstream worker exists here but
// its Kafka DATA topic has not yet appeared in broker metadata (no frame produced
// yet), so findTransformSourceTopicForStream() cannot discover it. The output
// topic name is deterministic and the worker holds it in memory, so resolve it
// directly from the registries. This keeps deep pipelines starting deterministically
// instead of racing the first frame through each stage.
std::shared_ptr<nat::core::BasicTopicInformation>
findGraphInternalOutputTopicForStream(uint64_t stream_id)
{
    {
        std::lock_guard<std::mutex> lock(g_transform_mutex);
        const auto search = g_transform_workers.find(stream_id);
        if (search != g_transform_workers.end() && search->second) {
            return search->second->getOutputTopicInfo();
        }
    }
    {
        std::lock_guard<std::mutex> lock(g_combine_mutex);
        const auto search = g_combine_workers.find(stream_id);
        if (search != g_combine_workers.end() && search->second) {
            return search->second->getOutputTopicInfo();
        }
    }
    return nullptr;
}

// The in-memory MARKER-topic counterpart of findGraphInternalOutputTopicForStream:
// a combine "stream"/markers output publishes to Marker/<id> before that topic
// appears in broker metadata. Resolve it from the worker so a subscriber / a
// downstream combine can bind it without racing the first forwarded marker.
std::shared_ptr<nat::core::BasicTopicInformation>
findGraphInternalMarkerTopicForStream(uint64_t stream_id)
{
    std::lock_guard<std::mutex> lock(g_combine_mutex);
    const auto search = g_combine_workers.find(stream_id);
    if (search != g_combine_workers.end() && search->second) {
        return search->second->getMarkerOutputTopicInfo();
    }
    return nullptr;
}

std::optional<LiveTransformWorkerSnapshot> getLiveCombineWorkerSnapshot(
    uint64_t output_stream_id)
{
    std::lock_guard<std::mutex> lock(g_combine_mutex);
    const auto search = g_combine_workers.find(output_stream_id);
    if (search == g_combine_workers.end() || !search->second) {
        return std::nullopt;
    }

    return LiveTransformWorkerSnapshot{
        search->second->getFramesProcessed(),
        search->second->getLastFrameAtUs(),
        "natkit-local-combine-worker",
        search->second->getThreadSlotId(),
    };
}

std::optional<LiveTransformWorkerSnapshot> getLiveGraphWorkerSnapshot(
    uint64_t output_stream_id)
{
    const auto transform_snapshot = getLiveTransformWorkerSnapshot(output_stream_id);
    if (transform_snapshot.has_value()) {
        return transform_snapshot;
    }
    return getLiveCombineWorkerSnapshot(output_stream_id);
}

struct CreateCombineWorkerResult {
    bool ok = false;
    bool alreadyExists = false;
    std::string error{};
    uint64_t outputStreamId = 0;
    std::string outputIdentifier{};
    std::string workerId{"natkit-local-combine-worker"};
    std::string threadSlotId{};
    // The merged output channel (Part B): a DATA topic and/or a MARKER topic,
    // both sharing outputStreamId. Populated for the caller's node status.
    std::vector<StreamGraphOutputTopic> outputTopics{};
};

// One resolved combine input = the upstream channel's topic set + its transport
// hints. The DATA topic (if any) feeds the numeric merge lane; the MARKER topic
// (if any) feeds the interleave lane.
struct CombineWorkerInput {
    std::vector<StreamGraphOutputTopic> topics{};
    bool inProcess = false;
    int64_t startOffset = -1;
};

CreateCombineWorkerResult createCombineWorker(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    const std::vector<CombineWorkerInput>& input_channels,
    const std::string& output_identifier,
    bool output_in_process = false)
{
    CreateCombineWorkerResult result;
    result.outputIdentifier = output_identifier;

    if (!broker_manager) {
        result.error = "Broker manager not available";
        return result;
    }
    if (input_channels.size() < 2U) {
        result.error = "combine nodes require at least two connected inputs";
        return result;
    }

    // Data and marker outputs share the channel id: stableStreamId is keyed on
    // namespace:identifier ("combine":output_identifier), not the topic type, so
    // Data/<out> and Marker/<out> collide on id by design — that IS the bundle.
    const auto data_output_topic = createTopicInfo(
        nat::core::StreamType::DATA, "combine", output_identifier,
        nat::core::NatSignalFrameDataSchemaV1::name);
    const auto marker_output_topic = createTopicInfo(
        nat::core::StreamType::MARKER, "combine", output_identifier,
        nat::core::MarkerEventV1::name);
    if (data_output_topic == nullptr || marker_output_topic == nullptr) {
        result.error = "Failed to create topic information for combine";
        return result;
    }
    const uint64_t output_stream_id = data_output_topic->id;
    result.outputStreamId = output_stream_id;

    // Reconstruct the output channel from an existing worker (dedup / reuse path).
    const auto outputTopicsForWorker =
        [&](const std::shared_ptr<CombineWorker>& worker) {
            std::vector<StreamGraphOutputTopic> topics{};
            if (worker->getOutputTopicInfo() != nullptr) {
                topics.push_back(makeChannelTopic(
                    nat::core::StreamType::DATA, output_stream_id,
                    nat::core::NatSignalFrameDataSchemaV1::name));
            }
            if (worker->getMarkerOutputTopicInfo() != nullptr) {
                topics.push_back(makeChannelTopic(
                    nat::core::StreamType::MARKER, output_stream_id,
                    nat::core::MarkerEventV1::name));
            }
            return topics;
        };

    {
        std::lock_guard<std::mutex> lock(g_combine_mutex);
        const auto search = g_combine_workers.find(output_stream_id);
        if (search != g_combine_workers.end()) {
            result.ok = true;
            result.alreadyExists = true;
            result.threadSlotId = search->second->getThreadSlotId();
            result.outputTopics = outputTopicsForWorker(search->second);
            return result;
        }
    }

    // Build a concrete topic from a resolved channel entry (type, id, schema).
    // Used as the last-resort fallback when neither broker discovery nor the
    // in-memory worker registry has materialised the topic yet (e.g. an
    // experiment's marker topic before the first publish) — the topic string is
    // deterministic, so a consumer can bind ahead of the first record.
    const auto buildTopicFromParts =
        [](const std::string& type_str, uint64_t id, const std::string& schema)
        -> std::shared_ptr<nat::core::BasicTopicInformation> {
        if (schema.empty()) return nullptr;
        const std::string topic =
            type_str + "-" + std::to_string(id) + "-Json-" + schema;
        auto maybe = nat::core::BasicTopicInformation::create(topic);
        if (!maybe.has_value()) return nullptr;
        std::unique_ptr<nat::core::BasicTopicInformation> owned =
            std::move(maybe.value());
        return std::shared_ptr<nat::core::BasicTopicInformation>(owned.release());
    };

    std::vector<CombineInputState> data_inputs{};
    std::vector<CombineMarkerInputState> marker_inputs{};
    for (const auto& channel : input_channels) {
        for (const auto& topic_ref : channel.topics) {
            if (topic_ref.type == nat::core::toString(nat::core::StreamType::MARKER)) {
                // Marker lane: resolve the MARKER topic (Kafka discovery →
                // in-memory combine worker → deterministic fallback). Markers
                // always flow over Kafka and read from the beginning.
                auto marker_topic = findMarkerOrMetaTopicForStreamId(
                    broker_manager, topic_ref.id);
                if (marker_topic == nullptr) {
                    marker_topic =
                        findGraphInternalMarkerTopicForStream(topic_ref.id);
                }
                if (marker_topic == nullptr) {
                    marker_topic = buildTopicFromParts(
                        topic_ref.type, topic_ref.id, topic_ref.schemaName);
                }
                if (marker_topic == nullptr) {
                    result.error =
                        "Could not locate the marker topic for one of combine's inputs";
                    return result;
                }
                CombineMarkerInputState marker_input{};
                marker_input.sourceStreamId = topic_ref.id;
                marker_input.sourceTopic = marker_topic;
                marker_input.sourceMessenger = makeGraphSourceMessenger(
                    broker_manager, marker_topic, false,
                    kMarkerConsumerStartOffset);
                marker_inputs.push_back(std::move(marker_input));
                continue;
            }
            if (topic_ref.type != nat::core::toString(nat::core::StreamType::DATA)) {
                continue;  // META (etc.) is carried later; ignore for now.
            }
            // Data lane: same intra-graph race as createTransformWorker — an
            // upstream node may exist but not yet have produced to Kafka.
            auto source_topic = nat::tools::resolveGraphSourceTopic(
                topic_ref.id,
                [&](uint64_t id) {
                    return findTransformSourceTopicForStream(broker_manager, id);
                },
                [](uint64_t id) {
                    return findGraphInternalOutputTopicForStream(id);
                });
            if (source_topic == nullptr) {
                source_topic = buildTopicFromParts(
                    topic_ref.type, topic_ref.id, topic_ref.schemaName);
            }
            if (source_topic == nullptr) {
                result.error =
                    "Could not locate a compatible JSON numeric channel topic for one of combine's source streams";
                return result;
            }
            auto descriptor_maybe =
                nat::core::DataSchemaDescriptorRegistry::getDefault()
                    .findBySchemaName(source_topic->schemaName);
            if (!descriptor_maybe.has_value()) {
                result.error =
                    "No descriptor is available for one of combine's source streams";
                return result;
            }
            CombineInputState input{};
            input.sourceStreamId = topic_ref.id;
            input.sourceTopic = source_topic;
            input.sourceMessenger = makeGraphSourceMessenger(
                broker_manager, source_topic, channel.inProcess,
                channel.startOffset);
            input.descriptorMaybe = descriptor_maybe.value();
            data_inputs.push_back(std::move(input));
        }
    }

    if (data_inputs.empty() && marker_inputs.empty()) {
        result.error = "combine inputs carried no data or marker topics";
        return result;
    }

    // Only mint an output messenger for a lane that has inputs (a data-only
    // combine has no marker output, and vice versa).
    std::shared_ptr<nat::core::BasicTopicInformation> out_data_topic{};
    std::unique_ptr<nat::core::TopicMessenger> out_data_messenger{};
    if (!data_inputs.empty()) {
        out_data_topic = data_output_topic;
        out_data_messenger = makeGraphOutputMessenger(
            broker_manager, data_output_topic, output_in_process);
        result.outputTopics.push_back(makeChannelTopic(
            nat::core::StreamType::DATA, output_stream_id,
            nat::core::NatSignalFrameDataSchemaV1::name));
    }
    std::shared_ptr<nat::core::BasicTopicInformation> out_marker_topic{};
    std::unique_ptr<nat::core::TopicMessenger> out_marker_messenger{};
    if (!marker_inputs.empty()) {
        out_marker_topic = marker_output_topic;
        // Markers always flow over Kafka (meta/marker are never in-process).
        out_marker_messenger =
            makeGraphOutputMessenger(broker_manager, marker_output_topic, false);
        result.outputTopics.push_back(makeChannelTopic(
            nat::core::StreamType::MARKER, output_stream_id,
            nat::core::MarkerEventV1::name));
    }

    std::shared_ptr<CombineWorker> worker;
    {
        std::lock_guard<std::mutex> lock(g_combine_mutex);
        const auto duplicate = g_combine_workers.find(output_stream_id);
        if (duplicate != g_combine_workers.end()) {
            result.ok = true;
            result.alreadyExists = true;
            result.threadSlotId = duplicate->second->getThreadSlotId();
            result.outputTopics = outputTopicsForWorker(duplicate->second);
            return result;
        }

        const size_t slot_index = g_combine_workers.size();
        worker = std::make_shared<CombineWorker>(
            output_identifier,
            slot_index,
            std::move(data_inputs),
            std::move(marker_inputs),
            out_data_topic,
            std::move(out_data_messenger),
            out_marker_topic,
            std::move(out_marker_messenger));
        g_combine_workers.emplace(output_stream_id, worker);
    }
    worker->start();

    result.ok = true;
    result.threadSlotId = worker->getThreadSlotId();
    return result;
}

bool stopCombineWorkerByOutputStreamId(uint64_t output_stream_id)
{
    std::shared_ptr<CombineWorker> worker;
    {
        std::lock_guard<std::mutex> lock(g_combine_mutex);
        const auto search = g_combine_workers.find(output_stream_id);
        if (search == g_combine_workers.end()) {
            return false;
        }
        worker = search->second;
        g_combine_workers.erase(search);
    }
    if (worker) {
        worker->stop();
    }
    return true;
}

// Tries both worker registries — a graph run's output_stream_id could belong
// to either a `transform` or a `combine` node.
bool stopGraphWorkerByOutputStreamId(uint64_t output_stream_id)
{
    size_t remaining_active_count = 0;
    if (stopTransformWorkerByOutputStreamId(output_stream_id, remaining_active_count)) {
        return true;
    }
    return stopCombineWorkerByOutputStreamId(output_stream_id);
}

} // namespace

std::optional<NatKitChannelFrameProjection> projectRecordToChannelFrame(
    const nat::core::Schema& record, uint64_t sourceStreamId)
{
    const auto descriptor_maybe =
        nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
            record.getName());
    if (!descriptor_maybe.has_value() || descriptor_maybe.value() == nullptr) {
        return std::nullopt;
    }
    const auto& descriptor = *descriptor_maybe.value();

    const auto toProjection = [](const NormalizedNumericChannelFrame& frame) {
        NatKitChannelFrameProjection projection;
        projection.deviceId = frame.deviceId;
        projection.seqNo = frame.seqNo;
        projection.deviceTsUs = frame.deviceTsUs;
        projection.sampleRateHz = frame.sampleRateHz;
        projection.channelLabels = frame.channelLabels;
        projection.samples = frame.samples;
        projection.samplesPerChannel = frame.samplesPerChannel;
        return projection;
    };

    // 1. The canonical contract (channels[].samples), if the schema publishes it.
    if (const auto canonical = tryNormalizeNumericChannelFrame(record, descriptor);
        canonical.has_value()) {
        return toProjection(canonical.value());
    }

    // 2. Otherwise an alternate input mapping — the same list that makes IMU and
    //    Muse filterable. Without this, export would reject exactly the sensors
    //    the transform path happily accepts.
    if (const auto mapping = findCompatibleAlternateInputMapping(descriptor);
        mapping.has_value()) {
        if (const auto mapped = tryNormalizeNumericChannelFrame(
                record, descriptor, mapping.value(), sourceStreamId);
            mapped.has_value()) {
            return toProjection(mapped.value());
        }
    }

    return std::nullopt;
}

void StreamViewerWebSocket::handleNewConnection(const HttpRequestPtr& req,
                                                 const WebSocketConnectionPtr& conn)
{
    const auto user = AuthManager::instance().authenticateRequest(req);
    if (!user.has_value()) {
        LOG_WARN << "StreamViewer WebSocket: Rejected unauthenticated connection from "
                 << conn->peerAddr().toIp();
        conn->send(R"({"type":"error","message":"Authentication required."})");
        conn->shutdown();
        return;
    }

    LOG_INFO << "StreamViewer WebSocket: New connection from " << conn->peerAddr().toIp()
             << " user=" << user->username;

    auto ctx = std::make_unique<StreamViewerClientContext>();
    ctx->user = user;
    
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_[conn] = std::move(ctx);
    }
    
    // Send initial status
    sendStatus(conn, clients_[conn].get());
}

void StreamViewerWebSocket::handleConnectionClosed(const WebSocketConnectionPtr& conn)
{
    LOG_INFO << "StreamViewer WebSocket: Connection closed";
    
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(conn);
    if (it != clients_.end()) {
        // Mark as inactive to stop streaming thread
        it->second->active = false;
        clients_.erase(it);
    }
}

void StreamViewerWebSocket::handleNewMessage(const WebSocketConnectionPtr& conn,
                                              std::string&& message,
                                              const WebSocketMessageType& type)
{
    // Ignore ping/pong and other control frames - they're handled by Drogon
    if (type == WebSocketMessageType::Ping || 
        type == WebSocketMessageType::Pong ||
        type == WebSocketMessageType::Close) {
        return;
    }
    
    if (type != WebSocketMessageType::Text) {
        sendError(conn, "Only text messages are supported");
        return;
    }

    try {
        auto json = nlohmann::json::parse(message);
        std::string action = json.value("action", "");

        StreamViewerClientContext* ctx = nullptr;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto it = clients_.find(conn);
            if (it != clients_.end()) {
                ctx = it->second.get();
            }
        }

        if (!ctx) {
            sendError(conn, "Client context not found");
            return;
        }

        if (action == "subscribe") {
            std::vector<uint64_t> stream_ids;
            if (json.contains("stream_ids") && json["stream_ids"].is_array()) {
                for (const auto& id : json["stream_ids"]) {
                    if (const auto parsed = parseStreamId(id); parsed.has_value()) {
                        stream_ids.push_back(parsed.value());
                    }
                }
            }
            // Optional historical start (Phase 3): -1 live tail (default),
            // -2 OFFSET_BEGINNING, >=0 a concrete offset.
            const int64_t start_offset =
                json.value("start_offset", static_cast<int64_t>(-1));
            handleSubscribe(conn, ctx, stream_ids, start_offset);
        }
        else if (action == "query_stream_time") {
            handleQueryStreamTime(conn, json);
        }
        else if (action == "unsubscribe") {
            std::vector<uint64_t> stream_ids;
            if (json.contains("stream_ids") && json["stream_ids"].is_array()) {
                for (const auto& id : json["stream_ids"]) {
                    if (const auto parsed = parseStreamId(id); parsed.has_value()) {
                        stream_ids.push_back(parsed.value());
                    }
                }
            }
            handleUnsubscribe(conn, ctx, stream_ids);
        }
        else if (action == "get_streams") {
            sendStreamList(conn);
        }
        else if (action == "publish_session_bundle") {
            handlePublishSessionBundle(conn, json);
        }
        else if (action == "list_transform_capabilities") {
            handleListTransformCapabilities(conn, json);
        }
        else if (action == "list_node_catalog") {
            handleListNodeCatalog(conn, json);
        }
        else if (action == "ml_proxy") {
            handleMlProxyAction(conn, json);
        }
        else if (action == "create_transform" || action == "create_emg_transform") {
            handleCreateTransform(conn, json);
        }
        else if (action == "list_transforms" || action == "list_emg_transforms") {
            handleListTransforms(conn, json);
        }
        else if (action == "stop_transform" || action == "stop_emg_transform") {
            handleStopTransform(conn, json);
        }
        else if (action == "list_stream_graphs") {
            handleListStreamGraphs(conn, json);
        }
        else if (action == "save_stream_graph") {
            handleSaveStreamGraph(conn, json);
        }
        else if (action == "fork_stream_graph") {
            handleForkStreamGraph(conn, json);
        }
        else if (action == "validate_stream_graph") {
            handleValidateStreamGraph(conn, json);
        }
        else if (action == "get_stream_graph_status") {
            handleGetStreamGraphStatus(conn, json);
        }
        else if (action == "start_stream_graph") {
            handleStartStreamGraph(conn, json);
        }
        else if (action == "stop_stream_graph") {
            handleStopStreamGraph(conn, json);
        }
        else if (action == "restart_stream_graph_node") {
            handleRestartStreamGraphNode(conn, json);
        }
        else if (action == "list_profiles") {
            handleListProfiles(conn, json);
        }
        else if (action == "save_profile") {
            handleSaveProfile(conn, json);
        }
        else if (action == "delete_profile") {
            handleDeleteProfile(conn, json);
        }
        else if (action == "list_workspaces") {
            handleListWorkspaces(conn, json);
        }
        else if (action == "save_workspace") {
            handleSaveWorkspace(conn, json);
        }
        else if (action == "delete_workspace") {
            handleDeleteWorkspace(conn, json);
        }
        else if (action == "list_experiments") {
            handleListExperiments(conn, json);
        }
        else if (action == "save_experiment") {
            handleSaveExperiment(conn, json);
        }
        else if (action == "delete_experiment") {
            handleDeleteExperiment(conn, json);
        }
        else if (action == "start_experiment_instance") {
            handleStartExperimentInstance(conn, json);
        }
        else if (action == "finish_experiment_instance") {
            handleFinishExperimentInstance(conn, json);
        }
        else if (action == "start_instance_replay") {
            handleStartInstanceReplay(conn, json);
        }
        else if (action == "stop_instance_replay") {
            handleStopInstanceReplay(conn, json);
        }
        else if (action == "list_instance_replays") {
            handleListInstanceReplays(conn, json);
        }
        else if (action == "verify_experiment_instance") {
            handleVerifyExperimentInstance(conn, json);
        }
        else if (action == "delete_stream_graph") {
            handleDeleteStreamGraph(conn, json);
        }
        else if (action == "list_log_streams") {
            handleListLogStreams(conn, json);
        }
        else if (action == "subscribe_logs") {
            handleSubscribeLogs(conn, json);
        }
        else if (action == "unsubscribe_logs") {
            handleUnsubscribeLogs(conn);
        }
        else if (action == "subscribe_device_health") {
            handleSubscribeDeviceHealth(conn, json);
        }
        else if (action == "unsubscribe_device_health") {
            handleUnsubscribeDeviceHealth(conn);
        }
        else if (action == "send_device_command") {
            handleSendDeviceCommand(conn, json);
        }
        else {
            sendError(conn, "Unknown action: " + action);
        }
    }
    catch (const std::exception& e) {
        sendError(conn, std::string("Failed to parse message: ") + e.what());
    }
}

void StreamViewerWebSocket::handleSubscribe(const WebSocketConnectionPtr& conn,
                                             StreamViewerClientContext* ctx,
                                             const std::vector<uint64_t>& stream_ids,
                                             int64_t start_offset)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    bool has_subscriptions = false;
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);

        // Get all available streams
        auto rawStreams = broker_manager_->getAllStreams();

        for (uint64_t stream_id : stream_ids) {
            if (ctx->subscribed_streams.count(stream_id) > 0) {
                continue; // Already subscribed
            }
            // Record the requested start offset so both this immediate bind and
            // the lazy-bind path start the consumer at the right point.
            ctx->requested_start_offsets[stream_id] = start_offset;

            // Find the stream and create a messenger for its DATA topic.
            std::shared_ptr<nat::core::BasicTopicInformation> dataTopic;
            for (const auto& stream : rawStreams) {
                if (stream->getId() == stream_id) {
                    auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
                    dataTopic = choosePreferredDataTopic(dataTopics);
                    break;
                }
            }
            // Fallback: a transform/combine output whose Kafka DATA topic has not
            // yet appeared in broker metadata (no frame produced yet). Resolve its
            // deterministic output topic from the in-process worker registry, the
            // same way createTransformWorker resolves a worker's input. Without
            // this, subscribing to a graph output raced the first frame and got
            // stuck on "Waiting for data" until the topic materialized.
            if (dataTopic == nullptr) {
                dataTopic = findGraphInternalOutputTopicForStream(stream_id);
            }
            if (dataTopic != nullptr) {
                ctx->messengers.push_back(
                    broker_manager_->createMessenger(dataTopic, start_offset));
                ctx->bound_data_ids.insert(stream_id);
            }
            // Topic-aware channels (Part A): a channel can bundle a DATA topic
            // and a MARKER topic under the SAME stream id (a combine "stream"
            // output = Data/<id> + Marker/<id>). Bind the marker/meta topic in
            // ADDITION to any data topic so both feeds flow to this client; for a
            // markers-only stream (e.g. an experiment output) this is the only
            // topic. Markers read from the beginning — low-volume, want the whole
            // cue timeline, and the topic only exists after the first publish.
            auto markerTopic =
                findMarkerOrMetaTopicForStreamId(broker_manager_, stream_id);
            if (markerTopic == nullptr) {
                markerTopic = findGraphInternalMarkerTopicForStream(stream_id);
            }
            if (markerTopic != nullptr) {
                const bool is_marker =
                    markerTopic->type == nat::core::StreamType::MARKER;
                const bool distinct =
                    dataTopic == nullptr ||
                    markerTopic->toTopicString() != dataTopic->toTopicString();
                // Bundle a MARKER alongside data (new), OR bind a lone marker/meta
                // stream when there's no data topic (unchanged behavior). A data
                // stream's META topic is NOT newly bound — that would stream meta
                // records to viewers that never saw them before.
                if (distinct && (dataTopic == nullptr || is_marker)) {
                    ctx->messengers.push_back(broker_manager_->createMessenger(
                        markerTopic, kMarkerConsumerStartOffset));
                    ctx->bound_marker_ids.insert(stream_id);
                }
            }

            // Record the subscription intent regardless of whether the stream
            // was discoverable just now. A stream produced by a graph that was
            // only just started (a transform/combine output) frequently isn't
            // in getAllStreams() yet at subscribe time; the streaming thread
            // resolves such pending subscriptions lazily. Previously the intent
            // was only recorded inside the "found" branch, so a not-yet-visible
            // stream was dropped silently and never delivered any data.
            ctx->subscribed_streams.insert(stream_id);
            LOG_INFO << "StreamViewer: Subscribed to stream " << stream_id;
        }

        has_subscriptions = !ctx->subscribed_streams.empty();
    } // Release lock before touching the thread / calling sendStatus

    // Manage the streaming thread OUTSIDE ctx->mutex (the loop body holds that
    // lock every iteration; joining under it would deadlock). Per-connection
    // handlers are serialized, so streaming_thread has no other writer here.
    // Reap a thread a prior unsubscribe stopped (still joinable but active=false)
    // before starting a fresh one — assigning onto a joinable std::thread calls
    // std::terminate.
    if (has_subscriptions) {
        if (ctx->streaming_thread.joinable() && !ctx->active.load()) {
            ctx->streaming_thread.join();
        }
        if (!ctx->streaming_thread.joinable()) {
            ctx->active = true;
            ctx->streaming_thread = std::thread(
                &StreamViewerWebSocket::streamingThreadFunc, this, conn, ctx);
        }
    }

    sendStatus(conn, ctx);
}

void StreamViewerWebSocket::handleQueryStreamTime(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }
    const std::string request_id = json.value("request_id", std::string{});
    const auto stream_id_maybe =
        json.contains("stream_id") ? parseStreamId(json["stream_id"])
                                    : std::nullopt;
    if (!stream_id_maybe.has_value()) {
        sendError(conn, "query_stream_time requires a valid stream_id");
        return;
    }
    const uint64_t stream_id = stream_id_maybe.value();
    const int64_t timestamp_us =
        json.value("timestamp_us", static_cast<int64_t>(-1));

    // Resolve the stream's topic the same way subscribe does: a DATA topic on
    // the matching stream, else a graph-internal output, else a marker/meta
    // topic. We need its Kafka topic name to query offsets.
    std::shared_ptr<nat::core::BasicTopicInformation> topic;
    auto rawStreams = broker_manager_->getAllStreams();
    for (const auto& stream : rawStreams) {
        if (stream && stream->getId() == stream_id) {
            topic = choosePreferredDataTopic(
                stream->getTopicsByType(nat::core::StreamType::DATA));
            break;
        }
    }
    if (topic == nullptr) {
        topic = findGraphInternalOutputTopicForStream(stream_id);
    }
    if (topic == nullptr) {
        topic = findMarkerOrMetaTopicForStreamId(broker_manager_, stream_id);
    }

    nlohmann::json response;
    response["type"] = "stream_time";
    response["request_id"] = request_id;
    response["stream_id"] = std::to_string(stream_id);
    if (topic == nullptr) {
        response["valid"] = false;
        response["reason"] = "stream not found or has no readable topic yet";
        if (conn && conn->connected()) {
            conn->send(response.dump());
        }
        return;
    }

    const auto extent =
        broker_manager_->queryStreamTime(topic->toTopicString(), timestamp_us);
    response["valid"] = extent.valid;
    response["earliest_offset"] = extent.earliestOffset;
    response["latest_offset"] = extent.latestOffset;
    response["offset_for_timestamp"] = extent.offsetForTimestamp;
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

void StreamViewerWebSocket::handlePublishSessionBundle(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    const std::string request_id = json.value("request_id", std::string{});
    const std::string session_id = json.value("session_id", std::string{});
    if (!isValidTopicIdentifier(session_id)) {
        sendError(
            conn,
            "session_id must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ for Kafka topic derivation");
        return;
    }

    const auto meta_topic_info = createTopicInfo(
        nat::core::StreamType::META,
        "session_id",
        session_id,
        nat::core::MetaRecord::name);
    const auto marker_topic_info = createTopicInfo(
        nat::core::StreamType::MARKER,
        "session_id",
        session_id,
        nat::core::MarkerEventV1::name);
    if (meta_topic_info == nullptr || marker_topic_info == nullptr) {
        sendError(conn, "Failed to create Kafka topic information for session bundle");
        return;
    }

    auto meta_messenger = broker_manager_->createMessenger(meta_topic_info);
    auto marker_messenger = broker_manager_->createMessenger(marker_topic_info);

    size_t published_meta_records = 0;
    size_t published_marker_events = 0;

    if (json.contains("meta_records")) {
        if (!json["meta_records"].is_array()) {
            sendError(conn, "meta_records must be an array");
            return;
        }

        for (const auto& value : json["meta_records"]) {
            auto record = parseSessionMetadataRecord(value);
            if (record->getSessionId().empty()) {
                sendError(conn, "meta_records entries must include session_id");
                return;
            }
            if (record->getSessionId() != session_id) {
                sendError(conn, "meta_records session_id does not match request session_id");
                return;
            }
            meta_messenger->sendMessage(*record);
            ++published_meta_records;
        }
    }

    if (json.contains("marker_events")) {
        if (!json["marker_events"].is_array()) {
            sendError(conn, "marker_events must be an array");
            return;
        }

        for (const auto& value : json["marker_events"]) {
            auto record = parseMarkerEventRecord(value);
            if (record->getSessionId().empty()) {
                sendError(conn, "marker_events entries must include session_id");
                return;
            }
            if (record->getSessionId() != session_id) {
                sendError(conn, "marker_events session_id does not match request session_id");
                return;
            }
            if (record->getMarkerType().empty() || record->getMarkerId().empty() ||
                record->getEvent().empty() || record->getLabel().empty()) {
                sendError(
                    conn,
                    "marker_events entries require marker_type, marker_id, event, and label");
                return;
            }
            marker_messenger->sendMessage(*record);
            ++published_marker_events;
        }
    }

    sendPublishResult(
        conn,
        request_id,
        session_id,
        published_meta_records,
        published_marker_events);
}

void StreamViewerWebSocket::handleListTransformCapabilities(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    sendTransformCapabilities(conn, json.value("request_id", std::string{}));
}

void StreamViewerWebSocket::handleListNodeCatalog(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    sendNodeCatalog(conn, json.value("request_id", std::string{}));
}

void StreamViewerWebSocket::handleCreateTransform(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    const std::string request_id = json.value("request_id", std::string{});
    const auto source_stream_id_maybe = parseStreamId(
        json.contains("source_stream_id") ? json["source_stream_id"] : nlohmann::json{});
    if (!source_stream_id_maybe.has_value()) {
        sendError(conn, "source_stream_id must be a valid non-negative integer");
        return;
    }

    const std::string output_identifier =
        json.value("output_identifier", std::string{});
    if (!isValidTopicIdentifier(output_identifier)) {
        sendError(
            conn,
            "output_identifier must match ^[A-Za-z0-9][A-Za-z0-9_-]*$");
        return;
    }

    const auto config_maybe = parseEmgTransformConfig(json);
    if (!config_maybe.has_value()) {
        sendError(
            conn,
            "transform_kind must be one of: rectify, lowpass_envelope, bandpass_iir, notch_iir, rms_window, sliding_window, highpass_iir, mav, rms, wl, zc, ssc, ar_coeffs, lda_classify, emg_gesture_classify, channel_select");
        return;
    }
    const std::string requested_input_mapping_id =
        json.value("input_mapping_id", std::string{});
    const auto result = createTransformWorker(
        broker_manager_,
        source_stream_id_maybe.value(),
        output_identifier,
        config_maybe.value(),
        requested_input_mapping_id);
    if (!result.ok) {
        sendError(conn, result.error);
        return;
    }

    sendTransformResult(
        conn,
        request_id,
        result.sourceStreamId,
        result.outputStreamId,
        result.outputIdentifier,
        result.transformKind,
        result.inputMappingId,
        result.topic,
        result.workerId,
        result.threadSlotId,
        result.slotCapacity,
        result.activeCount,
        result.alreadyExists);
    sendTransformList(conn, request_id);
    broadcastTransformList();
}

void StreamViewerWebSocket::handleListTransforms(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    sendTransformList(conn, json.value("request_id", std::string{}));
}

void StreamViewerWebSocket::handleStopTransform(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto output_stream_id_maybe = parseStreamId(
        json.contains("output_stream_id") ? json["output_stream_id"] : nlohmann::json{});
    if (!output_stream_id_maybe.has_value()) {
        sendError(conn, "output_stream_id must be a valid non-negative integer");
        return;
    }

    size_t remaining_active_count = 0;
    if (!stopTransformWorkerByOutputStreamId(
            output_stream_id_maybe.value(), remaining_active_count)) {
        sendError(conn, "No active transform exists for output_stream_id");
        return;
    }

    nlohmann::json response;
    response["type"] = "transform_stopped";
    response["request_id"] = request_id;
    response["output_stream_id"] = std::to_string(output_stream_id_maybe.value());
    response["worker_id"] = "natkit-local-transform-worker";
    response["active_count"] = remaining_active_count;
    response["slot_capacity"] = g_transform_slot_capacity;
    conn->send(response.dump());
    sendTransformList(conn, request_id);
    broadcastTransformList();
}

void StreamViewerWebSocket::handleListStreamGraphs(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error);
            return;
        }
    }
    sendStreamGraphList(conn, request_id);
}

// Derive the next free fork id under a parent instance: <parent>-a, -b, ... then
// -aa. Suffixing rather than renumbering keeps the lineage readable in the tree
// (run-0001-a-1 is plainly a fork of a fork of recording 1).
std::string nextForkInstanceIdLocked(const std::string& parentInstanceId)
{
    const auto taken = [](const std::string& candidate) {
        for (const auto& entry : g_stream_graphs) {
            if (entry.second.instanceId == candidate) {
                return true;
            }
        }
        return false;
    };
    for (int width = 1; width <= 2; ++width) {
        for (char first = 'a'; first <= 'z'; ++first) {
            if (width == 1) {
                const auto candidate = parentInstanceId + "-" + std::string(1, first);
                if (!taken(candidate)) {
                    return candidate;
                }
                continue;
            }
            for (char second = 'a'; second <= 'z'; ++second) {
                const auto candidate =
                    parentInstanceId + "-" + std::string(1, first) + std::string(1, second);
                if (!taken(candidate)) {
                    return candidate;
                }
            }
        }
    }
    return parentInstanceId + "-" + std::to_string(nowUs());
}

void StreamViewerWebSocket::handleForkStreamGraph(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const auto source_graph_id = json.value("source_graph_id", std::string{});
    if (source_graph_id.empty()) {
        sendError(conn, "fork_stream_graph requires source_graph_id",
                  json.value("request_id", std::string{}));
        return;
    }

    StreamGraphDefinition fork;
    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error);
            return;
        }
        const auto source = g_stream_graphs.find(source_graph_id);
        if (source == g_stream_graphs.end()) {
            sendError(conn, "Unknown graph: " + source_graph_id,
                  json.value("request_id", std::string{}));
            return;
        }
        if (source->second.instanceId.empty()) {
            sendError(
                conn,
                "Only an instance can be forked. Graph '" + source_graph_id +
                    "' is a live board with no recorded data behind it.",
                json.value("request_id", std::string{}));
            return;
        }

        // Copy the pipeline, inherit the data.
        fork = source->second;
        fork.graphId = source_graph_id + "-fork-" + std::to_string(nowUs());
        fork.instanceId = nextForkInstanceIdLocked(source->second.instanceId);
        fork.immutable = false;                       // the whole point
        fork.origin = "fork";
        fork.forkedFrom = source->second.instanceId;  // nests under its parent
        fork.experimentId = source->second.experimentId;
        fork.recording = source->second.recording;    // SAME artifacts, not a copy
        const auto requested_label = json.value("label", std::string{});
        fork.label = requested_label.empty()
                         ? source->second.label + " (fork)"
                         : requested_label;
        fork.createdAtUs = nowUs();
        fork.updatedAtUs = fork.createdAtUs;

        g_stream_graphs[fork.graphId] = fork;
        if (!persistStreamGraphStoreLocked(persist_error)) {
            sendError(conn, persist_error);
            return;
        }
    }

    LOG_INFO << "Forked instance " << fork.forkedFrom << " -> " << fork.instanceId
             << " (graph " << fork.graphId << ")";

    nlohmann::json response;
    response["type"] = "stream_graph_forked";
    response["request_id"] = json.value("request_id", std::string{});
    response["graph"] = fork;
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

void StreamViewerWebSocket::handleSaveStreamGraph(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    if (!json.contains("graph")) {
        sendError(conn, "save_stream_graph requires a graph payload");
        return;
    }

    StreamGraphDefinition graph;
    try {
        graph = json.at("graph").get<StreamGraphDefinition>();
        for (auto& node : graph.nodes) {
            normalizeGraphNodePorts(node);
        }
    } catch (const std::exception& exception) {
        sendError(
            conn,
            std::string("Failed to parse stream graph payload: ") + exception.what());
        return;
    }

    if (graph.createdAtUs == 0) {
        graph.createdAtUs = nowUs();
    }
    graph.updatedAtUs = nowUs();

    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error);
            return;
        }
        // Immutability backstop. A recorded instance is a historical fact: the
        // graph as it was when the data was captured. Reject the write here
        // rather than trusting the editor's read-only mode -- the WS protocol is
        // a public surface, and the reactive auto-save (Phase 7) fires on a
        // debounce, so a stray edit would otherwise silently rewrite history.
        const auto existing = g_stream_graphs.find(graph.graphId);
        if (existing != g_stream_graphs.end() && existing->second.immutable) {
            sendError(
                conn,
                "Cannot save over immutable instance '" + graph.graphId +
                    "': it is the recorded snapshot for instance " +
                    existing->second.instanceId +
                    ". Fork it (fork_stream_graph) to get an editable copy.",
                json.value("request_id", std::string{}));
            return;
        }
        // Provenance is owned by the backend, not the client: a save must not be
        // able to launder a fork into a recording, re-parent it, or repoint it at
        // another session's artifacts.
        if (existing != g_stream_graphs.end()) {
            graph.experimentId = existing->second.experimentId;
            graph.instanceId = existing->second.instanceId;
            graph.immutable = existing->second.immutable;
            graph.origin = existing->second.origin;
            graph.forkedFrom = existing->second.forkedFrom;
            graph.recording = existing->second.recording;
        }
        g_stream_graphs[graph.graphId] = graph;
        if (!persistStreamGraphStoreLocked(persist_error)) {
            sendError(conn, persist_error);
            return;
        }
    }

    sendStreamGraphSaved(
        conn,
        json.value("request_id", std::string{}),
        graph);
}

void StreamViewerWebSocket::handleListProfiles(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    {
        std::lock_guard<std::mutex> lock(g_profile_mutex);
        ensureProfileStoreLoadedLocked();
        if (!g_profile_store_error.empty()) {
            sendError(conn, g_profile_store_error);
            return;
        }
    }
    sendProfileList(conn, request_id);
}

void StreamViewerWebSocket::handleSaveProfile(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    if (!json.contains("profile")) {
        sendError(conn, "save_profile requires a profile payload");
        return;
    }

    Profile profile;
    try {
        profile = json.at("profile").get<Profile>();
    } catch (const std::exception& exception) {
        sendError(
            conn,
            std::string("Failed to parse profile payload: ") + exception.what());
        return;
    }
    if (profile.participantId.empty()) {
        sendError(conn, "save_profile requires a non-empty participant_id");
        return;
    }

    if (profile.createdAtUs == 0) {
        profile.createdAtUs = nowUs();
    }
    profile.updatedAtUs = nowUs();

    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_profile_mutex);
        ensureProfileStoreLoadedLocked();
        if (!g_profile_store_error.empty()) {
            sendError(conn, g_profile_store_error);
            return;
        }
        // Preserve the original creation timestamp on update.
        const auto existing = g_profiles.find(profile.participantId);
        if (existing != g_profiles.end() && existing->second.createdAtUs != 0) {
            profile.createdAtUs = existing->second.createdAtUs;
        }
        g_profiles[profile.participantId] = profile;
        if (!persistProfileStoreLocked(persist_error)) {
            sendError(conn, persist_error);
            return;
        }
    }

    sendProfileSaved(conn, json.value("request_id", std::string{}), profile);
}

void StreamViewerWebSocket::handleDeleteProfile(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string participant_id = json.value("participant_id", std::string{});
    if (participant_id.empty()) {
        sendError(conn, "delete_profile requires a participant_id");
        return;
    }

    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_profile_mutex);
        ensureProfileStoreLoadedLocked();
        if (!g_profile_store_error.empty()) {
            sendError(conn, g_profile_store_error);
            return;
        }
        if (g_profiles.erase(participant_id) == 0) {
            sendError(conn, "No profile found for participant_id " + participant_id);
            return;
        }
        if (!persistProfileStoreLocked(persist_error)) {
            sendError(conn, persist_error);
            return;
        }
    }

    sendProfileDeleted(
        conn, json.value("request_id", std::string{}), participant_id);
}

void StreamViewerWebSocket::handleStartExperimentInstance(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto experiment_id = json.value("experiment_id", std::string{});
    if (experiment_id.empty()) {
        sendError(conn, "start_experiment_instance requires experiment_id", request_id);
        return;
    }

    StreamGraphDefinition instance;
    std::string persist_error;
    {
        std::lock_guard<std::mutex> experiment_lock(g_experiment_mutex);
        ensureExperimentStoreLoadedLocked();
        if (!g_experiment_store_error.empty()) {
            sendError(conn, g_experiment_store_error, request_id);
            return;
        }
        const auto experiment = g_experiments.find(experiment_id);
        if (experiment == g_experiments.end()) {
            sendError(conn, "Unknown experiment: " + experiment_id, request_id);
            return;
        }
        const auto live_graph_id = experiment->second.liveGraphId;
        if (live_graph_id.empty()) {
            sendError(conn,
                      "Experiment '" + experiment_id +
                          "' has no board bound, so there is no graph to snapshot.",
                      request_id);
            return;
        }

        std::lock_guard<std::mutex> graph_lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error, request_id);
            return;
        }
        const auto live = g_stream_graphs.find(live_graph_id);
        if (live == g_stream_graphs.end()) {
            sendError(conn, "Unknown graph: " + live_graph_id, request_id);
            return;
        }

        // THE SNAPSHOT. Copy the live board wholesale -- nodes, edges and the
        // opaque editor_metadata (the unflattened composite tree), because an
        // instance has to reopen as the board it was, not as a flattened
        // approximation of it.
        instance = live->second;
        instance.instanceId = nextRecordingInstanceIdLocked(experiment_id);
        instance.graphId = experiment_id + "-" + instance.instanceId;
        instance.label = live->second.label + " · " + instance.instanceId;
        instance.experimentId = experiment_id;
        instance.origin = "recording";
        instance.forkedFrom.clear();
        // Sealed only when materialization succeeds. Immutable from the start
        // would be self-defeating: the status transitions below are writes.
        instance.immutable = false;
        instance.createdAtUs = nowUs();
        instance.updatedAtUs = instance.createdAtUs;

        // Recorded streams: RAW SOURCES ONLY (decided in the plan). Snapshotting
        // derived streams would bloat the instance and let a fork silently read
        // stale derived data instead of recomputing -- which is the entire point
        // of forking.
        nlohmann::json streams = nlohmann::json::array();
        // The same ids, plainly, for the clock-fit snapshot below.
        std::vector<std::string> streams_for_clock;
        for (const auto& node : instance.nodes) {
            if (node.kind != "stream_source" || !node.streamId.has_value()) {
                continue;
            }
            nlohmann::json entry;
            entry["stream_id"] = std::to_string(node.streamId.value());
            entry["schema_name"] = node.schemaName.value_or("");
            entry["role"] = "source";
            entry["node_id"] = node.id;
            streams_for_clock.push_back(std::to_string(node.streamId.value()));
            streams.push_back(std::move(entry));
        }

        nlohmann::json recording;
        // One marker topic per experiment (Marker/<experiment_id>), so every run
        // shares a session id and is delimited by its own window.
        recording["session_id"] = experiment_id;
        // WHO, and WHAT THEY WERE ASKED TO DO, captured HERE rather than resolved
        // at read time. Both used to be read back through the live experiment
        // record, whose participant_id is an editable text field and whose
        // protocol is re-authorable -- so retyping either silently re-attributed
        // every past run of this experiment, while the sha256'd artifacts kept
        // verifying because none of the DATA had changed. A sealed instance has to
        // answer both questions from itself.
        //
        // The board snapshot above cannot cover this: the protocol moved off the
        // canvas to the experiment record, so the copied nodes carry only a
        // config-less `markers` source.
        //
        // WHO comes from the REQUEST, per run (TEC-NATKIT-55): one experiment
        // records a whole cohort, so the procedure cannot name the person. An
        // absent participant is recorded as absent rather than quietly inherited
        // from anywhere -- the same three states the back-fill keeps apart, so a
        // run taken before the Record gate exists says so about itself instead of
        // borrowing an attribution.
        const auto participant_id = json.value("participant_id", std::string{});
        recording["participant_id"] = participant_id;
        if (participant_id.empty()) {
            recording["participant_unrecorded"] = true;
        }
        recording["protocol"] = experiment->second.protocol;
        // WHAT WAS WORN WHERE (TEC-NATKIT-62). Per run, like the participant: the
        // same board records different people with the sensors physically re-placed
        // each time, so a mapping that lives only on the board says nothing about
        // any particular run. Absent when the client sent none -- recorded as
        // absent rather than as an empty placement.
        if (json.contains("sensor_positions") &&
            json["sensor_positions"].is_array() &&
            !json["sensor_positions"].empty()) {
            recording["sensor_positions"] = json["sensor_positions"];
        }
        // WHETHER THE CLOCKS COULD BE TRUSTED (TEC-NATKIT-77), at both ends of
        // the run.
        //
        // ⚠️ Sealed HERE rather than left to be looked up later, for the same
        // reason as the participant and the sensor placement: the status frames
        // age out of Kafka retention and nothing else keeps a copy, so a cohort
        // recorded while a leaf had no valid fit becomes indistinguishable from a
        // clean one — permanently, and "was the clock all right?" is a question
        // asked months later or not at all.
        //
        // Both ends, because the endpoints answer different questions. The start
        // says whether it was reasonable to begin; the pair says whether the fit
        // was REBUILT mid-run, which no single sample can reveal and which means
        // timestamps either side of it sit on different fits.
        {
            auto& health = nat::tools::DeviceHealthService::instance();
            health.start(broker_manager_);
            const auto fits = nat::tools::snapshotClockFits(
                health.snapshot(), streams_for_clock,
                health.running() && health.topicsTailed() > 0);
            nlohmann::json at_start = nlohmann::json::array();
            for (const auto& fit : fits) {
                at_start.push_back(fit.toJson());
            }
            recording["clock_at_start"] = std::move(at_start);
        }
        // Recorded through the calibration gate on purpose (TEC-NATKIT-63). Stored
        // with the reason the operator was shown, because by the time anyone asks
        // why, the accuracies that justified the decision are long gone.
        //
        // ⚠️ Its ABSENCE is the claim that the run met the threshold, so this is
        // written only from the gate's own field and never defaulted to a value.
        const auto calibration_override =
            json.value("calibration_override", std::string{});
        if (!calibration_override.empty()) {
            recording["calibration_override"] = calibration_override;
        }
        recording["window_start_us"] =
            json.value("window_start_us", static_cast<uint64_t>(nowUs()));
        recording["window_end_us"] = nullptr;
        recording["streams"] = std::move(streams);
        recording["artifacts"] = nlohmann::json::object();
        recording["status"] = "recording";
        instance.recording = std::move(recording);

        g_stream_graphs[instance.graphId] = instance;
        if (!persistStreamGraphStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
    }

    LOG_INFO << "Recording instance " << instance.instanceId << " for experiment "
             << experiment_id << " (graph " << instance.graphId << ", "
             << instance.recording.value("streams", nlohmann::json::array()).size()
             << " recorded source(s))";
    sendExperimentInstance(conn, request_id, instance);
}

void StreamViewerWebSocket::handleFinishExperimentInstance(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "finish_experiment_instance requires graph_id", request_id);
        return;
    }

    StreamGraphDefinition instance;
    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error, request_id);
            return;
        }
        const auto stored = g_stream_graphs.find(graph_id);
        if (stored == g_stream_graphs.end()) {
            sendError(conn, "Unknown graph: " + graph_id, request_id);
            return;
        }
        if (stored->second.instanceId.empty() ||
            stored->second.origin != "recording") {
            sendError(conn,
                      "Graph '" + graph_id + "' is not a recording instance.",
                      request_id);
            return;
        }
        if (stored->second.recording.value("status", std::string{}) != "recording") {
            sendError(conn,
                      "Instance " + stored->second.instanceId + " is already " +
                          stored->second.recording.value("status", std::string{}) +
                          ".",
                      request_id);
            return;
        }
        const uint64_t window_end_us =
            json.value("window_end_us", static_cast<uint64_t>(nowUs()));
        stored->second.recording["window_end_us"] = window_end_us;

        // The other end of the clock record (TEC-NATKIT-77). Differenced against
        // the start so the beacon-loss figure is a RATE OVER THIS RUN rather than
        // a total since the device booted — the total would say "this run missed
        // 3662 beacons" about a device that missed them yesterday.
        {
            std::vector<std::string> source_stream_ids;
            for (const auto& entry :
                 stored->second.recording.value("streams", nlohmann::json::array())) {
                const auto stream_id = entry.value("stream_id", std::string{});
                if (!stream_id.empty()) {
                    source_stream_ids.push_back(stream_id);
                }
            }
            auto& health = nat::tools::DeviceHealthService::instance();
            health.start(broker_manager_);
            const auto fits = nat::tools::snapshotClockFits(
                health.snapshot(), source_stream_ids,
                health.running() && health.topicsTailed() > 0);
            nlohmann::json at_finish = nlohmann::json::array();
            for (const auto& fit : fits) {
                at_finish.push_back(fit.toJson());
            }
            const uint64_t window_start_us =
                stored->second.recording.value("window_start_us", window_end_us);
            const double run_seconds =
                window_end_us > window_start_us
                    ? static_cast<double>(window_end_us - window_start_us) / 1e6
                    : 0.0;
            stored->second.recording["clock_quality"] = nat::tools::buildClockQuality(
                stored->second.recording.value("clock_at_start",
                                               nlohmann::json::array()),
                at_finish, run_seconds);
            stored->second.recording["clock_at_finish"] = std::move(at_finish);
        }
        stored->second.recording["status"] = "materializing";
        stored->second.updatedAtUs = nowUs();
        if (!json.value("completed", true)) {
            stored->second.recording["message"] =
                "Recording was stopped early; the window is a partial run.";
        }
        instance = stored->second;
        if (!persistStreamGraphStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
    }

    sendExperimentInstance(conn, request_id, instance);

    // Materialize off the WS thread: draining a topic per recorded source takes
    // tens of seconds, and pinning a drogon worker for that would stall every
    // other client. Status updates are broadcast as they land.
    std::thread([this, graph_id]() { materializeInstance(graph_id); }).detach();
}

void StreamViewerWebSocket::materializeInstance(const std::string& graph_id)
{
    std::string experiment_id;
    std::string instance_id;
    std::string participant_id;
    std::vector<std::pair<uint64_t, std::string>> sources;  // id, schema_name
    int64_t window_start = 0;
    int64_t window_end = 0;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto stored = g_stream_graphs.find(graph_id);
        if (stored == g_stream_graphs.end()) {
            return;
        }
        experiment_id = stored->second.experimentId;
        instance_id = stored->second.instanceId;
        const auto& recording = stored->second.recording;
        // Read from the INSTANCE, never from the live experiment: this runs on a
        // detached thread long after Record was pressed, so the experiment's
        // participant field may already have been edited for the next run.
        participant_id = recording.value("participant_id", std::string{});
        window_start = recording.value("window_start_us", static_cast<int64_t>(0));
        window_end = recording.value("window_end_us", static_cast<int64_t>(0));
        for (const auto& entry : recording.value("streams", nlohmann::json::array())) {
            const auto id_text = entry.value("stream_id", std::string{});
            try {
                sources.emplace_back(std::stoull(id_text),
                                     entry.value("schema_name", std::string{}));
            } catch (const std::exception&) {
                continue;
            }
        }
    }

    const auto finish = [this, &graph_id](const std::string& status,
                                         const std::string& message,
                                         const nlohmann::json& artifacts) {
        StreamGraphDefinition updated;
        std::string persist_error;
        {
            std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
            const auto stored = g_stream_graphs.find(graph_id);
            if (stored == g_stream_graphs.end()) {
                return;
            }
            stored->second.recording["status"] = status;
            if (!message.empty()) {
                stored->second.recording["message"] = message;
            }
            if (!artifacts.is_null()) {
                stored->second.recording["artifacts"] = artifacts;
            }
            // Seal it. A completed instance is a historical fact: the graph as it
            // was when the data was captured, welded to files that exist. A FAILED
            // one stays mutable so it can be retried or deleted -- sealing an
            // empty snapshot is exactly the failure mode the plan warns about.
            stored->second.immutable = (status == "complete");
            stored->second.updatedAtUs = nowUs();
            updated = stored->second;
            persistStreamGraphStoreLocked(persist_error);
        }
        if (!persist_error.empty()) {
            LOG_ERROR << "Failed to persist instance status: " << persist_error;
        }
        LOG_INFO << "Instance " << updated.instanceId << " -> " << status
                 << (message.empty() ? "" : (": " + message));
        broadcastExperimentInstance(updated);
    };

    if (!natkit::tools::parquetExportAvailable()) {
        finish("failed",
               "This backend was built without Parquet support, so a recording "
               "cannot be materialized. Rebuild the image with libparquet-dev.",
               nlohmann::json(nullptr));
        return;
    }
    if (sources.empty()) {
        finish("failed",
               "The recorded board has no stream_source nodes, so there is no raw "
               "data to materialize.",
               nlohmann::json(nullptr));
        return;
    }

    const auto directory = instanceArtifactDir(experiment_id, instance_id);
    std::error_code dir_error;
    std::filesystem::create_directories(directory, dir_error);
    if (dir_error) {
        finish("failed",
               "Could not create the instance directory " + directory.string() +
                   ": " + dir_error.message(),
               nlohmann::json(nullptr));
        return;
    }

    // The experiment's marker topic supplies both the cue labels joined onto each
    // row and the sidecar timeline.
    uint64_t marker_stream_id = 0;
    if (isValidTopicIdentifier(experiment_id)) {
        const auto marker_topic = createTopicInfo(
            nat::core::StreamType::MARKER, "session_id", experiment_id,
            nat::core::MarkerEventV1::name);
        if (marker_topic != nullptr) {
            marker_stream_id = marker_topic->id;
        }
    }

    nlohmann::json artifacts;
    artifacts["data"] = nlohmann::json::array();
    size_t total_rows = 0;
    bool markers_written = false;
    std::vector<std::string> failures;

    for (const auto& [stream_id, schema_name] : sources) {
        natkit::tools::ParquetExportRequest request;
        request.streamId = stream_id;
        request.markerStreamId = marker_stream_id;
        // Travels into the file's key-value metadata, so a Parquet that leaves
        // this rig still says whose run it is.
        request.participantId = participant_id;
        request.startUs = window_start > 0 ? std::optional<int64_t>(window_start)
                                          : std::nullopt;
        request.endUs = window_end > 0 ? std::optional<int64_t>(window_end)
                                       : std::nullopt;
        // Only the first source writes the sidecar: the marker timeline belongs to
        // the instance, not to a stream, and re-draining it per source would
        // multiply the cost for identical content.
        if (!markers_written) {
            request.markersSidecarPath = (directory / "markers.jsonl").string();
        }

        const auto exported = natkit::tools::exportStreamToParquet(
            broker_manager_, request, directory.string());
        if (!exported.ok) {
            failures.push_back("stream " + std::to_string(stream_id) + ": " +
                               exported.error);
            continue;
        }
        // MUST verify it captured something. A recording that ran against a
        // wedged bridge produces a session with zero data frames, and sealing
        // that as an immutable snapshot of nothing is worse than failing.
        if (exported.frameCount == 0) {
            failures.push_back(
                "stream " + std::to_string(stream_id) +
                ": no data frames in the recorded window — nothing was flowing "
                "(check the bridge/device), so there is nothing to snapshot.");
            std::error_code remove_error;
            std::filesystem::remove(exported.filePath, remove_error);
            continue;
        }
        if (exported.markersSidecarWritten) {
            markers_written = true;
        }

        // Stable name inside the instance directory (the exporter writes a
        // download-shaped temp name).
        const auto final_path = directory / (std::to_string(stream_id) + ".parquet");
        std::error_code rename_error;
        std::filesystem::rename(exported.filePath, final_path, rename_error);
        if (rename_error) {
            failures.push_back("stream " + std::to_string(stream_id) +
                               ": could not place the artifact: " +
                               rename_error.message());
            continue;
        }

        std::string checksum_error;
        const auto checksum = sha256OfFile(final_path, checksum_error);
        markArtifactReadOnly(final_path);

        nlohmann::json entry;
        entry["stream_id"] = std::to_string(stream_id);
        entry["schema_name"] =
            exported.schemaName.empty() ? schema_name : exported.schemaName;
        entry["path"] = final_path.filename().string();
        entry["rows"] = exported.frameCount;
        entry["labelled_rows"] = exported.labelledFrameCount;
        // Per-class row counts: the difference between "this instance recorded
        // 400k rows" and "this instance is usable" (one class may never have
        // fired). Empty key = rows inside the window but between cues.
        nlohmann::json label_counts = nlohmann::json::object();
        for (const auto& [label, count] : exported.labelCounts) {
            label_counts[label.empty() ? "(unlabelled)" : label] = count;
        }
        entry["label_counts"] = std::move(label_counts);
        entry["sha256"] = checksum;
        if (!checksum_error.empty()) {
            entry["checksum_error"] = checksum_error;
        }
        // Truncation is never silent: the file is a prefix of the stream.
        if (exported.truncated) {
            entry["truncated"] = true;
        }
        artifacts["data"].push_back(std::move(entry));
        total_rows += exported.frameCount;
    }

    if (markers_written) {
        const auto markers_path = directory / "markers.jsonl";
        std::string checksum_error;
        artifacts["markers"] = "markers.jsonl";
        artifacts["markers_sha256"] = sha256OfFile(markers_path, checksum_error);
        markArtifactReadOnly(markers_path);
    }
    artifacts["directory"] = directory.string();
    artifacts["total_rows"] = total_rows;

    if (artifacts["data"].empty()) {
        // Nothing usable was captured, so keep no half-written files. The markers
        // sidecar is written before the (much longer) data drain, so a failure here
        // otherwise leaves an orphan file that is on disk but not in the manifest.
        std::error_code cleanup_error;
        std::filesystem::remove_all(directory, cleanup_error);
        finish("failed",
               failures.empty() ? "Materialization produced no artifacts."
                                : ("Materialization captured nothing. " +
                                   [&failures]() {
                                       std::string joined;
                                       for (const auto& failure : failures) {
                                           if (!joined.empty()) joined += " | ";
                                           joined += failure;
                                       }
                                       return joined;
                                   }()),
               artifacts);
        return;
    }

    std::string message = "Materialized " +
                          std::to_string(artifacts["data"].size()) + " stream(s), " +
                          std::to_string(total_rows) + " rows.";
    if (!failures.empty()) {
        // Partial success is still a success, but say exactly what is missing --
        // a snapshot the operator believes is complete is worse than a loud gap.
        message += " Some streams failed: ";
        for (size_t index = 0; index < failures.size(); ++index) {
            message += (index > 0 ? " | " : "") + failures[index];
        }
    }
    finish("complete", message, artifacts);
}

// Tear down every worker belonging to a graph's active run and mark the runtime
// stopped. Shared by the explicit stop_stream_graph action and by the replay-end
// cleanup, which must stop the run BEFORE its scratch topics are deleted.
//
// `expected_replay_id`, when non-empty, makes this a no-op unless the run is still
// the one bound to that replay — otherwise a replay ending would stop a live run
// the user started in the meantime.
void stopStreamGraphRuntimeById(const std::string& graph_id,
                               const std::string& expected_replay_id = std::string{})
{
    StreamGraphRuntimeState runtime;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto search = g_stream_graph_runtime.find(graph_id);
        if (search == g_stream_graph_runtime.end()) {
            return;
        }
        if (!expected_replay_id.empty() &&
            search->second.boundReplayId != expected_replay_id) {
            return;
        }
        runtime = search->second;
    }

    for (const auto output_stream_id : runtime.outputStreamIds) {
        stopGraphWorkerByOutputStreamId(output_stream_id);
    }

    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        auto& active_runtime = g_stream_graph_runtime[graph_id];
        active_runtime.runState = "stopped";
        active_runtime.boundReplayId.clear();
        for (auto& entry : active_runtime.nodeStatuses) {
            if (entry.second.state == "running" || entry.second.state == "stalled" ||
                entry.second.state == "starting" || entry.second.state == "blocked" ||
                entry.second.state == "error") {
                entry.second.state = entry.second.outputStreamId.has_value()
                                         ? "stopped"
                                         : entry.second.state;
            }
        }
    }
}

void StreamViewerWebSocket::handleStartInstanceReplay(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "start_instance_replay requires graph_id", request_id);
        return;
    }
    if (!natkit::tools::replayAvailable()) {
        sendError(conn,
                  "This backend was built without Parquet support, so an instance "
                  "cannot be replayed. Rebuild the image with libparquet-dev.",
                  request_id);
        return;
    }

    // Review = paced from the original timestamp deltas (watch it back like a
    // video). Recompute = unpaced, for running a fork's changed pipeline or
    // training, where nobody is watching frames go by.
    const auto mode = json.value("mode", std::string{"review"});
    natkit::tools::ReplayRequest request;
    request.paced = (mode != "recompute");
    request.speed = std::clamp(json.value("speed", 1.0), 0.25, 8.0);

    std::string instance_id;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        const auto stored = g_stream_graphs.find(graph_id);
        if (stored == g_stream_graphs.end()) {
            sendError(conn, "Unknown graph: " + graph_id, request_id);
            return;
        }
        if (stored->second.instanceId.empty()) {
            sendError(conn,
                      "Only an instance can be replayed; '" + graph_id +
                          "' is a live board (its data is still on the broker).",
                      request_id);
            return;
        }
        instance_id = stored->second.instanceId;
        const auto artifacts =
            stored->second.recording.value("artifacts", nlohmann::json::object());
        const auto directory = std::filesystem::path(artifacts.value(
            "directory",
            instanceArtifactDir(stored->second.experimentId, instance_id).string()));
        for (const auto& data : artifacts.value("data", nlohmann::json::array())) {
            natkit::tools::ReplaySourceSpec spec;
            try {
                spec.originalStreamId =
                    std::stoull(data.value("stream_id", std::string{"0"}));
            } catch (const std::exception&) {
                continue;
            }
            spec.parquetPath =
                (directory / data.value("path", std::string{})).string();
            request.sources.push_back(std::move(spec));
        }
        if (artifacts.contains("markers")) {
            request.markersPath =
                (directory / artifacts.value("markers", std::string{})).string();
        }
        if (request.sources.empty()) {
            sendError(conn,
                      "Instance " + instance_id +
                          " has no materialized artifacts to replay.",
                      request_id);
            return;
        }
    }

    // A fresh identifier per replay session keeps two replays of the same instance
    // (or a replay and a live board) on separate topics.
    const auto nonce = std::to_string(nowUs());
    request.replayIdentifier =
        "replay-" + sanitizeTopicIdentifier(instance_id) + "-" + nonce;

    const auto plan = natkit::tools::planReplay(broker_manager_, request);
    if (!plan.ok) {
        sendError(conn, plan.error, request_id);
        return;
    }

    ActiveReplay active;
    active.replayId = request.replayIdentifier;
    active.graphId = graph_id;
    active.plan = plan;
    active.cancelled = std::make_shared<std::atomic<bool>>(false);
    active.paced = request.paced;
    active.speed = request.speed;
    {
        std::lock_guard<std::mutex> lock(g_replay_mutex);
        g_active_replays[active.replayId] = active;
    }

    const auto replay_id = active.replayId;
    const auto cancelled = active.cancelled;
    std::thread([this, request, plan, replay_id, graph_id, cancelled]() {
        natkit::tools::runReplay(
            broker_manager_, request, plan, *cancelled,
            [this, replay_id, graph_id, &plan](
                const natkit::tools::ReplayProgress& progress) {
                {
                    std::lock_guard<std::mutex> lock(g_replay_mutex);
                    const auto search = g_active_replays.find(replay_id);
                    if (search != g_active_replays.end()) {
                        search->second.progress = progress;
                    }
                }
                if (progress.finished) {
                    broadcastInstanceReplayState(
                        replay_id, graph_id, plan, progress,
                        progress.cancelled ? "stopped"
                                           : (progress.error.empty() ? "finished"
                                                                     : "failed"));
                } else {
                    broadcastInstanceReplayState(replay_id, graph_id, plan, progress,
                                                "running");
                }
            });
        // Stop the run this replay owns BEFORE its topics go away. Two reasons:
        // workers left consuming a deleted topic spam the bridge with "topic does
        // not exist", and a runtime left in "running" makes the next Replay fail
        // with "Graph is already running; stop it before starting again" — so
        // replay worked once and then refused until the user pressed Stop.
        stopStreamGraphRuntimeById(graph_id, replay_id);

        // The scratch topics die with the replay.
        natkit::tools::deleteReplayTopics(broker_manager_, plan);
        {
            std::lock_guard<std::mutex> lock(g_replay_mutex);
            g_active_replays.erase(replay_id);
        }
        LOG_INFO << "Replay " << replay_id << " ended; scratch topics removed";
    }).detach();

    // Reply once the first record is actually on the topic.
    //
    // A Kafka topic is auto-created on first PRODUCE, so replying immediately would
    // hand the client scratch stream ids for topics that do not exist yet — and a
    // transform worker resolves its source topic against the broker, so it would
    // fail to start with "could not locate a compatible DATA topic". Losing the
    // first few frames from a viewer's perspective is invisible; a transform that
    // refuses to start is not. Bounded so a failing replay still answers.
    for (int attempt = 0; attempt < 60; ++attempt) {
        bool published = false;
        bool gone = false;
        {
            std::lock_guard<std::mutex> lock(g_replay_mutex);
            const auto search = g_active_replays.find(replay_id);
            if (search == g_active_replays.end()) {
                gone = true;  // finished or failed already
            } else {
                published = search->second.progress.framesPublished > 0 ||
                            search->second.progress.markersPublished > 0;
            }
        }
        if (published || gone) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    natkit::tools::ReplayProgress observed;
    {
        std::lock_guard<std::mutex> lock(g_replay_mutex);
        const auto search = g_active_replays.find(replay_id);
        if (search != g_active_replays.end()) {
            observed = search->second.progress;
        }
    }
    sendInstanceReplayState(conn, request_id, replay_id, graph_id, plan, observed,
                            "started");
}

void StreamViewerWebSocket::handleStopInstanceReplay(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto replay_id = json.value("replay_id", std::string{});
    if (replay_id.empty()) {
        sendError(conn, "stop_instance_replay requires replay_id", request_id);
        return;
    }
    std::string graph_id;
    natkit::tools::ReplayPlan plan;
    natkit::tools::ReplayProgress progress;
    {
        std::lock_guard<std::mutex> lock(g_replay_mutex);
        const auto search = g_active_replays.find(replay_id);
        if (search == g_active_replays.end()) {
            sendError(conn, "No active replay with id " + replay_id, request_id);
            return;
        }
        search->second.cancelled->store(true);
        graph_id = search->second.graphId;
        plan = search->second.plan;
        progress = search->second.progress;
    }
    // The replay thread does the topic cleanup and broadcasts the final state; this
    // just acknowledges the request.
    sendInstanceReplayState(conn, request_id, replay_id, graph_id, plan, progress,
                            "stopping");
}

void StreamViewerWebSocket::handleListInstanceReplays(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    nlohmann::json response;
    response["type"] = "instance_replay_list";
    response["request_id"] = json.value("request_id", std::string{});
    response["replays"] = nlohmann::json::array();
    std::lock_guard<std::mutex> lock(g_replay_mutex);
    for (const auto& entry : g_active_replays) {
        response["replays"].push_back(
            makeReplayStateJson(entry.first, entry.second.graphId,
                                entry.second.plan, entry.second.progress,
                                "running"));
    }
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

void StreamViewerWebSocket::handleVerifyExperimentInstance(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "verify_experiment_instance requires graph_id", request_id);
        return;
    }

    std::string experiment_id;
    std::string instance_id;
    nlohmann::json artifacts;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        const auto stored = g_stream_graphs.find(graph_id);
        if (stored == g_stream_graphs.end()) {
            sendError(conn, "Unknown graph: " + graph_id, request_id);
            return;
        }
        if (stored->second.instanceId.empty()) {
            sendError(conn, "Graph '" + graph_id + "' is not an instance.", request_id);
            return;
        }
        experiment_id = stored->second.experimentId;
        instance_id = stored->second.instanceId;
        artifacts = stored->second.recording.value("artifacts", nlohmann::json::object());
    }

    // A fork inherits its ancestor's artifacts verbatim, so it verifies the very
    // same files -- which is the point: a fork depending on corrupted data should
    // report it too.
    const auto directory = artifacts.contains("directory")
                               ? std::filesystem::path(
                                     artifacts.value("directory", std::string{}))
                               : instanceArtifactDir(experiment_id, instance_id);

    nlohmann::json report = nlohmann::json::array();
    bool all_ok = true;
    const auto verify = [&](const std::string& relative_path,
                            const std::string& expected) {
        nlohmann::json entry;
        entry["path"] = relative_path;
        const auto full = directory / relative_path;
        if (!std::filesystem::exists(full)) {
            entry["ok"] = false;
            entry["problem"] = "missing";
            all_ok = false;
            report.push_back(std::move(entry));
            return;
        }
        std::error_code size_error;
        entry["size"] = static_cast<uint64_t>(
            std::filesystem::file_size(full, size_error));
        if (expected.empty()) {
            // Nothing to compare against: say so rather than implying a pass.
            entry["ok"] = false;
            entry["problem"] = "no recorded checksum";
            all_ok = false;
            report.push_back(std::move(entry));
            return;
        }
        std::string checksum_error;
        const auto actual = sha256OfFile(full, checksum_error);
        entry["expected_sha256"] = expected;
        entry["actual_sha256"] = actual;
        if (!checksum_error.empty()) {
            entry["ok"] = false;
            entry["problem"] = checksum_error;
            all_ok = false;
        } else if (actual != expected) {
            entry["ok"] = false;
            entry["problem"] = "checksum mismatch — the file changed since it was "
                               "materialized (truncated, rewritten or corrupted)";
            all_ok = false;
        } else {
            entry["ok"] = true;
        }
        report.push_back(std::move(entry));
    };

    for (const auto& data : artifacts.value("data", nlohmann::json::array())) {
        verify(data.value("path", std::string{}),
               data.value("sha256", std::string{}));
    }
    if (artifacts.contains("markers")) {
        verify(artifacts.value("markers", std::string{}),
               artifacts.value("markers_sha256", std::string{}));
    }
    if (report.empty()) {
        all_ok = false;
    }

    nlohmann::json response;
    response["type"] = "experiment_instance_verification";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["instance_id"] = instance_id;
    response["ok"] = all_ok;
    response["artifacts"] = std::move(report);
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

void StreamViewerWebSocket::handleDeleteStreamGraph(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const auto graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "delete_stream_graph requires graph_id", request_id);
        return;
    }
    const bool force = json.value("force", false);

    std::string experiment_to_unbind;
    std::filesystem::path artifacts_to_remove;
    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error, request_id);
            return;
        }
        const auto stored = g_stream_graphs.find(graph_id);
        if (stored == g_stream_graphs.end()) {
            sendError(conn, "Unknown graph: " + graph_id, request_id);
            return;
        }
        // Deleting a graph out from under its running workers would leave them
        // orphaned with no record to stop them by.
        const auto runtime = g_stream_graph_runtime.find(graph_id);
        if (runtime != g_stream_graph_runtime.end() &&
            (runtime->second.runState == "running" ||
             runtime->second.runState == "starting" ||
             runtime->second.runState == "stalled")) {
            sendError(conn,
                      "Graph '" + graph_id + "' is " + runtime->second.runState +
                          "; stop it before deleting.",
                      request_id);
            return;
        }
        // A sealed instance is permanent BY DEFAULT. It is still deletable, but
        // only deliberately: `force` says the operator means to destroy recorded
        // history, not that they mis-clicked a board.
        if (stored->second.immutable && !force) {
            sendError(conn,
                      "Instance " + stored->second.instanceId +
                          " is an immutable recorded snapshot. Deleting it destroys "
                          "the recording permanently; pass force=true if that is "
                          "what you mean.",
                      request_id);
            return;
        }
        if (!stored->second.instanceId.empty() &&
            !stored->second.experimentId.empty()) {
            artifacts_to_remove = instanceArtifactDir(stored->second.experimentId,
                                                      stored->second.instanceId);
        }
        experiment_to_unbind = stored->second.experimentId;
        g_stream_graphs.erase(stored);
        g_stream_graph_runtime.erase(graph_id);
        if (!persistStreamGraphStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
    }

    // Only an instance owns a directory, and only its own: a live board shares
    // nothing, so this never touches another instance's artifacts.
    if (!artifacts_to_remove.empty()) {
        std::error_code remove_error;
        // Read-only artifacts still need clearing before removal.
        for (std::filesystem::recursive_directory_iterator
                 iterator(artifacts_to_remove, remove_error), end;
             !remove_error && iterator != end; ++iterator) {
            std::error_code permission_error;
            std::filesystem::permissions(
                iterator->path(), std::filesystem::perms::owner_all,
                std::filesystem::perm_options::add, permission_error);
        }
        remove_error.clear();
        const auto removed = std::filesystem::remove_all(artifacts_to_remove,
                                                         remove_error);
        if (remove_error) {
            LOG_ERROR << "Deleted graph " << graph_id
                      << " but could not remove its artifacts at "
                      << artifacts_to_remove << ": " << remove_error.message();
        } else if (removed > 0) {
            LOG_INFO << "Removed " << removed << " artifact file(s) for " << graph_id;
        }
        // Prune the now-empty per-experiment parent so the store doesn't accrete a
        // directory per experiment forever. remove() only succeeds on an empty
        // directory, so a sibling instance's artifacts are never at risk.
        std::error_code prune_error;
        std::filesystem::remove(artifacts_to_remove.parent_path(), prune_error);
    }

    // A live board that an experiment pointed at must not leave the experiment
    // bound to a graph that no longer exists.
    if (!experiment_to_unbind.empty()) {
        std::lock_guard<std::mutex> lock(g_experiment_mutex);
        ensureExperimentStoreLoadedLocked();
        const auto experiment = g_experiments.find(experiment_to_unbind);
        if (experiment != g_experiments.end() &&
            experiment->second.liveGraphId == graph_id) {
            experiment->second.liveGraphId.clear();
            experiment->second.updatedAtUs = nowUs();
            std::string experiment_persist_error;
            if (!persistExperimentStoreLocked(experiment_persist_error)) {
                LOG_ERROR << "Failed to unbind experiment after graph delete: "
                          << experiment_persist_error;
            }
        }
    }

    nlohmann::json response;
    response["type"] = "stream_graph_deleted";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

// --- device commands (EXECUTION_COMMAND / LOGGING_LOG) --------------------

namespace {

// Commands and their answers are plain JSON on topics named for the schema; no
// C++ schema class is involved, because the only thing that decodes these
// payloads is a human reading a log line.
constexpr const char* kCommandSchemaName = "NatExecutionCommandV1";
constexpr const char* kLogSchemaName = "NatLogV1";

std::string deviceCommandTopic(uint64_t stream_id) {
    return "Command-" + std::to_string(stream_id) + "-Json-" + kCommandSchemaName;
}

std::string deviceLogTopic(uint64_t stream_id) {
    return "Log-" + std::to_string(stream_id) + "-Json-" + kLogSchemaName;
}

std::string makeCommandId() {
    const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    std::ostringstream out;
    out << "cmd-" << std::hex << now_us << "-" << std::hex
        << (static_cast<uint32_t>(std::random_device{}()) & 0xffffffu);
    return out.str();
}

}  // namespace

void StreamViewerWebSocket::handleSendDeviceCommand(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available", request_id);
        return;
    }

    // stream_id arrives as a string from the browser (it exceeds 2^53) but a
    // number from scripts, so accept both.
    uint64_t stream_id = 0;
    try {
        const auto& raw = json.at("stream_id");
        stream_id = raw.is_string() ? std::stoull(raw.get<std::string>())
                                    : raw.get<uint64_t>();
    } catch (const std::exception&) {
        sendError(conn, "send_device_command requires a numeric stream_id",
                  request_id);
        return;
    }
    if (stream_id == 0) {
        sendError(conn, "send_device_command requires a numeric stream_id",
                  request_id);
        return;
    }

    const auto command = json.value("command", std::string{});
    if (command.empty()) {
        sendError(conn, "send_device_command requires a command", request_id);
        return;
    }
    // The device's command buffer is 40 bytes; refusing here beats a payload the
    // device silently truncates.
    if (command.size() > 32) {
        sendError(conn, "Command name is too long (32 characters max)", request_id);
        return;
    }

    // ⚠️ REFUSED WHILE A RECORDING IS RUNNING, for commands that change what a
    // node collects. Parquet has one column set per file, so reconfiguring a
    // sensor mid-recording changes a file's schema halfway through and nothing
    // downstream can express that (TEC-NATKIT-40).
    //
    // Checked HERE rather than on the device or in the browser: a leaf cannot
    // know whether anything is recording it, and a check in the frontend is a
    // suggestion -- the same command can be published straight to the broker.
    //
    // The list is specific rather than blanket. Refusing every command during a
    // recording would also refuse the diagnostic ones, and "why has this node
    // gone quiet mid-session" is exactly when you want to ask it something.
    static const std::set<std::string> kCommandsBlockedWhileRecording = {
        "set_reports",
    };
    if (kCommandsBlockedWhileRecording.count(command) != 0 &&
        nat::tools::isRecordingActive()) {
        sendError(conn,
                  "Cannot change the sensor configuration while a recording is "
                  "in progress: it would change the recording's schema halfway "
                  "through. Stop the recording first.",
                  request_id);
        return;
    }

    // Which side executes it. "sensor" is the common case; "server" is here so
    // the same topic can carry the other direction later.
    const auto target = json.value("target", std::string{"sensor"});
    if (target != "sensor" && target != "server") {
        sendError(conn, "target must be \"sensor\" or \"server\"", request_id);
        return;
    }

    // How long to wait for the device to answer. A calibration save takes tens of
    // milliseconds, but the device only executes between sample batches and the
    // Kafka round trip adds a hop each way.
    int64_t timeout_ms = json.value("timeout_ms", static_cast<int64_t>(8000));
    timeout_ms = std::clamp<int64_t>(timeout_ms, 500, 30000);

    nlohmann::json command_json;
    command_json["schema_version"] = "nat.execution.command.v1";
    command_json["command_id"] = makeCommandId();
    command_json["source"] = "server";
    command_json["target"] = target;
    command_json["command"] = command;
    if (json.contains("args") && json.at("args").is_object()) {
        command_json["args"] = json.at("args");
    }

    std::thread([this, conn, request_id, stream_id, command, timeout_ms,
                 command_json]() {
        const auto command_id = command_json.value("command_id", std::string{});
        const auto command_topic = deviceCommandTopic(stream_id);
        const auto log_topic = deviceLogTopic(stream_id);

        const auto sendFailure = [&](const std::string& message) {
            nlohmann::json response;
            response["type"] = "device_command_result";
            response["request_id"] = request_id;
            response["stream_id"] = std::to_string(stream_id);
            response["command"] = command;
            response["command_id"] = command_id;
            response["ok"] = false;
            response["timed_out"] = false;
            response["error"] = message;
            response["records"] = nlohmann::json::array();
            if (conn && conn->connected()) {
                conn->send(response.dump());
            }
        };

        try {
            const auto command_info_maybe =
                nat::core::BasicTopicInformation::create(command_topic);
            const auto log_info_maybe =
                nat::core::BasicTopicInformation::create(log_topic);
            if (!command_info_maybe.has_value() || !log_info_maybe.has_value()) {
                sendFailure("Could not build the device's command/log topics");
                return;
            }

            // Subscribe to the answer BEFORE sending, or a device that answers
            // quickly answers into a topic nobody is reading yet.
            const auto log_messenger = broker_manager_->createMessenger(
                std::make_shared<nat::core::BasicTopicInformation>(
                    *log_info_maybe.value()));
            if (!log_messenger) {
                sendFailure("Could not consume the device's log topic");
                return;
            }

            // The bridge only forwards Kafka topics it has a messenger for, and it
            // discovers new ones on a 1 s poll. On a device's FIRST command the
            // command topic does not exist yet: producing immediately would write
            // at offset 0, the bridge would attach at OFFSET_END afterwards, and
            // that first command would be silently dropped. So create the topic,
            // give the bridge a poll cycle to attach to an empty topic, and only
            // then produce.
            const auto existing = broker_manager_->getAllTopicStrings();
            const bool command_topic_existed =
                std::find(existing.begin(), existing.end(), command_topic) !=
                existing.end();

            const auto command_messenger = broker_manager_->createMessenger(
                std::make_shared<nat::core::BasicTopicInformation>(
                    *command_info_maybe.value()));
            if (!command_messenger) {
                sendFailure("Could not produce to the device's command topic");
                return;
            }
            if (!command_topic_existed) {
                LOG_INFO << "Command topic " << command_topic
                         << " is new; waiting for the bridge to attach before "
                            "sending";
                std::this_thread::sleep_for(std::chrono::milliseconds(2500));
            }

            const auto payload = command_json.dump();
            nat::core::message_t message(payload.begin(), payload.end());
            command_messenger->sendRawMessage(message);
            command_messenger->flush();
            LOG_INFO << "Sent device command " << command << " (" << command_id
                     << ") to " << command_topic;

            // Collect every log record carrying our command_id until one says it is
            // the last, so a multi-step command can report progress.
            nlohmann::json records = nlohmann::json::array();
            bool ok = false;
            bool terminal = false;
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(timeout_ms);
            while (std::chrono::steady_clock::now() < deadline && !terminal) {
                auto next = log_messenger->tryGetNextRawMessage();
                if (!next.has_value()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(25));
                    continue;
                }
                const auto& bytes = *next.value();
                std::string text(bytes.begin(), bytes.end());
                nlohmann::json record;
                try {
                    record = nlohmann::json::parse(text);
                } catch (const std::exception&) {
                    continue;  // not one of ours; the log topic is shared
                }
                if (record.value("command_id", std::string{}) != command_id) {
                    continue;
                }
                records.push_back(record);
                ok = record.value("ok", false);
                terminal = record.value("terminal", false);
            }

            nlohmann::json response;
            response["type"] = "device_command_result";
            response["request_id"] = request_id;
            response["stream_id"] = std::to_string(stream_id);
            response["command"] = command;
            response["command_id"] = command_id;
            response["records"] = records;
            response["timed_out"] = !terminal;
            response["ok"] = terminal && ok;
            if (!terminal) {
                // Distinguish "the device said no" from "the device said nothing":
                // the second usually means it is offline or on old firmware.
                response["error"] =
                    records.empty()
                        ? "The device did not answer. It may be offline, or its "
                          "firmware may predate the command channel."
                        : "The device stopped answering before the command "
                          "finished.";
            } else if (!ok && !records.empty()) {
                response["error"] =
                    records.back().value("message", std::string{"Command failed"});
            }
            if (conn && conn->connected()) {
                conn->send(response.dump());
            }
        } catch (const std::exception& e) {
            sendFailure(std::string("Failed to send device command: ") + e.what());
        }
    }).detach();
}


// --- device health (TEC-NATKIT-33) ----------------------------------------
//
// The hub publishes its own health and every leaf's once a second on
// `Log-<id>-Binary-NatKitPrimaryStatusV1` / `-NatKitNodeStatusV1`. Those are
// LOGGING_LOG topics, which sendStreamList deliberately does not carry -- they
// are not data a graph can consume -- so a client has no way to reach them
// through subscribe. This is the way in.
//
// One thread per client, tailing every status topic on the broker. That is more
// consumers than strictly necessary if several browsers watch at once, but it
// keeps each client's differencing history its own: a second browser connecting
// must not reset the first one's rates, and a shared tracker would do exactly
// that on every reconnect.

namespace {


// How long without a frame before a device is shown as quiet. The hardware
// publishes at 1 Hz, so 3 s is three missed frames -- long enough that a single
// dropped publish or a GC pause does not flap the indicator, short enough to
// notice a board that fell off the rig while somebody is looking at the panel.
constexpr uint64_t kDefaultQuietAfterMs = 3000;
constexpr uint64_t kDefaultIntervalMs = 1000;


uint64_t nowMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

}  // namespace

void StreamViewerWebSocket::handleSubscribeDeviceHealth(
    const WebSocketConnectionPtr& conn, const nlohmann::json& json)
{
    StreamViewerClientContext* ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(conn);
        if (it == clients_.end()) {
            return;
        }
        ctx = it->second.get();
    }
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }
    // Already running: a repeated subscribe (a re-render, a reconnect that raced
    // the close) must not start a second thread pushing to the same socket.
    if (ctx->device_health_active.exchange(true)) {
        return;
    }

    const uint64_t interval_ms = json.value("interval_ms", kDefaultIntervalMs);
    const uint64_t quiet_after_ms = json.value("quiet_after_ms", kDefaultQuietAfterMs);

    if (ctx->device_health_thread.joinable()) {
        ctx->device_health_thread.join();
    }
    ctx->device_health_thread = std::thread(
        &StreamViewerWebSocket::deviceHealthThreadFunc, this, conn, ctx,
        std::max<uint64_t>(250, interval_ms), quiet_after_ms);
}

void StreamViewerWebSocket::handleUnsubscribeDeviceHealth(
    const WebSocketConnectionPtr& conn)
{
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(conn);
    if (it == clients_.end()) {
        return;
    }
    // Just clear the flag. The thread notices within one interval and returns;
    // joining here would block the WS thread for up to that long, and the
    // context's destructor joins anyway.
    it->second->device_health_active = false;
}

void StreamViewerWebSocket::deviceHealthThreadFunc(
    const WebSocketConnectionPtr& conn, StreamViewerClientContext* ctx,
    const uint64_t interval_ms, const uint64_t quiet_after_ms)
{
    // Reads the SHARED tailer rather than opening its own consumers
    // (TEC-NATKIT-77). Two reasons, and the second is the load-bearing one:
    // N browsers no longer mean N Kafka consumers on the same five topics; and
    // more importantly a RECORDING can now ask the same question, which it could
    // not when the only tailer lived inside a per-connection thread — the answer
    // would have depended on whether anybody happened to have the panel open.
    auto& service = nat::tools::DeviceHealthService::instance();
    service.start(broker_manager_);

    LOG_INFO << "Device health: pushing to a client (interval " << interval_ms
             << " ms, quiet after " << quiet_after_ms << " ms)";

    while (ctx->active && ctx->device_health_active && conn->connected()) {
        nlohmann::json response;
        response["type"] = "device_health";
        response["wall_ms"] = nowMs();
        response["quiet_after_ms"] = quiet_after_ms;
        // Whether we are even watching, distinct from having nothing to report.
        // Zero topics means the rig has never published; a positive count with no
        // devices means every board that used to report has stopped.
        response["topics_tailed"] = service.topicsTailed();
        nlohmann::json devices = nlohmann::json::array();
        for (const auto& health : service.snapshot()) {
            devices.push_back(health.toJson(quiet_after_ms));
        }
        response["devices"] = std::move(devices);

        if (conn->connected()) {
            conn->send(response.dump());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }

    ctx->device_health_active = false;
    LOG_INFO << "Device health: stopped pushing to a client";
}

// --- the log viewer (TEC-NATKIT-33, second half) ---------------------------
//
// Devices publish free-form log lines on `Log-<id>-Json-NatLogV1` and structured
// health on `Log-<id>-Binary-NatKit*StatusV1`. Both are LOGGING_LOG topics, and
// until now the only way to read either was to attach a consumer by hand.
//
// This tails whichever of them the client asks for and forwards each record. It
// is deliberately GENERIC -- it resolves a topic through the schema registry
// rather than knowing schema names -- because the point is that a log added
// tomorrow is readable without a frontend change. Three cases, in order:
//
//   1. a registered decoder exists  -> decode and send its JSON, plus the
//      schema's own timestamp, which is better than arrival time.
//   2. no decoder but the topic says Json -> pass the text through. THIS IS THE
//      COMMON CASE: NatLogV1 has no schema class at all, it is free-form JSON
//      that the firmware and the command path already agree on informally.
//   3. neither -> report the record as an undecodable byte count.
//
// ⚠️ Case 3 must not silently drop. A log viewer that shows nothing for a topic
// it cannot parse is indistinguishable from a device that is not logging, and
// the second is what everybody will conclude.

namespace {

constexpr size_t kMaxLogRecordsPerPush = 200;
constexpr uint64_t kDefaultLogIntervalMs = 500;
// A single record's text, truncated. A device that logs a megabyte in one line
// should not be able to wedge a browser tab, and the truncation is reported.
constexpr size_t kMaxLogTextBytes = 8192;

bool isLogTopic(const nat::core::BasicTopicInformation& topic) {
    return topic.type == nat::core::StreamType::LOGGING_LOG;
}

}  // namespace

void StreamViewerWebSocket::handleListLogStreams(
    const WebSocketConnectionPtr& conn, const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available", request_id);
        return;
    }

    nlohmann::json response;
    response["type"] = "log_stream_list";
    response["request_id"] = request_id;
    nlohmann::json topics = nlohmann::json::array();

    auto& registry = nat::core::DataSchemaDescriptorRegistry::getDefault();
    for (const auto& topic : broker_manager_->getAllTopics()) {
        if (!topic || !isLogTopic(*topic)) {
            continue;
        }
        nlohmann::json entry;
        entry["topic"] = topic->toTopicString();
        // A string: these ids exceed 2^53 and JavaScript would round them.
        entry["stream_id"] = std::to_string(topic->id);
        entry["schema_name"] = topic->schemaName;
        entry["serialization_type"] = nat::core::toString(topic->serializationType);
        // Whether this topic can be rendered as structured fields or only as
        // text, so the frontend can say which rather than discovering it per
        // record. A descriptor is what makes field labels possible.
        const auto descriptor = registry.findBySchemaName(topic->schemaName);
        entry["has_descriptor"] = descriptor.has_value();
        const auto descriptor_json = getDescriptorJsonForSchemaName(topic->schemaName);
        if (descriptor_json.has_value()) {
            entry["descriptor"] = descriptor_json.value();
        }
        topics.push_back(std::move(entry));
    }
    response["topics"] = std::move(topics);
    conn->send(response.dump());
}

void StreamViewerWebSocket::handleSubscribeLogs(
    const WebSocketConnectionPtr& conn, const nlohmann::json& json)
{
    StreamViewerClientContext* ctx = nullptr;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(conn);
        if (it == clients_.end()) {
            return;
        }
        ctx = it->second.get();
    }
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    std::set<std::string> requested;
    if (json.contains("topics") && json["topics"].is_array()) {
        for (const auto& entry : json["topics"]) {
            if (entry.is_string()) {
                requested.insert(entry.get<std::string>());
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(ctx->log_topics_mutex);
        ctx->log_topics = requested;
    }

    // An empty set means "stop", which is what unchecking the last topic does.
    // Treating it as "tail everything" would turn a deselection into a flood.
    if (requested.empty()) {
        ctx->log_tail_active = false;
        return;
    }

    // Already running: the set has been swapped above and the thread re-reads it
    // each tick, so a client toggling a checkbox keeps its place in every topic
    // it was already reading rather than restarting at the live end.
    if (ctx->log_tail_active.exchange(true)) {
        return;
    }

    const uint64_t interval_ms = json.value("interval_ms", kDefaultLogIntervalMs);
    if (ctx->log_tail_thread.joinable()) {
        ctx->log_tail_thread.join();
    }
    ctx->log_tail_thread = std::thread(&StreamViewerWebSocket::logTailThreadFunc,
                                       this, conn, ctx,
                                       std::max<uint64_t>(100, interval_ms));
}

void StreamViewerWebSocket::handleUnsubscribeLogs(const WebSocketConnectionPtr& conn)
{
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(conn);
    if (it == clients_.end()) {
        return;
    }
    it->second->log_tail_active = false;
}

void StreamViewerWebSocket::logTailThreadFunc(const WebSocketConnectionPtr& conn,
                                              StreamViewerClientContext* ctx,
                                              const uint64_t interval_ms)
{
    // Messengers are kept per topic for the life of the subscription: rebuilding
    // one re-attaches at the live end and loses whatever arrived in between.
    // Deliberately NOT torn down when a topic is deselected, so re-checking it
    // resumes rather than skipping the gap.
    std::map<std::string, std::unique_ptr<nat::core::TopicMessenger>> messengers;
    std::map<std::string, std::shared_ptr<nat::core::BasicTopicInformation>> infos;
    auto registry = broker_manager_->getRegistry();

    LOG_INFO << "Log tail: started for a client";

    while (ctx->active && ctx->log_tail_active && conn->connected()) {
        std::set<std::string> wanted;
        {
            std::lock_guard<std::mutex> lock(ctx->log_topics_mutex);
            wanted = ctx->log_topics;
        }

        for (const auto& name : wanted) {
            if (messengers.count(name) > 0) {
                continue;
            }
            const auto info_maybe = nat::core::BasicTopicInformation::create(name);
            if (!info_maybe.has_value()) {
                // A name the client made up, or a topic string this build cannot
                // parse. Said out loud rather than ignored: silence here reads as
                // "that topic has no logs".
                nlohmann::json problem;
                problem["type"] = "log_records";
                problem["records"] = nlohmann::json::array();
                problem["error"] = "Not a topic this backend can parse: " + name;
                if (conn->connected()) {
                    conn->send(problem.dump());
                }
                continue;
            }
            auto info = std::make_shared<nat::core::BasicTopicInformation>(
                *info_maybe.value());
            auto messenger = broker_manager_->createMessenger(info);
            if (!messenger) {
                LOG_WARN << "Log tail: could not consume " << name;
                continue;
            }
            LOG_INFO << "Log tail: tailing " << name;
            infos[name] = info;
            messengers[name] = std::move(messenger);
        }

        nlohmann::json records = nlohmann::json::array();
        size_t dropped = 0;

        for (auto& entry : messengers) {
            if (wanted.count(entry.first) == 0) {
                // Not selected right now. Its messenger stays alive (see above),
                // but its records are drained and discarded rather than queued
                // forever -- an unbounded queue behind a deselected checkbox is a
                // leak that only shows up hours later.
                while (entry.second->tryGetNextRawMessage().has_value()) {
                }
                continue;
            }
            const auto& info = infos[entry.first];
            for (size_t drained = 0; drained < kMaxLogRecordsPerPush * 2; ++drained) {
                if (records.size() >= kMaxLogRecordsPerPush) {
                    // Count what we walked away from rather than pretending the
                    // burst did not happen.
                    if (entry.second->tryGetNextRawMessage().has_value()) {
                        ++dropped;
                        continue;
                    }
                    break;
                }
                auto next = entry.second->tryGetNextRawMessage();
                if (!next.has_value()) {
                    break;
                }
                const auto& bytes = *next.value();

                nlohmann::json record;
                record["topic"] = entry.first;
                record["stream_id"] = std::to_string(info->id);
                record["schema_name"] = info->schemaName;
                record["received_ms"] = nowMs();
                record["bytes"] = bytes.size();

                bool rendered = false;

                // 1. A registered decoder: structured fields, and the schema's
                //    own timestamp rather than arrival time.
                if (registry) {
                    nat::core::TopicTranslator translator(info, registry);
                    auto decoded = translator.tryDecodeMessage(bytes);
                    if (decoded.has_value() && decoded.value()) {
                        const auto& schema = *decoded.value();
                        try {
                            record["json"] = nlohmann::json::parse(schema.toString());
                        } catch (const std::exception&) {
                            record["text"] = schema.toString();
                        }
                        record["device_ts_us"] = schema.getTimestampUs();
                        record["decoded"] = "schema";
                        rendered = true;
                    }
                }

                // 2. No decoder, but the topic says Json. NatLogV1 lives here:
                //    it has no schema class, only a shape the firmware and the
                //    command path agree on.
                if (!rendered &&
                    info->serializationType == nat::core::SerializationType::Json) {
                    std::string text(bytes.begin(), bytes.end());
                    bool truncated = false;
                    if (text.size() > kMaxLogTextBytes) {
                        text.resize(kMaxLogTextBytes);
                        truncated = true;
                    }
                    try {
                        record["json"] = nlohmann::json::parse(text);
                        record["decoded"] = "json";
                    } catch (const std::exception&) {
                        record["text"] = text;
                        record["decoded"] = "text";
                    }
                    if (truncated) {
                        record["truncated"] = true;
                    }
                    rendered = true;
                }

                // 3. Neither. Reported, NOT dropped -- see the note at the top.
                if (!rendered) {
                    record["decoded"] = "none";
                }
                records.push_back(std::move(record));
            }
        }

        if (!records.empty() || dropped > 0) {
            nlohmann::json response;
            response["type"] = "log_records";
            response["records"] = std::move(records);
            response["dropped"] = dropped;
            if (conn->connected()) {
                conn->send(response.dump());
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }

    ctx->log_tail_active = false;
    LOG_INFO << "Log tail: stopped for a client";
}

void StreamViewerWebSocket::handleListExperiments(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    {
        std::lock_guard<std::mutex> lock(g_experiment_mutex);
        ensureExperimentStoreLoadedLocked();
        if (!g_experiment_store_error.empty()) {
            sendError(conn, g_experiment_store_error, request_id);
            return;
        }
        // Hung off the first experiment listing rather than startup: this is the
        // earliest point where BOTH stores are loaded and the experiment mutex is
        // already held, which is the half of the lock order that has to come
        // first. Once per process.
        static std::once_flag backfill_once;
        std::call_once(backfill_once, backfillInstanceParticipantsLocked);
    }
    sendExperimentList(conn, request_id);
}

void StreamViewerWebSocket::handleSaveExperiment(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    if (!json.contains("experiment")) {
        sendError(conn, "save_experiment requires an experiment payload", request_id);
        return;
    }

    Experiment experiment;
    try {
        experiment = json.at("experiment").get<Experiment>();
    } catch (const std::exception& exception) {
        sendError(
            conn,
            std::string("Failed to parse experiment payload: ") + exception.what(),
            request_id);
        return;
    }
    // The experiment_id is a Kafka topic key: markers publish to and the
    // `markers` node subscribes to Marker/<experiment_id>, so an id that can't
    // be a topic identifier would produce an experiment that silently records
    // nowhere.
    if (!isValidTopicIdentifier(experiment.experimentId)) {
        sendError(
            conn,
            "save_experiment requires an experiment_id matching "
            "^[A-Za-z0-9][A-Za-z0-9_-]*$ (it keys the Marker/<experiment_id> topic)",
            request_id);
        return;
    }

    if (experiment.createdAtUs == 0) {
        experiment.createdAtUs = nowUs();
    }
    experiment.updatedAtUs = nowUs();

    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_experiment_mutex);
        ensureExperimentStoreLoadedLocked();
        if (!g_experiment_store_error.empty()) {
            sendError(conn, g_experiment_store_error, request_id);
            return;
        }
        // Preserve the original creation timestamp on update.
        const auto existing = g_experiments.find(experiment.experimentId);
        if (existing != g_experiments.end() && existing->second.createdAtUs != 0) {
            experiment.createdAtUs = existing->second.createdAtUs;
        }
        // Bind the board FIRST: if the graph side is rejected (unknown graph, or
        // an immutable instance) the experiment record is left untouched rather
        // than saved with a binding that was refused.
        if (!applyExperimentGraphBindingLocked(
                experiment.experimentId, experiment.liveGraphId,
                experiment.workspaceId, persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
        g_experiments[experiment.experimentId] = experiment;
        if (!persistExperimentStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
    }

    sendExperimentSaved(conn, request_id, experiment);
}

void StreamViewerWebSocket::handleDeleteExperiment(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const std::string experiment_id = json.value("experiment_id", std::string{});
    if (experiment_id.empty()) {
        sendError(conn, "delete_experiment requires an experiment_id", request_id);
        return;
    }

    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_experiment_mutex);
        ensureExperimentStoreLoadedLocked();
        if (!g_experiment_store_error.empty()) {
            sendError(conn, g_experiment_store_error, request_id);
            return;
        }
        if (g_experiments.find(experiment_id) == g_experiments.end()) {
            sendError(conn, "No experiment found for experiment_id " + experiment_id,
                      request_id);
            return;
        }
        // An experiment with history can't be deleted: its instances are
        // immutable snapshots that are only reachable through it, so removing
        // the experiment would orphan them.
        {
            std::lock_guard<std::mutex> graph_lock(g_stream_graph_mutex);
            ensureStreamGraphStoreLoadedLocked();
            if (!g_stream_graph_store_error.empty()) {
                sendError(conn, g_stream_graph_store_error, request_id);
                return;
            }
            std::size_t instance_count = 0;
            for (const auto& entry : g_stream_graphs) {
                if (entry.second.experimentId == experiment_id &&
                    !entry.second.instanceId.empty()) {
                    ++instance_count;
                }
            }
            if (instance_count > 0) {
                sendError(
                    conn,
                    "Experiment '" + experiment_id + "' has " +
                        std::to_string(instance_count) +
                        " recorded instance(s); they are permanent history and "
                        "would be orphaned. Delete the instances first.",
                    request_id);
                return;
            }
        }
        // Unbind the live board before dropping the record, so no graph is left
        // pointing at an experiment that no longer exists.
        // Empty workspace on the unbind path: there is no bound board left to
        // stamp, so the argument is unused -- but it must not read as "file the
        // board into workspace X" either.
        if (!applyExperimentGraphBindingLocked(experiment_id, std::string{},
                                               std::string{}, persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
        g_experiments.erase(experiment_id);
        if (!persistExperimentStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
    }

    sendExperimentDeleted(conn, request_id, experiment_id);
}

void StreamViewerWebSocket::handleValidateStreamGraph(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    if (!json.contains("graph")) {
        sendError(conn, "validate_stream_graph requires a graph payload");
        return;
    }

    try {
        StreamGraphDefinition graph = json.at("graph").get<StreamGraphDefinition>();
        for (auto& node : graph.nodes) {
            normalizeGraphNodePorts(node);
        }
        const auto validation =
            validateStreamGraphDefinition(graph, broker_manager_);
        sendStreamGraphValidation(
            conn,
            json.value("request_id", std::string{}),
            graph.graphId,
            validation.valid,
            validation.graphDiagnostics,
            validation.nodeDiagnostics,
            validation.edgeDiagnostics);
    } catch (const std::exception& exception) {
        sendError(
            conn,
            std::string("Failed to parse stream graph payload: ") + exception.what());
    }
}

void StreamViewerWebSocket::handleGetStreamGraphStatus(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "get_stream_graph_status requires graph_id");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error);
            return;
        }
        if (g_stream_graphs.find(graph_id) == g_stream_graphs.end()) {
            sendError(conn, "No saved graph exists for graph_id");
            return;
        }
    }

    sendStreamGraphStatus(
        conn,
        json.value("request_id", std::string{}),
        graph_id);
}

namespace {

// Push helpers usable from the background start thread (the member send*
// methods are only reachable with `this`, and this free-function form lets the
// detached worker report progress). Both no-op if the client has gone away.
void pushStreamGraphStatusMessage(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& graph_id)
{
    if (!conn || !conn->connected()) {
        return;
    }
    nlohmann::json response;
    response["type"] = "stream_graph_status";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["status"] = nlohmann::json::object();
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto search = g_stream_graphs.find(graph_id);
        if (search != g_stream_graphs.end()) {
            response["status"] = makeGraphStatusJson(search->second);
        }
    }
    conn->send(response.dump());
}

void pushStreamGraphStartedMessage(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& graph_id)
{
    if (!conn || !conn->connected()) {
        return;
    }
    nlohmann::json response;
    response["type"] = "stream_graph_started";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["graph_run_id"] = nullptr;
    response["node_statuses"] = nlohmann::json::object();
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto runtime_search = g_stream_graph_runtime.find(graph_id);
        if (runtime_search != g_stream_graph_runtime.end()) {
            response["graph_run_id"] = runtime_search->second.activeRunId;
            for (const auto& entry : runtime_search->second.nodeStatuses) {
                response["node_statuses"][entry.first] = entry.second;
            }
        }
    }
    conn->send(response.dump());
}

// Brings a validated graph up node-by-node on a background thread. Each
// transform/combine node creates Kafka topics + a worker, which is slow, so the
// caller runs this detached after seeding a "starting" runtime; here we stream a
// stream_graph_status after every node so the client watches progress instead of
// staring at a frozen UI. activeRunId is the generation token: if the user stops
// the graph or starts a fresh run mid-flight, this run is cancelled and any
// workers it already created are torn back down.
// Global kill-switch for the in-process fast path. In-process is the default
// transport for private colocated edges (it also cuts startup latency — an
// internal edge skips Kafka messenger creation); a deployment can force every
// edge back onto Kafka by setting NATKIT_INPROCESS_TRANSPORT to 0/off/false/no.
bool inProcessTransportEnabled()
{
    const char* value = std::getenv("NATKIT_INPROCESS_TRANSPORT");
    if (value == nullptr) {
        return true;
    }
    const std::string setting = nat::core::Strings::toLowercase(value);
    return !(setting == "0" || setting == "off" || setting == "false" ||
             setting == "no");
}

// Adapt the runtime graph to the pure transport classifier. Returns the set of
// producer node ids whose output edge may travel in-process.
std::unordered_set<std::string> classifyGraphPrivateOutputs(
    const StreamGraphDefinition& graph)
{
    if (!inProcessTransportEnabled()) {
        return {};
    }
    std::vector<nat::tools::TransportGraphNode> nodes;
    nodes.reserve(graph.nodes.size());
    for (const auto& node : graph.nodes) {
        nodes.push_back({node.id, node.kind});
    }
    std::vector<nat::tools::TransportGraphEdge> edges;
    edges.reserve(graph.edges.size());
    for (const auto& edge : graph.edges) {
        edges.push_back({edge.sourceNodeId, edge.targetNodeId});
    }
    // Colocation is trivially true today — every graph node shares the backend
    // process — so the default proof accepts all edges. When ADR 001 Phase 4
    // places transforms on independent worker slots, swap in a scheduler proof
    // here and cross-slot edges fall back to Kafka with no other change.
    return nat::tools::classifyPrivateOutputs(nodes, edges);
}

// Part A (topic-aware channels): build a node's output channel — the topic set it
// carries. `channelTopicsFromWorker` reads the just-created transform/combine
// worker's output topic (its real StreamType + schema).
std::vector<StreamGraphOutputTopic> channelTopicsFromWorker(uint64_t output_stream_id)
{
    std::vector<StreamGraphOutputTopic> topics{};
    const auto info = findGraphInternalOutputTopicForStream(output_stream_id);
    if (info != nullptr) {
        topics.push_back(
            makeChannelTopic(info->type, info->id, info->schemaName));
    }
    // A combine "stream" output also carries a MARKER topic sharing the same id.
    const auto marker_info =
        findGraphInternalMarkerTopicForStream(output_stream_id);
    if (marker_info != nullptr) {
        topics.push_back(makeChannelTopic(
            marker_info->type, marker_info->id, marker_info->schemaName));
    }
    return topics;
}

void executeStreamGraphStart(
    std::shared_ptr<nat::kafka::BrokerManager> broker_manager,
    WebSocketConnectionPtr conn,
    std::string request_id,
    StreamGraphDefinition graph,
    std::string active_run_id,
    int64_t replay_start_offset = -1,
    // Non-zero when this run is bound to a replay: the replay republishes the
    // recording's marker timeline onto its own scratch MARKER topic, so a markers
    // node must resolve to THAT instead of the experiment's live marker topic.
    uint64_t replay_marker_stream_id = 0)
{
    // Provenance edges carry lineage/control, not streaming data — drop them from
    // the executed graph entirely (mirrors flattenGraph dropping param nodes) so
    // every downstream edge iteration, transport classification, and input
    // resolution sees only real data edges. (`graph` is a local copy.)
    graph.edges.erase(
        std::remove_if(
            graph.edges.begin(),
            graph.edges.end(),
            [](const StreamGraphEdge& edge) { return isProvenanceEdge(edge); }),
        graph.edges.end());

    std::unordered_map<std::string, const StreamGraphNode*> nodes_by_id;
    std::unordered_map<std::string, StreamGraphNode*> mutable_nodes_by_id;
    std::unordered_map<std::string, uint64_t> resolved_output_stream_ids;
    // Part A: the full output channel (topic set) each node exposes, so a
    // downstream combine can merge per-type (data + markers) instead of assuming
    // one DATA topic. Keyed by node id; parallels resolved_output_stream_ids.
    std::unordered_map<std::string, std::vector<StreamGraphOutputTopic>>
        resolved_output_channels;
    // Root Kafka sources (stream_source nodes). On a replay run (replay_start_
    // offset >= 0), only these seek to the historical offset; downstream inputs
    // are graph-internal topics fed live by the re-run chain and stay live-tail.
    // (Phase 5 — aligned start.)
    std::set<uint64_t> root_source_stream_ids;
    for (auto& node : graph.nodes) {
        nodes_by_id[node.id] = &node;
        mutable_nodes_by_id[node.id] = &node;
        if (node.kind == "stream_source" && node.streamId.has_value()) {
            resolved_output_stream_ids[node.id] = node.streamId.value();
            // A raw source is a data-only channel; its schema is resolved later
            // from the live topic (left empty here — combine discovers it).
            resolved_output_channels[node.id] = {makeChannelTopic(
                nat::core::StreamType::DATA, node.streamId.value(),
                std::string{})};
            root_source_stream_ids.insert(node.streamId.value());
        }
    }
    const auto sourceOffsetFor = [&](uint64_t input_stream_id) -> int64_t {
        return root_source_stream_ids.count(input_stream_id)
                   ? replay_start_offset
                   : -1;
    };

    // Transport classification (ADR 005): decide, per producer output, whether the
    // edge is private + colocated (in-process) or published (Kafka). Recomputed on
    // every run, so exposing a previously private edge to a viewer/sink/session/
    // train — or ungrouping a subgraph — promotes it back to Kafka automatically.
    const auto privateOutputs = classifyGraphPrivateOutputs(graph);
    const auto outputInProcess = [&](const std::string& node_id) {
        return privateOutputs.count(node_id) != 0;
    };

    struct ResolvedInput {
        std::optional<uint64_t> streamId;
        std::optional<std::string> upstreamNodeId;
    };

    const auto resolveInputStreamId = [&](const StreamGraphNode& node) -> ResolvedInput {
        const auto upstream_edge = std::find_if(
            graph.edges.begin(),
            graph.edges.end(),
            [&node](const StreamGraphEdge& edge) {
                return edge.targetNodeId == node.id &&
                       !isProvenanceEdge(edge);
            });
        if (upstream_edge == graph.edges.end()) {
            return ResolvedInput{std::nullopt, std::nullopt};
        }

        const auto upstream_node_search =
            nodes_by_id.find(upstream_edge->sourceNodeId);
        if (upstream_node_search == nodes_by_id.end()) {
            return ResolvedInput{std::nullopt, std::nullopt};
        }

        if (upstream_node_search->second->kind == "stream_source" &&
            upstream_node_search->second->streamId.has_value()) {
            return ResolvedInput{
                upstream_node_search->second->streamId.value(),
                upstream_edge->sourceNodeId};
        }

        const auto resolved_search =
            resolved_output_stream_ids.find(upstream_edge->sourceNodeId);
        if (resolved_search == resolved_output_stream_ids.end()) {
            return ResolvedInput{std::nullopt, upstream_edge->sourceNodeId};
        }
        return ResolvedInput{resolved_search->second, upstream_edge->sourceNodeId};
    };

    // True if this run has been superseded/stopped (checked without mutating).
    const auto runCancelled = [&]() -> bool {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto it = g_stream_graph_runtime.find(graph.graphId);
        return it == g_stream_graph_runtime.end() ||
               it->second.activeRunId != active_run_id ||
               it->second.runState == "stopped";
    };

    std::vector<uint64_t> created_output_ids;
    const auto abortCleanup = [&]() {
        for (uint64_t id : created_output_ids) {
            stopGraphWorkerByOutputStreamId(id);
        }
    };

    // Publishes a node's status into the shared runtime, but only while this run
    // is still the active one — the check and the write happen under the same
    // lock so a concurrent stop can't slip a leaked worker past us. Returns false
    // when the run was cancelled, signalling the caller to abort. new_output_id,
    // when set, is registered so a later stop tears that worker down.
    const auto commitNodeStatus =
        [&](const std::string& node_id,
            const StreamGraphNodeRuntimeStatus& status,
            std::optional<uint64_t> new_output_id) -> bool {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto it = g_stream_graph_runtime.find(graph.graphId);
        if (it == g_stream_graph_runtime.end() ||
            it->second.activeRunId != active_run_id ||
            it->second.runState == "stopped") {
            return false;
        }
        it->second.nodeStatuses[node_id] = status;
        if (new_output_id.has_value()) {
            it->second.outputStreamIds.push_back(new_output_id.value());
        }
        return true;
    };

    bool encountered_error = false;
    bool aborted = false;

    for (const auto& node_id : topologicallySortStreamGraph(graph)) {
        if (runCancelled()) {
            aborted = true;
            break;
        }
        const auto node_search = nodes_by_id.find(node_id);
        if (node_search == nodes_by_id.end()) {
            continue;
        }
        const auto& node = *node_search->second;

        // Sources were marked running when the runtime was seeded.
        if (node.kind == "stream_source") {
            continue;
        }

        if (node.kind == "viewer" || node.kind == "sink") {
            const auto resolved_input = resolveInputStreamId(node);
            StreamGraphNodeRuntimeStatus status;
            if (!resolved_input.streamId.has_value()) {
                encountered_error = true;
                status = StreamGraphNodeRuntimeStatus{
                    "blocked",
                    std::nullopt,
                    std::nullopt,
                    std::nullopt,
                    0,
                    0,
                    resolved_input.upstreamNodeId.has_value()
                        ? std::string("Blocked: upstream node ") +
                              resolved_input.upstreamNodeId.value() + " failed to start."
                        : std::string("Utility node input is not connected to a stream-producing node.")};
            } else {
                status = StreamGraphNodeRuntimeStatus{
                    "running",
                    resolved_input.streamId.value(),
                    std::nullopt,
                    std::nullopt,
                    0,
                    0,
                    node.kind == "viewer"
                        ? std::optional<std::string>("Viewer node is ready to inspect the upstream stream.")
                        : std::optional<std::string>("Sink node is attached to the upstream stream.")};
            }
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }
        if (isMarkerSourceKind(node.kind)) {
            // Recording is driven client-side (the browser runs the protocol
            // timeline and publishes the session bundle via
            // publish_session_bundle). The runtime just marks the node ready. A
            // markers node is source-like: it takes no inputs and its one output
            // port, `markers`, resolves to the deterministic MarkerEventV1 topic
            // Marker/<experiment_id> so downstream marker-aware nodes can
            // subscribe to the resolved output_stream_id (like a source).
            //
            // The id comes from the GRAPH's bound experiment. A legacy
            // `experiment` node's own config is the fallback, so a board that
            // predates the split still resolves its original marker topic and its
            // recorded data stays reachable.
            std::optional<uint64_t> markers_stream_id{};
            std::string experiment_id = graph.experimentId;
            if (experiment_id.empty() && node.config.is_object()) {
                experiment_id = node.config.value("experiment_id", std::string{});
            }
            if (isValidTopicIdentifier(experiment_id)) {
                const auto marker_topic_info = createTopicInfo(
                    nat::core::StreamType::MARKER,
                    "session_id",
                    experiment_id,
                    nat::core::MarkerEventV1::name);
                if (marker_topic_info != nullptr) {
                    markers_stream_id = marker_topic_info->id;
                }
            }
            // Replay-bound run: the recording's markers are being republished to
            // the replay's scratch MARKER topic, and nothing is writing to the
            // experiment's live marker topic. Point the node (and therefore every
            // marker-aware viewer downstream of it) at the replayed timeline —
            // otherwise the viewer subscribes to a topic that stays silent for the
            // whole replay and just reports "Waiting for data on this stream…".
            if (replay_marker_stream_id != 0) {
                markers_stream_id = replay_marker_stream_id;
            }
            StreamGraphNodeRuntimeStatus status{
                "running",
                markers_stream_id,
                std::nullopt,
                std::nullopt,
                0,
                0,
                markers_stream_id.has_value()
                    ? std::optional<std::string>(
                          "Republishing the marker timeline for experiment '" +
                          experiment_id + "'.")
                    : std::optional<std::string>(
                          "No experiment is bound to this board, so there is no "
                          "marker timeline to republish.")};
            if (markers_stream_id.has_value()) {
                status.outputTopics.push_back(makeChannelTopic(
                    nat::core::StreamType::MARKER, markers_stream_id.value(),
                    nat::core::MarkerEventV1::name));
            }
            // No worker to register for teardown (recording is client-side), so
            // pass nullopt as the created-output id. But DO publish the markers
            // output stream id into resolved_output_stream_ids + the node so a
            // downstream marker-aware node (e.g. a viewer on the `markers` port)
            // can resolve its input — without this it was wrongly "blocked:
            // upstream experiment failed to start."
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            if (markers_stream_id.has_value()) {
                resolved_output_stream_ids[node.id] = markers_stream_id.value();
                resolved_output_channels[node.id] = status.outputTopics;
                const auto mutable_node_search = mutable_nodes_by_id.find(node.id);
                if (mutable_node_search != mutable_nodes_by_id.end() &&
                    mutable_node_search->second != nullptr) {
                    mutable_node_search->second->outputStreamId =
                        markers_stream_id.value();
                }
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }
        if (node.kind == "export") {
            // Export runs as a control-plane job submitted client-side through
            // the ML proxy (like train); the graph runtime just marks the node
            // ready. Nothing is written until the operator runs the export.
            StreamGraphNodeRuntimeStatus status{
                "running",
                std::nullopt,
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>(
                    "Export node is ready; run the export from the inspector.")};
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }
        if (node.kind == "train") {
            // Training runs as a control-plane job submitted client-side through
            // the ML proxy; the graph runtime just marks the node ready.
            StreamGraphNodeRuntimeStatus status{
                "running",
                std::nullopt,
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>(
                    "Train node is ready; submit a job from the inspector.")};
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }
        if (node.kind == "combine") {
            std::vector<CombineWorkerInput> input_channels{};
            std::optional<std::string> blocking_upstream{};
            bool all_resolved = true;
            for (const auto& edge : graph.edges) {
                if (edge.targetNodeId != node.id) {
                    continue;
                }
                // Resolve the upstream node's full output channel (topic set) so
                // combine can merge per-type — a data source, an experiment's
                // markers, or another combine's "stream" all flow in cleanly.
                const auto upstream_node_search = nodes_by_id.find(edge.sourceNodeId);
                std::vector<StreamGraphOutputTopic> channel_topics{};
                std::optional<uint64_t> resolved_stream_id{};
                if (upstream_node_search != nodes_by_id.end() &&
                    upstream_node_search->second->kind == "stream_source" &&
                    upstream_node_search->second->streamId.has_value()) {
                    resolved_stream_id = upstream_node_search->second->streamId.value();
                    channel_topics = {makeChannelTopic(
                        nat::core::StreamType::DATA, resolved_stream_id.value(),
                        std::string{})};
                } else {
                    const auto channel_search =
                        resolved_output_channels.find(edge.sourceNodeId);
                    if (channel_search != resolved_output_channels.end()) {
                        channel_topics = channel_search->second;
                    }
                    const auto resolved_search =
                        resolved_output_stream_ids.find(edge.sourceNodeId);
                    if (resolved_search != resolved_output_stream_ids.end()) {
                        resolved_stream_id = resolved_search->second;
                    }
                    // Fallback for an upstream that reported no channel (older
                    // path): treat its single output id as one DATA topic.
                    if (channel_topics.empty() && resolved_stream_id.has_value()) {
                        channel_topics = {makeChannelTopic(
                            nat::core::StreamType::DATA,
                            resolved_stream_id.value(), std::string{})};
                    }
                }
                // Honor the per-edge topic filter: drop any topic type the user
                // hid on this link so combine merges only the enabled part of the
                // channel (e.g. hide the markers, keep the data).
                if (!edge.hiddenTopicTypes.empty()) {
                    std::vector<StreamGraphOutputTopic> filtered{};
                    for (const auto& topic : channel_topics) {
                        if (std::find(edge.hiddenTopicTypes.begin(),
                                      edge.hiddenTopicTypes.end(),
                                      topic.type) == edge.hiddenTopicTypes.end()) {
                            filtered.push_back(topic);
                        }
                    }
                    channel_topics = std::move(filtered);
                }
                if (channel_topics.empty()) {
                    all_resolved = false;
                    blocking_upstream = edge.sourceNodeId;
                    break;
                }
                CombineWorkerInput channel{};
                channel.topics = channel_topics;
                channel.inProcess = outputInProcess(edge.sourceNodeId);
                channel.startOffset = resolved_stream_id.has_value()
                    ? sourceOffsetFor(resolved_stream_id.value())
                    : -1;
                input_channels.push_back(std::move(channel));
            }

            if (!all_resolved || input_channels.size() < 2U) {
                encountered_error = true;
                const StreamGraphNodeRuntimeStatus status{
                    blocking_upstream.has_value() ? "blocked" : "error",
                    std::nullopt, std::nullopt, std::nullopt, 0, 0,
                    blocking_upstream.has_value()
                        ? std::string("Blocked: upstream node ") + blocking_upstream.value() +
                              " failed to start."
                        : std::string("combine node requires at least two connected inputs.")};
                if (!commitNodeStatus(node.id, status, std::nullopt)) {
                    aborted = true;
                    break;
                }
                pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
                continue;
            }

            const auto create_result = createCombineWorker(
                broker_manager, input_channels,
                node.outputIdentifier.value_or(node.id), outputInProcess(node.id));
            if (!create_result.ok) {
                encountered_error = true;
                const StreamGraphNodeRuntimeStatus status{
                    "error", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                    create_result.error};
                if (!commitNodeStatus(node.id, status, std::nullopt)) {
                    aborted = true;
                    break;
                }
                pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
                continue;
            }

            created_output_ids.push_back(create_result.outputStreamId);
            StreamGraphNodeRuntimeStatus status{
                "running",
                create_result.outputStreamId,
                create_result.workerId,
                create_result.threadSlotId,
                0, 0,
                create_result.alreadyExists
                    ? std::optional<std::string>("Reused existing combine worker.")
                    : std::nullopt,
            };
            status.outputTopics = create_result.outputTopics;
            if (!commitNodeStatus(node.id, status, create_result.outputStreamId)) {
                aborted = true;
                break;
            }
            resolved_output_stream_ids[node.id] = create_result.outputStreamId;
            resolved_output_channels[node.id] = create_result.outputTopics;
            const auto mutable_node_search = mutable_nodes_by_id.find(node.id);
            if (mutable_node_search != mutable_nodes_by_id.end() &&
                mutable_node_search->second != nullptr) {
                mutable_node_search->second->outputStreamId = create_result.outputStreamId;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }

        if (node.kind != "transform") {
            continue;
        }

        const auto resolved_input = resolveInputStreamId(node);
        if (!resolved_input.streamId.has_value()) {
            encountered_error = true;
            const StreamGraphNodeRuntimeStatus status{
                resolved_input.upstreamNodeId.has_value() ? "blocked" : "error",
                std::nullopt, std::nullopt, std::nullopt, 0, 0,
                resolved_input.upstreamNodeId.has_value()
                    ? std::string("Blocked: upstream node ") +
                          resolved_input.upstreamNodeId.value() + " failed to start."
                    : std::string("Transform node input is not connected.")};
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }

        nlohmann::json transform_json;
        transform_json["transform_kind"] =
            node.transformKind.value_or(std::string{});
        transform_json["config"] = node.config;
        const auto config_maybe = parseEmgTransformConfig(transform_json);
        if (!config_maybe.has_value()) {
            encountered_error = true;
            const StreamGraphNodeRuntimeStatus status{
                "error", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                std::string("Transform configuration could not be parsed.")};
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }

        const bool input_in_process =
            resolved_input.upstreamNodeId.has_value() &&
            outputInProcess(resolved_input.upstreamNodeId.value());
        const auto create_result = createTransformWorker(
            broker_manager,
            resolved_input.streamId.value(),
            node.outputIdentifier.value_or(node.id),
            config_maybe.value(),
            node.inputMappingId.value_or(std::string{}),
            graph.graphId,
            active_run_id,
            node.id,
            input_in_process,
            outputInProcess(node.id),
            sourceOffsetFor(resolved_input.streamId.value()));
        if (!create_result.ok) {
            encountered_error = true;
            const StreamGraphNodeRuntimeStatus status{
                "error", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                create_result.error};
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }

        created_output_ids.push_back(create_result.outputStreamId);
        StreamGraphNodeRuntimeStatus status{
            "running",
            create_result.outputStreamId,
            create_result.workerId,
            create_result.threadSlotId,
            0,
            0,
            create_result.alreadyExists
                ? std::optional<std::string>("Reused existing transform worker.")
                : std::nullopt,
        };
        status.outputTopics = channelTopicsFromWorker(create_result.outputStreamId);
        if (!commitNodeStatus(node.id, status, create_result.outputStreamId)) {
            aborted = true;
            break;
        }
        resolved_output_stream_ids[node.id] = create_result.outputStreamId;
        resolved_output_channels[node.id] = status.outputTopics;
        const auto mutable_node_search = mutable_nodes_by_id.find(node.id);
        if (mutable_node_search != mutable_nodes_by_id.end() &&
            mutable_node_search->second != nullptr) {
            mutable_node_search->second->outputStreamId = create_result.outputStreamId;
        }
        pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
    }

    if (aborted) {
        abortCleanup();
        return;
    }

    // Finalize under the lock: derive the terminal run state and, mirroring the
    // original all-or-nothing failure handling, tear every worker back down if
    // any node failed. Persist the graph with its resolved output stream ids.
    std::vector<uint64_t> ids_to_stop;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto it = g_stream_graph_runtime.find(graph.graphId);
        if (it == g_stream_graph_runtime.end() ||
            it->second.activeRunId != active_run_id ||
            it->second.runState == "stopped") {
            aborted = true;
        } else {
            auto& runtime = it->second;
            bool has_running_node = false;
            for (const auto& entry : runtime.nodeStatuses) {
                if (entry.second.state == "running") {
                    has_running_node = true;
                    break;
                }
            }

            if (encountered_error && !runtime.outputStreamIds.empty()) {
                ids_to_stop = runtime.outputStreamIds;
                for (auto& entry : runtime.nodeStatuses) {
                    auto& status = entry.second;
                    if (status.outputStreamId.has_value() &&
                        (status.state == "running" || status.state == "starting" ||
                         status.state == "stalled")) {
                        status.state = "stopped";
                        if (!status.message.has_value()) {
                            status.message =
                                std::string("Stopped after graph startup failed.");
                        }
                    }
                }
                runtime.outputStreamIds.clear();
            }

            runtime.runState = encountered_error
                ? "error"
                : (has_running_node ? "running" : "stopped");

            // Deliberately NOT written back to g_stream_graphs. `graph` is a local
            // copy taken when the start was requested and nothing here mutates it,
            // so persisting it saves nothing — but it does actively cause harm:
            //
            //  - On a replay-bound run, handleStartStreamGraph has rewritten every
            //    replayed source to its SCRATCH stream id. Persisting that welds a
            //    sealed instance to a topic that is deleted when the replay ends,
            //    so the next replay finds no source matching its bindings and fails
            //    with "no bindings matching this graph's sources" — replay worked
            //    exactly once per instance, and the recorded provenance was lost.
            //  - It clobbers any save another client made after this start began.
            //
            // Runtime facts (output stream ids, node states) belong to `runtime`,
            // which is where they already live.
        }
    }

    if (aborted) {
        abortCleanup();
        return;
    }

    for (uint64_t id : ids_to_stop) {
        stopGraphWorkerByOutputStreamId(id);
    }

    pushStreamGraphStartedMessage(conn, request_id, graph.graphId);
    // Follow the started signal with an authoritative status so a graph that
    // came up in an error/stopped state lands immediately — the client's
    // started handler optimistically assumes "running".
    pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
}

}  // namespace

void StreamViewerWebSocket::handleStartStreamGraph(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const std::string graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "start_stream_graph requires graph_id");
        return;
    }
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    StreamGraphDefinition graph;
    bool clear_failed_run = false;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        ensureStreamGraphStoreLoadedLocked();
        if (!g_stream_graph_store_error.empty()) {
            sendError(conn, g_stream_graph_store_error);
            return;
        }
        const auto search = g_stream_graphs.find(graph_id);
        if (search == g_stream_graphs.end()) {
            sendError(conn, "No saved graph exists for graph_id");
            return;
        }
        const auto runtime_search = g_stream_graph_runtime.find(graph_id);
        if (runtime_search != g_stream_graph_runtime.end()) {
            const auto& run_state = runtime_search->second.runState;
            // "error" is a REPORT of a start that failed, not a live run: by the
            // time it is set, executeStreamGraphStart has already stopped every
            // worker it managed to create and cleared outputStreamIds. Treating it
            // as busy stranded such a graph permanently — the only way out was an
            // explicit stop, which the UI gives the user no reason to press.
            //
            // "starting" still blocks: a start is genuinely in flight and racing it
            // would build a second set of workers for the same nodes. "stalled"
            // blocks too — a stalled node IS running, just idle (a classifier
            // waiting for a model), and restarting the graph is not the fix.
            if (run_state != "stopped" && run_state != "error") {
                sendError(conn,
                          "Graph is " + run_state + "; stop it before starting again",
                          request_id);
                return;
            }
            clear_failed_run = (run_state == "error");
        }
        graph = search->second;
    }

    // Belt and braces before reusing a failed run's slot: the teardown on failure is
    // believed complete, but a straggler would silently double-produce onto a node
    // output. Deliberately called OUTSIDE the block above — it takes
    // g_stream_graph_mutex itself, which is not recursive.
    if (clear_failed_run) {
        stopStreamGraphRuntimeById(graph_id);
    }

    // Replay binding (Phase 5). When a replay id is supplied, every stream_source
    // whose recorded stream matches a replay binding is repointed at that replay's
    // SCRATCH topic for this run. The rewrite happens on the local copy only: the
    // stored instance keeps the stream ids it actually recorded from, because that
    // is provenance, not configuration.
    //
    // Doing it here — before validation, topo sort and worker creation — means the
    // whole downstream chain (transform inputs, viewer subscriptions, combine
    // lanes) resolves against the replayed stream with no other code change. That
    // is the entire reason replay publishes to Kafka instead of an in-process
    // channel.
    const auto replay_id = json.value("replay_id", std::string{});
    std::unordered_set<uint64_t> replay_source_streams;
    uint64_t replay_marker_stream_id = 0;
    if (!replay_id.empty()) {
        std::unordered_map<uint64_t, uint64_t> replay_bindings;
        {
            std::lock_guard<std::mutex> lock(g_replay_mutex);
            const auto search = g_active_replays.find(replay_id);
            if (search == g_active_replays.end()) {
                sendError(conn,
                          "No active replay with id " + replay_id +
                              " — start the replay before starting the graph "
                              "against it.",
                          request_id);
                return;
            }
            for (const auto& binding : search->second.plan.bindings) {
                replay_bindings[binding.originalStreamId] = binding.replayStreamId;
            }
            replay_marker_stream_id = search->second.plan.markerStreamId;
        }
        size_t rebound = 0;
        for (auto& node : graph.nodes) {
            if (node.kind != "stream_source" || !node.streamId.has_value()) {
                continue;
            }
            const auto binding = replay_bindings.find(node.streamId.value());
            if (binding != replay_bindings.end()) {
                node.streamId = binding->second;
                // The replayed stream is ALWAYS the canonical channel frame — the
                // Parquet is that projection — whatever the sensor originally
                // published. So a fork's transforms bind directly, with none of the
                // alternate input mappings a live IMU board needs.
                node.schemaName = nat::core::NatSignalFrameDataSchemaV1::name;
                ++rebound;
            }
        }
        if (rebound == 0) {
            sendError(conn,
                      "Replay " + replay_id +
                          " has no bindings matching this graph's sources, so "
                          "starting it would read live topics instead of the "
                          "recording.",
                      request_id);
            return;
        }
        LOG_INFO << "Starting graph " << graph.graphId << " against replay "
                 << replay_id << " (" << rebound << " source(s) rebound)";
        for (const auto& node : graph.nodes) {
            if (node.kind == "stream_source" && node.streamId.has_value()) {
                replay_source_streams.insert(node.streamId.value());
            }
        }
    }

    const auto validation =
        validateStreamGraphDefinition(graph, broker_manager_, replay_source_streams);
    if (!validation.valid) {
        sendStreamGraphValidation(
            conn,
            request_id,
            graph.graphId,
            false,
            validation.graphDiagnostics,
            validation.nodeDiagnostics,
            validation.edgeDiagnostics);
        return;
    }

    // Provenance edges are lineage, not data — drop them now (after validation,
    // which still inspects them) so the up-front output-id seeding and the
    // detached executeStreamGraphStart both route only real data edges.
    graph.edges.erase(
        std::remove_if(
            graph.edges.begin(),
            graph.edges.end(),
            [](const StreamGraphEdge& edge) { return isProvenanceEdge(edge); }),
        graph.edges.end());

    // Seed a "starting" runtime up front — sources are immediately available,
    // every other node is pending — so a status snapshot reflects the click at
    // once. The slow per-node worker creation then runs on a detached thread
    // (executeStreamGraphStart) that streams incremental status updates. Doing
    // this inline previously froze the WebSocket handler for the full Kafka
    // topic-creation time (tens of seconds) before the client saw anything.
    // Precompute every node's deterministic output stream id (transform/combine
    // outputs are a pure function of their identifier, so they're known before
    // any worker exists). This lets us seed a viewer/sink with the stream it will
    // inspect immediately, so the frontend can subscribe and start rendering as
    // soon as the upstream produces its first frame — instead of waiting for the
    // whole pipeline to finish starting (which, for a deep composite, is the
    // multi-second "Waiting for data" window users hit).
    std::unordered_map<std::string, uint64_t> node_output_id;
    for (const auto& node : graph.nodes) {
        if (node.kind == "stream_source" && node.streamId.has_value()) {
            node_output_id[node.id] = node.streamId.value();
        } else if (node.kind == "transform" || node.kind == "combine") {
            if (node.outputStreamId.has_value()) {
                node_output_id[node.id] = node.outputStreamId.value();
            } else if (node.outputIdentifier.has_value()) {
                const auto topic = createTopicInfo(
                    nat::core::StreamType::DATA,
                    node.kind == "combine" ? "combine" : "transform",
                    node.outputIdentifier.value(),
                    nat::core::NatSignalFrameDataSchemaV1::name);
                if (topic != nullptr) {
                    node_output_id[node.id] = topic->id;
                }
            }
        }
    }
    const auto seedUpstreamOutput =
        [&](const std::string& nodeId) -> std::optional<uint64_t> {
        for (const auto& edge : graph.edges) {
            if (edge.targetNodeId != nodeId) {
                continue;
            }
            const auto it = node_output_id.find(edge.sourceNodeId);
            return it != node_output_id.end()
                       ? std::optional<uint64_t>(it->second)
                       : std::nullopt;
        }
        return std::nullopt;
    };

    StreamGraphRuntimeState runtime;
    runtime.graphId = graph.graphId;
    runtime.activeRunId = graph.graphId + ":" + std::to_string(nowUs());
    runtime.runState = "starting";
    // Remember the replay this run is bound to, so the replay can stop the run it
    // owns when it ends instead of leaving it wedged on deleted scratch topics.
    runtime.boundReplayId = replay_id;
    for (const auto& node : graph.nodes) {
        if (node.kind == "stream_source") {
            runtime.nodeStatuses[node.id] = StreamGraphNodeRuntimeStatus{
                "running",
                node.streamId,
                std::nullopt,
                std::nullopt,
                0,
                0,
                node.streamId.has_value()
                    ? std::optional<std::string>("Source stream is available to downstream nodes.")
                    : std::nullopt};
            if (node.streamId.has_value()) {
                // A raw source is a data-only channel (one DATA topic). The
                // schema is filled in frontend-side from the stream descriptor.
                runtime.nodeStatuses[node.id].outputTopics.push_back(
                    makeChannelTopic(nat::core::StreamType::DATA,
                                     node.streamId.value(), std::string{}));
            }
        } else if (node.kind == "viewer" || node.kind == "sink") {
            // Seed the resolved upstream stream so the frontend subscribes now.
            runtime.nodeStatuses[node.id] = StreamGraphNodeRuntimeStatus{
                "starting",
                seedUpstreamOutput(node.id),
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>("Waiting to start…")};
        } else {
            // transform/combine: seed the deterministic output id too.
            std::optional<uint64_t> seeded_output;
            const auto it = node_output_id.find(node.id);
            if (it != node_output_id.end()) {
                seeded_output = it->second;
            }
            runtime.nodeStatuses[node.id] = StreamGraphNodeRuntimeStatus{
                "starting",
                seeded_output,
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>("Waiting to start…")};
        }
    }

    const std::string active_run_id = runtime.activeRunId;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        g_stream_graph_runtime[graph.graphId] = runtime;
    }

    // Optional replay start (Phase 5): -1 live (default), -2 beginning, >=0 a
    // concrete offset (from a query_stream_time offset_for_timestamp). Only the
    // graph's root stream_source consumers seek to it; the re-run chain feeds
    // downstream nodes live.
    const int64_t replay_start_offset =
        json.value("start_offset", static_cast<int64_t>(-1));

    // Immediate feedback before the (slow) worker creation begins.
    sendStreamGraphStatus(conn, request_id, graph.graphId);

    std::thread(
        executeStreamGraphStart,
        broker_manager_,
        conn,
        request_id,
        std::move(graph),
        active_run_id,
        replay_start_offset,
        replay_marker_stream_id)
        .detach();
}

void StreamViewerWebSocket::handleRestartStreamGraphNode(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    // Incremental reactivity (Phase 7, part A): after a node's config is saved
    // in a RUNNING graph, restart only that node + its downstream subgraph,
    // leaving upstream and unrelated branches running. Output stream ids are
    // deterministic from output_identifier, so they are stable across a
    // config-only restart — input resolution just reads each node's known id.
    const std::string request_id = json.value("request_id", std::string{});
    const std::string graph_id = json.value("graph_id", std::string{});
    const std::string node_id = json.value("node_id", std::string{});
    if (graph_id.empty() || node_id.empty()) {
        sendError(conn, "restart_stream_graph_node requires graph_id and node_id");
        return;
    }

    StreamGraphDefinition graph;
    std::string active_run_id;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto graph_search = g_stream_graphs.find(graph_id);
        if (graph_search == g_stream_graphs.end()) {
            sendError(conn, "No saved graph exists for graph_id");
            return;
        }
        const auto runtime_search = g_stream_graph_runtime.find(graph_id);
        if (runtime_search == g_stream_graph_runtime.end() ||
            runtime_search->second.runState != "running") {
            sendError(conn,
                      "Graph is not running; start it before reconfiguring a node");
            return;
        }
        graph = graph_search->second;
        active_run_id = runtime_search->second.activeRunId;
    }

    // Provenance edges are lineage, not data — drop them so the restart's
    // transport classification, descendant BFS, and input resolution operate on
    // the pure data graph (matches executeStreamGraphStart).
    graph.edges.erase(
        std::remove_if(
            graph.edges.begin(),
            graph.edges.end(),
            [](const StreamGraphEdge& edge) { return isProvenanceEdge(edge); }),
        graph.edges.end());

    // Re-classify transport over the whole (flattened) graph so a restarted node
    // agrees with its still-running upstream about the transport of the shared
    // edge, and any edit that exposed a previously private edge is honored.
    const auto privateOutputs = classifyGraphPrivateOutputs(graph);
    const auto outputInProcess = [&](const std::string& id) {
        return privateOutputs.count(id) != 0;
    };

    // Forward adjacency + BFS to collect node_id and all its descendants.
    std::unordered_map<std::string, std::vector<std::string>> adjacency;
    for (const auto& edge : graph.edges) {
        adjacency[edge.sourceNodeId].push_back(edge.targetNodeId);
    }
    std::unordered_set<std::string> affected;
    affected.insert(node_id);
    std::vector<std::string> frontier{node_id};
    while (!frontier.empty()) {
        const auto current = frontier.back();
        frontier.pop_back();
        const auto adj = adjacency.find(current);
        if (adj == adjacency.end()) {
            continue;
        }
        for (const auto& next : adj->second) {
            if (affected.insert(next).second) {
                frontier.push_back(next);
            }
        }
    }

    // Index nodes and seed each node's known output stream id (stable). Sources
    // contribute their stream id; transform/combine their persisted output id.
    std::unordered_map<std::string, const StreamGraphNode*> nodes_by_id;
    std::unordered_map<std::string, uint64_t> output_stream_ids;
    for (const auto& node : graph.nodes) {
        nodes_by_id[node.id] = &node;
        if (node.kind == "stream_source" && node.streamId.has_value()) {
            output_stream_ids[node.id] = node.streamId.value();
        } else if (node.outputStreamId.has_value()) {
            output_stream_ids[node.id] = node.outputStreamId.value();
        }
    }

    // Stop the affected transform/combine workers first (frees their output
    // stream ids so recreation actually picks up the new config — the create
    // helpers short-circuit if the id is still registered).
    for (const auto& affected_id : affected) {
        const auto search = nodes_by_id.find(affected_id);
        if (search == nodes_by_id.end()) {
            continue;
        }
        const auto& kind = search->second->kind;
        if (kind != "transform" && kind != "combine") {
            continue;
        }
        const auto out_search = output_stream_ids.find(affected_id);
        if (out_search != output_stream_ids.end()) {
            stopGraphWorkerByOutputStreamId(out_search->second);
        }
    }

    // Recreate the affected transform/combine workers in topological order.
    for (const auto& ordered_id : topologicallySortStreamGraph(graph)) {
        if (affected.find(ordered_id) == affected.end()) {
            continue;
        }
        const auto node_search = nodes_by_id.find(ordered_id);
        if (node_search == nodes_by_id.end()) {
            continue;
        }
        const StreamGraphNode& node = *node_search->second;

        StreamGraphNodeRuntimeStatus status;
        std::optional<uint64_t> new_output_id;

        if (node.kind == "transform") {
            std::optional<uint64_t> input_stream_id;
            std::optional<std::string> upstream_node_id;
            for (const auto& edge : graph.edges) {
                if (edge.targetNodeId != node.id) {
                    continue;
                }
                upstream_node_id = edge.sourceNodeId;
                const auto up = output_stream_ids.find(edge.sourceNodeId);
                if (up != output_stream_ids.end()) {
                    input_stream_id = up->second;
                }
                break;
            }
            if (!input_stream_id.has_value()) {
                status = {"blocked", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                          std::optional<std::string>("Blocked: upstream input not resolved.")};
            } else {
                nlohmann::json transform_json;
                transform_json["transform_kind"] =
                    node.transformKind.value_or(std::string{});
                transform_json["config"] = node.config;
                const auto config_maybe = parseEmgTransformConfig(transform_json);
                if (!config_maybe.has_value()) {
                    status = {"error", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                              std::optional<std::string>("Transform configuration could not be parsed.")};
                } else {
                    const bool input_in_process =
                        upstream_node_id.has_value() &&
                        outputInProcess(upstream_node_id.value());
                    const auto create_result = createTransformWorker(
                        broker_manager_,
                        input_stream_id.value(),
                        node.outputIdentifier.value_or(node.id),
                        config_maybe.value(),
                        node.inputMappingId.value_or(std::string{}),
                        graph.graphId,
                        active_run_id,
                        node.id,
                        input_in_process,
                        outputInProcess(node.id));
                    if (!create_result.ok) {
                        status = {"error", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                                  create_result.error};
                    } else {
                        new_output_id = create_result.outputStreamId;
                        output_stream_ids[node.id] = create_result.outputStreamId;
                        status = {"running", create_result.outputStreamId,
                                  create_result.workerId, create_result.threadSlotId, 0, 0,
                                  std::optional<std::string>("Restarted with updated config.")};
                    }
                }
            }
        } else if (node.kind == "combine") {
            std::vector<CombineWorkerInput> input_channels;
            bool all_resolved = true;
            // The restart path only tracks a single output id per node, so probe
            // each input id for its DATA and/or MARKER topic to rebuild the
            // channel (topic-aware combine, Part B).
            const auto probeChannelTopics =
                [&](uint64_t id) -> std::vector<StreamGraphOutputTopic> {
                std::vector<StreamGraphOutputTopic> topics{};
                auto data_topic = nat::tools::resolveGraphSourceTopic(
                    id,
                    [&](uint64_t sid) {
                        return findTransformSourceTopicForStream(broker_manager_, sid);
                    },
                    [](uint64_t sid) {
                        return findGraphInternalOutputTopicForStream(sid);
                    });
                if (data_topic != nullptr) {
                    topics.push_back(makeChannelTopic(
                        nat::core::StreamType::DATA, id, data_topic->schemaName));
                }
                auto marker_topic =
                    findMarkerOrMetaTopicForStreamId(broker_manager_, id);
                if (marker_topic == nullptr) {
                    marker_topic = findGraphInternalMarkerTopicForStream(id);
                }
                if (marker_topic != nullptr) {
                    topics.push_back(makeChannelTopic(
                        marker_topic->type, id, marker_topic->schemaName));
                }
                if (topics.empty()) {
                    topics.push_back(makeChannelTopic(
                        nat::core::StreamType::DATA, id, std::string{}));
                }
                return topics;
            };
            for (const auto& edge : graph.edges) {
                if (edge.targetNodeId != node.id) {
                    continue;
                }
                const auto up = output_stream_ids.find(edge.sourceNodeId);
                if (up == output_stream_ids.end()) {
                    all_resolved = false;
                    break;
                }
                auto channel_topics = probeChannelTopics(up->second);
                // Honor the per-edge topic filter (same as the start path).
                if (!edge.hiddenTopicTypes.empty()) {
                    std::vector<StreamGraphOutputTopic> filtered{};
                    for (const auto& topic : channel_topics) {
                        if (std::find(edge.hiddenTopicTypes.begin(),
                                      edge.hiddenTopicTypes.end(),
                                      topic.type) == edge.hiddenTopicTypes.end()) {
                            filtered.push_back(topic);
                        }
                    }
                    channel_topics = std::move(filtered);
                }
                if (channel_topics.empty()) {
                    all_resolved = false;
                    break;
                }
                CombineWorkerInput channel{};
                channel.topics = std::move(channel_topics);
                channel.inProcess = outputInProcess(edge.sourceNodeId);
                channel.startOffset = -1;
                input_channels.push_back(std::move(channel));
            }
            if (!all_resolved || input_channels.size() < 2U) {
                status = {"blocked", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                          std::optional<std::string>("combine node inputs not resolved.")};
            } else {
                const auto create_result = createCombineWorker(
                    broker_manager_, input_channels,
                    node.outputIdentifier.value_or(node.id),
                    outputInProcess(node.id));
                if (!create_result.ok) {
                    status = {"error", std::nullopt, std::nullopt, std::nullopt, 0, 0,
                              create_result.error};
                } else {
                    new_output_id = create_result.outputStreamId;
                    output_stream_ids[node.id] = create_result.outputStreamId;
                    status = {"running", create_result.outputStreamId,
                              create_result.workerId, create_result.threadSlotId, 0, 0,
                              std::optional<std::string>("Restarted (downstream of a reconfigured node).")};
                }
            }
        } else {
            continue;  // viewer/sink/session/train/source need no worker restart
        }

        if (new_output_id.has_value()) {
            status.outputTopics = channelTopicsFromWorker(new_output_id.value());
        }

        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto rt = g_stream_graph_runtime.find(graph_id);
        if (rt == g_stream_graph_runtime.end() ||
            rt->second.activeRunId != active_run_id ||
            rt->second.runState == "stopped") {
            return;  // superseded by a concurrent stop/start
        }
        rt->second.nodeStatuses[node.id] = status;
        if (new_output_id.has_value()) {
            auto& ids = rt->second.outputStreamIds;
            if (std::find(ids.begin(), ids.end(), new_output_id.value()) == ids.end()) {
                ids.push_back(new_output_id.value());
            }
            const auto stored = g_stream_graphs.find(graph_id);
            if (stored != g_stream_graphs.end()) {
                for (auto& stored_node : stored->second.nodes) {
                    if (stored_node.id == node.id) {
                        stored_node.outputStreamId = new_output_id;
                        break;
                    }
                }
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        std::string persist_error;
        persistStreamGraphStoreLocked(persist_error);
    }
    pushStreamGraphStatusMessage(conn, request_id, graph_id);
    broadcastTransformList();
}

void StreamViewerWebSocket::handleStopStreamGraph(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const std::string graph_id = json.value("graph_id", std::string{});
    if (graph_id.empty()) {
        sendError(conn, "stop_stream_graph requires graph_id");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        if (!g_stream_graph_runtime.contains(graph_id)) {
            sendError(conn, "No active graph runtime exists for graph_id");
            return;
        }
    }

    // Same teardown the replay-end cleanup uses; no expected replay id, because an
    // explicit stop applies to whatever run is currently active.
    stopStreamGraphRuntimeById(graph_id);

    sendStreamGraphStopped(conn, request_id, graph_id);
    broadcastTransformList();
}

void StreamViewerWebSocket::handleUnsubscribe(const WebSocketConnectionPtr& conn,
                                               StreamViewerClientContext* ctx,
                                               const std::vector<uint64_t>& stream_ids)
{
    bool went_idle = false;
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);

        for (uint64_t stream_id : stream_ids) {
            ctx->subscribed_streams.erase(stream_id);
            ctx->requested_start_offsets.erase(stream_id);

            // Remove messengers for this stream
            ctx->messengers.erase(
                std::remove_if(ctx->messengers.begin(), ctx->messengers.end(),
                    [stream_id](const std::unique_ptr<nat::core::TopicMessenger>& m) {
                        return m->getId() == stream_id;
                    }),
                ctx->messengers.end()
            );
            
            LOG_INFO << "StreamViewer: Unsubscribed from stream " << stream_id;
        }

        // Stop streaming thread if no subscriptions
        if (ctx->subscribed_streams.empty()) {
            ctx->active = false;
            went_idle = true;
        }
    } // Release lock before calling sendStatus

    // Join the stopped thread OUTSIDE ctx->mutex (the loop body holds that lock
    // each iteration, so joining under it would deadlock). Leaving the thread
    // unjoined here was the "close/reopen a few times" bug: the object stayed
    // joinable, so the next subscribe neither restarted the loop nor reset
    // active, wedging the viewer on "Waiting for data".
    if (went_idle && ctx->streaming_thread.joinable()) {
        ctx->streaming_thread.join();
    }

    sendStatus(conn, ctx);
}

void StreamViewerWebSocket::sendStreamList(const WebSocketConnectionPtr& conn)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    nlohmann::json response;
    response["type"] = "stream_list";
    response["streams"] = nlohmann::json::object();

    auto rawStreams = broker_manager_->getAllStreams();
    for (const auto& stream : rawStreams) {
        uint64_t id = stream->getId();
        nlohmann::json stream_json;
        stream_json["topics"] = nlohmann::json::array();

        auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
        for (const auto& topic : dataTopics) {
            nlohmann::json topic_json;
            topic_json["schema_name"] = topic->schemaName;
            topic_json["type"] = "Data";
            topic_json["serialization_type"] = nat::core::toString(topic->serializationType);
            const auto descriptor_json =
                getDescriptorJsonForSchemaName(topic->schemaName);
            if (descriptor_json.has_value()) {
                topic_json["descriptor"] = descriptor_json.value();
            }
            stream_json["topics"].push_back(topic_json);
        }

        auto metaTopics = stream->getTopicsByType(nat::core::StreamType::META);
        for (const auto& topic : metaTopics) {
            nlohmann::json topic_json;
            topic_json["schema_name"] = topic->schemaName;
            topic_json["type"] = "Meta";
            topic_json["serialization_type"] = nat::core::toString(topic->serializationType);
            const auto descriptor_json =
                getDescriptorJsonForSchemaName(topic->schemaName);
            if (descriptor_json.has_value()) {
                topic_json["descriptor"] = descriptor_json.value();
            }
            stream_json["topics"].push_back(topic_json);
        }

        response["streams"][std::to_string(id)] = stream_json;
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::streamingThreadFunc(const WebSocketConnectionPtr& conn,
                                                 StreamViewerClientContext* ctx)
{
    LOG_INFO << "StreamViewer: Streaming thread started";

    uint64_t last_pending_resolve_us = 0;

    while (ctx->active.load()) {
        // Check if connection is still valid
        if (!conn || !conn->connected()) {
            LOG_INFO << "StreamViewer: Connection lost, stopping streaming thread";
            ctx->active = false;
            break;
        }

        bool had_data = false;

        try {
            std::lock_guard<std::mutex> lock(ctx->mutex);

            // Lazily bind messengers for any subscription whose topic wasn't
            // discoverable at subscribe time (e.g. a transform/combine output
            // from a graph that had only just been started). Without this, such
            // a subscription would never deliver data — the "Waiting for
            // EMG data…" that showed up whenever the viewer was opened before
            // the output stream had propagated. Throttled so we don't call
            // getAllStreams() on every 10 ms poll.
            //
            // Topic-aware channels: a channel id can carry BOTH a DATA and a
            // MARKER topic (a combine "stream" output), each materialising at a
            // different time, so we track and retry the two lanes independently
            // (a messenger-count check can't tell them apart — both share the id).
            const bool has_unbound =
                ctx->bound_data_ids.size() + ctx->bound_marker_ids.size() <
                ctx->subscribed_streams.size() * 2;
            if (broker_manager_ && has_unbound) {
                const uint64_t now_us = nowUs();
                if (now_us - last_pending_resolve_us > 500000ULL) {
                    last_pending_resolve_us = now_us;
                    // getAllStreams() is only needed to discover a DATA topic; skip
                    // it when every data lane is already bound (only markers remain
                    // to probe, which use the cheaper per-id topic lookups).
                    const bool any_data_unbound =
                        ctx->bound_data_ids.size() <
                        ctx->subscribed_streams.size();
                    std::vector<std::unique_ptr<nat::core::RawStream>> rawStreams;
                    if (any_data_unbound) {
                        rawStreams = broker_manager_->getAllStreams();
                    }
                    for (uint64_t stream_id : ctx->subscribed_streams) {
                        if (ctx->bound_data_ids.count(stream_id) == 0) {
                            std::shared_ptr<nat::core::BasicTopicInformation> dataTopic;
                            for (const auto& stream : rawStreams) {
                                if (stream->getId() != stream_id) {
                                    continue;
                                }
                                auto dataTopics = stream->getTopicsByType(
                                    nat::core::StreamType::DATA);
                                dataTopic = choosePreferredDataTopic(dataTopics);
                                break;
                            }
                            // Same in-memory fallback as handleSubscribe for a
                            // graph output not yet visible in broker metadata.
                            if (dataTopic == nullptr) {
                                dataTopic =
                                    findGraphInternalOutputTopicForStream(stream_id);
                            }
                            if (dataTopic != nullptr) {
                                const int64_t data_offset =
                                    ctx->requested_start_offsets.count(stream_id)
                                        ? ctx->requested_start_offsets.at(stream_id)
                                        : -1;
                                ctx->messengers.push_back(
                                    broker_manager_->createMessenger(
                                        dataTopic, data_offset));
                                ctx->bound_data_ids.insert(stream_id);
                                LOG_INFO << "StreamViewer: Lazily bound data "
                                            "messenger for stream "
                                         << stream_id;
                            }
                        }
                        // Marker/meta topics (an experiment's `markers` output, or
                        // a combine "stream" output's Marker/<id>) materialise only
                        // once the first record is published — retry until then.
                        if (ctx->bound_marker_ids.count(stream_id) == 0) {
                            auto markerTopic = findMarkerOrMetaTopicForStreamId(
                                broker_manager_, stream_id);
                            if (markerTopic == nullptr) {
                                markerTopic =
                                    findGraphInternalMarkerTopicForStream(stream_id);
                            }
                            if (markerTopic != nullptr) {
                                const bool is_marker =
                                    markerTopic->type ==
                                    nat::core::StreamType::MARKER;
                                // Same rule as the immediate bind: bundle a MARKER
                                // with data, or bind a lone marker/meta stream that
                                // has no data topic. Don't newly stream a data
                                // stream's META topic.
                                if (is_marker ||
                                    ctx->bound_data_ids.count(stream_id) == 0) {
                                    ctx->messengers.push_back(
                                        broker_manager_->createMessenger(
                                            markerTopic,
                                            kMarkerConsumerStartOffset));
                                    ctx->bound_marker_ids.insert(stream_id);
                                    LOG_INFO << "StreamViewer: Lazily bound marker "
                                                "messenger for stream "
                                             << stream_id;
                                }
                            }
                        }
                    }
                }
            }

            for (auto& messenger : ctx->messengers) {
                if (!ctx->active.load()) break;  // Check again in case we should stop
                
                uint64_t stream_id = messenger->getId();
                std::string encoding_type = nat::core::toString(messenger->getSerializationType());
                std::string schema_name = messenger->getSchemaName();

                // Try to get next message
                auto messageMaybe = messenger->tryGetNexMessage();
                if (messageMaybe.has_value()) {
                    had_data = true;
                    std::unique_ptr<nat::core::Schema> message = std::move(messageMaybe.value());

                    // Try to cast to NatImuBulkDataSchema first
                    nat::core::NatImuBulkDataSchema* bulkData = 
                        dynamic_cast<nat::core::NatImuBulkDataSchema*>(message.get());
                    
                    if (bulkData != nullptr) {
                        auto json = formatBulkDataAsJson(*bulkData, stream_id, encoding_type, 5000);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Try single NatImuDataSchema
                    nat::core::NatImuDataSchema* imuData = 
                        dynamic_cast<nat::core::NatImuDataSchema*>(message.get());
                    
                    if (imuData != nullptr) {
                        auto json = formatImuDataAsJson(*imuData, stream_id, encoding_type, 50);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Try NatMuseBulkDataSchema
                    nat::core::NatMuseBulkDataSchema* museBulkData = 
                        dynamic_cast<nat::core::NatMuseBulkDataSchema*>(message.get());
                    
                    if (museBulkData != nullptr) {
                        auto json = formatMuseBulkDataAsJson(*museBulkData, stream_id, encoding_type, 34900);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Try single NatMuseDataSchema
                    nat::core::NatMuseDataSchema* museData = 
                        dynamic_cast<nat::core::NatMuseDataSchema*>(message.get());
                    
                    if (museData != nullptr) {
                        auto json = formatMuseDataAsJson(*museData, stream_id, encoding_type, 349);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    nat::core::ExgPillEmgDataSchemaV1* emgData =
                        dynamic_cast<nat::core::ExgPillEmgDataSchemaV1*>(message.get());

                    if (emgData != nullptr) {
                        size_t encoding_size = 0;
                        std::unique_ptr<std::vector<uint8_t>> encoded =
                            emgData->encodeToBytes(
                                messenger->getSerializationType());
                        if (encoded) {
                            encoding_size = encoded->size();
                        }
                        auto json = formatEmgDataAsJson(
                            *emgData, stream_id, encoding_type, encoding_size);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    nat::core::ExgPillEmgTransformDataSchemaV1* transformedEmgData =
                        dynamic_cast<nat::core::ExgPillEmgTransformDataSchemaV1*>(
                            message.get());

                    if (transformedEmgData != nullptr) {
                        size_t encoding_size = 0;
                        std::unique_ptr<std::vector<uint8_t>> encoded =
                            transformedEmgData->encodeToBytes(
                                messenger->getSerializationType());
                        if (encoded) {
                            encoding_size = encoded->size();
                        }
                        auto json = formatEmgDataAsJson(
                            *transformedEmgData, stream_id, encoding_type, encoding_size);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    nat::core::NatSignalFrameDataSchemaV1* signalFrame =
                        dynamic_cast<nat::core::NatSignalFrameDataSchemaV1*>(
                            message.get());

                    if (signalFrame != nullptr) {
                        size_t encoding_size = 0;
                        std::unique_ptr<std::vector<uint8_t>> encoded =
                            signalFrame->encodeToBytes(
                                messenger->getSerializationType());
                        if (encoded) {
                            encoding_size = encoded->size();
                        }
                        auto json = formatEmgDataAsJson(
                            *signalFrame, stream_id, encoding_type, encoding_size);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    nat::core::TransformProvenanceRecord* transformProvenance =
                        dynamic_cast<nat::core::TransformProvenanceRecord*>(
                            message.get());

                    if (transformProvenance != nullptr) {
                        size_t encoding_size = 0;
                        std::unique_ptr<std::vector<uint8_t>> encoded =
                            transformProvenance->encodeToBytes(
                                messenger->getSerializationType());
                        if (encoded) {
                            encoding_size = encoded->size();
                        }
                        auto json = formatTransformProvenanceAsJson(
                            *transformProvenance,
                            stream_id,
                            encoding_type,
                            encoding_size);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Marker events (Phase 2): forward MarkerEventV1 records so
                    // the marker renderer can draw cue/session events. Markers
                    // are published to Marker/<session_id> topics (e.g. an
                    // experiment node's `markers` output).
                    nat::core::MarkerEventV1* markerEvent =
                        dynamic_cast<nat::core::MarkerEventV1*>(message.get());
                    if (markerEvent != nullptr) {
                        size_t encoding_size = 0;
                        std::unique_ptr<std::vector<uint8_t>> encoded =
                            markerEvent->encodeToBytes(
                                messenger->getSerializationType());
                        if (encoded) {
                            encoding_size = encoded->size();
                        }
                        auto json = formatMarkerEventAsJson(
                            *markerEvent, stream_id, encoding_type, encoding_size);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // --- Generic descriptor-driven fallback (Phase 3) ---
                    // No concrete formatter matched. If the record's descriptor
                    // matches the canonical channel-frame contract, project it
                    // to one generic `frame` message. This is how a NEW sensor
                    // is onboarded end-to-end with zero dispatch/formatter edits:
                    // register a schema + descriptor whose fields match the
                    // channel-frame contract and it plots + is transform-ready.
                    auto descriptor_maybe =
                        nat::core::DataSchemaDescriptorRegistry::getDefault()
                            .findBySchemaName(schema_name);
                    if (descriptor_maybe.has_value() &&
                        descriptor_maybe.value() != nullptr) {
                        auto frame = tryNormalizeNumericChannelFrame(
                            *message, *descriptor_maybe.value());
                        if (frame.has_value()) {
                            auto json = formatNormalizedFrameAsJson(
                                frame.value(),
                                stream_id,
                                encoding_type,
                                schema_name);
                            if (conn && conn->connected()) {
                                conn->send(json.dump());
                            }
                            continue;
                        }
                    }
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR << "StreamViewer: Error in streaming thread: " << e.what();
        }

        // Small sleep to avoid busy-waiting
        if (!had_data) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    LOG_INFO << "StreamViewer: Streaming thread stopped";
}

nlohmann::json StreamViewerWebSocket::formatImuDataAsJson(const nat::core::NatImuDataSchema& data,
                                                           uint64_t stream_id,
                                                           const std::string& encoding_type,
                                                           size_t encoding_size)
{
    const float* values = data.getData();

    nlohmann::json json;
    json["type"] = "imu_data";
    json["stream_id"] = std::to_string(stream_id);
    json["timestamp"] = static_cast<uint64_t>(data.getTime());
    
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;

    json["data"]["accel"]["x"] = values[0];
    json["data"]["accel"]["y"] = values[1];
    json["data"]["accel"]["z"] = values[2];

    json["data"]["gyro"]["x"] = values[3];
    json["data"]["gyro"]["y"] = values[4];
    json["data"]["gyro"]["z"] = values[5];

    json["data"]["quat"]["real"] = values[6];
    json["data"]["quat"]["i"] = values[7];
    json["data"]["quat"]["j"] = values[8];
    json["data"]["quat"]["k"] = values[9];

    // Magnetometer, frame version 2 onwards. ⚠️ Always emitted, so the shape of
    // the message does not depend on the recording -- a consumer that switched on
    // the key's PRESENCE would work on new data and break on old. It is
    // has_data.magnetometer that says whether the numbers mean anything, and for
    // every version 1 frame that is false with the values zeroed.
    json["data"]["mag"]["x"] = values[10];
    json["data"]["mag"]["y"] = values[11];
    json["data"]["mag"]["z"] = values[12];

    json["accuracies"]["accelerometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getAccelerationAccuracy());
    json["accuracies"]["gyroscope"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getGyroscopeAccuracy());
    json["accuracies"]["rotation"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getRotationAccuracy());
    json["accuracies"]["magnetometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getMagnetometerAccuracy());

    json["has_data"]["accelerometer"] = data.wasDataSetForAcceleration();
    json["has_data"]["gyroscope"] = data.wasDataSetForGryoscope();
    json["has_data"]["rotation"] = data.wasDataSetForRotation();
    json["has_data"]["magnetometer"] = data.wasDataSetForMagnetometer();

    return json;
}

nlohmann::json StreamViewerWebSocket::formatBulkDataAsJson(const nat::core::NatImuBulkDataSchema& bulk,
                                                            uint64_t stream_id,
                                                            const std::string& encoding_type,
                                                            size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "imu_bulk_data";
    json["stream_id"] = std::to_string(stream_id);
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    // Frame envelope (populated for framed payloads; zero for legacy frames).
    json["schema_version"] = bulk.getSchemaVersion();
    json["seq_no"] = bulk.getSeqNo();
    json["device_ts_us"] = bulk.getDeviceTsUs();
    json["sample_rate_hz"] = bulk.getSampleRateHz();
    json["sample_count"] = bulk.getSampleCount();
    json["samples"] = nlohmann::json::array();

    auto records = bulk.createImuRecords();
    if (records) {
        for (const auto& record : *records) {
            const float* values = record.getData();
            
            nlohmann::json sample;
            sample["timestamp"] = static_cast<uint64_t>(record.getTime());
            
            sample["data"]["accel"]["x"] = values[0];
            sample["data"]["accel"]["y"] = values[1];
            sample["data"]["accel"]["z"] = values[2];

            sample["data"]["gyro"]["x"] = values[3];
            sample["data"]["gyro"]["y"] = values[4];
            sample["data"]["gyro"]["z"] = values[5];

            sample["data"]["quat"]["real"] = values[6];
            sample["data"]["quat"]["i"] = values[7];
            sample["data"]["quat"]["j"] = values[8];
            sample["data"]["quat"]["k"] = values[9];

            // See formatImuDataAsJson: always emitted, gated by has_data.
            sample["data"]["mag"]["x"] = values[10];
            sample["data"]["mag"]["y"] = values[11];
            sample["data"]["mag"]["z"] = values[12];

            sample["accuracies"]["accelerometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getAccelerationAccuracy());
            sample["accuracies"]["gyroscope"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getGyroscopeAccuracy());
            sample["accuracies"]["rotation"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getRotationAccuracy());
            sample["accuracies"]["magnetometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getMagnetometerAccuracy());

            sample["has_data"]["accelerometer"] = record.wasDataSetForAcceleration();
            sample["has_data"]["gyroscope"] = record.wasDataSetForGryoscope();
            sample["has_data"]["rotation"] = record.wasDataSetForRotation();
            sample["has_data"]["magnetometer"] = record.wasDataSetForMagnetometer();

            json["samples"].push_back(sample);
        }
    }

    return json;
}

nlohmann::json StreamViewerWebSocket::formatMuseDataAsJson(const nat::core::NatMuseDataSchema& data,
                                                            uint64_t stream_id,
                                                            const std::string& encoding_type,
                                                            size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "muse_data";
    json["stream_id"] = std::to_string(stream_id);
    json["timestamp"] = data.getTime();
    json["eeg_sequence"] = data.getEegSequence();
    json["motion_sequence"] = data.getMotionSequence();

    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;

    // EEG data (4 channels × 12 samples)
    const float* tp9 = data.getTp9();
    const float* af7 = data.getAf7();
    const float* af8 = data.getAf8();
    const float* tp10 = data.getTp10();

    json["eeg"]["tp9"] = nlohmann::json::array();
    json["eeg"]["af7"] = nlohmann::json::array();
    json["eeg"]["af8"] = nlohmann::json::array();
    json["eeg"]["tp10"] = nlohmann::json::array();

    for (int i = 0; i < nat::core::NatMuseDataSchema::EEG_SAMPLES_PER_PACKET; ++i) {
        json["eeg"]["tp9"].push_back(tp9[i]);
        json["eeg"]["af7"].push_back(af7[i]);
        json["eeg"]["af8"].push_back(af8[i]);
        json["eeg"]["tp10"].push_back(tp10[i]);
    }

    // Motion data (3 samples each)
    const float (*accel)[3] = data.getAccel();
    const float (*gyro)[3] = data.getGyro();

    json["accel"] = nlohmann::json::array();
    json["gyro"] = nlohmann::json::array();

    for (int i = 0; i < nat::core::NatMuseDataSchema::MOTION_SAMPLES; ++i) {
        json["accel"].push_back({{"x", accel[i][0]}, {"y", accel[i][1]}, {"z", accel[i][2]}});
        json["gyro"].push_back({{"x", gyro[i][0]}, {"y", gyro[i][1]}, {"z", gyro[i][2]}});
    }

    // PPG data (3 channels × 6 samples)
    const float* ppg0 = data.getPpg0();
    const float* ppg1 = data.getPpg1();
    const float* ppg2 = data.getPpg2();

    json["ppg"]["ppg0"] = nlohmann::json::array();
    json["ppg"]["ppg1"] = nlohmann::json::array();
    json["ppg"]["ppg2"] = nlohmann::json::array();

    for (int i = 0; i < nat::core::NatMuseDataSchema::PPG_SAMPLES; ++i) {
        json["ppg"]["ppg0"].push_back(ppg0[i]);
        json["ppg"]["ppg1"].push_back(ppg1[i]);
        json["ppg"]["ppg2"].push_back(ppg2[i]);
    }

    // has_data flags
    json["has_data"]["eeg"] = data.hasEegData();
    json["has_data"]["accel"] = data.hasAccelData();
    json["has_data"]["gyro"] = data.hasGyroData();
    json["has_data"]["ppg"] = data.hasPpgData();

    return json;
}

nlohmann::json StreamViewerWebSocket::formatMuseBulkDataAsJson(const nat::core::NatMuseBulkDataSchema& bulk,
                                                                uint64_t stream_id,
                                                                const std::string& encoding_type,
                                                                size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "muse_bulk_data";
    json["stream_id"] = std::to_string(stream_id);
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    json["samples"] = nlohmann::json::array();

    auto records = bulk.createMuseRecords();
    if (records) {
        for (const auto& record : *records) {
            nlohmann::json sample;
            sample["timestamp"] = record.getTime();
            sample["eeg_sequence"] = record.getEegSequence();
            sample["motion_sequence"] = record.getMotionSequence();

            // EEG data
            const float* tp9 = record.getTp9();
            const float* af7 = record.getAf7();
            const float* af8 = record.getAf8();
            const float* tp10 = record.getTp10();

            sample["eeg"]["tp9"] = nlohmann::json::array();
            sample["eeg"]["af7"] = nlohmann::json::array();
            sample["eeg"]["af8"] = nlohmann::json::array();
            sample["eeg"]["tp10"] = nlohmann::json::array();

            for (int i = 0; i < nat::core::NatMuseDataSchema::EEG_SAMPLES_PER_PACKET; ++i) {
                sample["eeg"]["tp9"].push_back(tp9[i]);
                sample["eeg"]["af7"].push_back(af7[i]);
                sample["eeg"]["af8"].push_back(af8[i]);
                sample["eeg"]["tp10"].push_back(tp10[i]);
            }

            // Motion data
            const float (*accel)[3] = record.getAccel();
            const float (*gyro)[3] = record.getGyro();

            sample["accel"] = nlohmann::json::array();
            sample["gyro"] = nlohmann::json::array();

            for (int i = 0; i < nat::core::NatMuseDataSchema::MOTION_SAMPLES; ++i) {
                sample["accel"].push_back({{"x", accel[i][0]}, {"y", accel[i][1]}, {"z", accel[i][2]}});
                sample["gyro"].push_back({{"x", gyro[i][0]}, {"y", gyro[i][1]}, {"z", gyro[i][2]}});
            }

            // PPG data
            const float* ppg0 = record.getPpg0();
            const float* ppg1 = record.getPpg1();
            const float* ppg2 = record.getPpg2();

            sample["ppg"]["ppg0"] = nlohmann::json::array();
            sample["ppg"]["ppg1"] = nlohmann::json::array();
            sample["ppg"]["ppg2"] = nlohmann::json::array();

            for (int i = 0; i < nat::core::NatMuseDataSchema::PPG_SAMPLES; ++i) {
                sample["ppg"]["ppg0"].push_back(ppg0[i]);
                sample["ppg"]["ppg1"].push_back(ppg1[i]);
                sample["ppg"]["ppg2"].push_back(ppg2[i]);
            }

            // has_data flags
            sample["has_data"]["eeg"] = record.hasEegData();
            sample["has_data"]["accel"] = record.hasAccelData();
            sample["has_data"]["gyro"] = record.hasGyroData();
            sample["has_data"]["ppg"] = record.hasPpgData();

            json["samples"].push_back(sample);
        }
    }

    return json;
}

nlohmann::json StreamViewerWebSocket::formatEmgDataAsJson(const nat::core::ExgPillEmgDataSchemaV1& data,
                                                           uint64_t stream_id,
                                                           const std::string& encoding_type,
                                                           size_t encoding_size)
{
    return formatSignalFrameAsJson(
        data,
        nat::core::ExgPillEmgDataSchemaV1::schemaVersion,
        stream_id,
        encoding_type,
        encoding_size);
}

nlohmann::json StreamViewerWebSocket::formatEmgDataAsJson(
    const nat::core::ExgPillEmgTransformDataSchemaV1& data,
    uint64_t stream_id,
    const std::string& encoding_type,
    size_t encoding_size)
{
    return formatSignalFrameAsJson(
        data,
        nat::core::ExgPillEmgTransformDataSchemaV1::schemaVersion,
        stream_id,
        encoding_type,
        encoding_size);
}

nlohmann::json StreamViewerWebSocket::formatEmgDataAsJson(
    const nat::core::NatSignalFrameDataSchemaV1& data,
    uint64_t stream_id,
    const std::string& encoding_type,
    size_t encoding_size)
{
    return formatSignalFrameAsJson(
        data,
        nat::core::NatSignalFrameDataSchemaV1::schemaVersion,
        stream_id,
        encoding_type,
        encoding_size);
}

nlohmann::json StreamViewerWebSocket::formatTransformProvenanceAsJson(
    const nat::core::TransformProvenanceRecord& record,
    uint64_t stream_id,
    const std::string& encoding_type,
    size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "transform_provenance";
    json["stream_id"] = std::to_string(stream_id);
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    json["output_identifier"] = record.getOutputIdentifier();
    json["output_stream_id"] = std::to_string(record.getOutputStreamId());
    json["output_schema_name"] = record.getOutputSchemaName();
    json["output_topic"] = record.getOutputTopic();
    json["source_stream_id"] = std::to_string(record.getSourceStreamId());
    json["source_schema_name"] = record.getSourceSchemaName();
    json["source_topic"] = record.getSourceTopic();
    json["transform_kind"] = record.getTransformKind();
    json["input_mapping_id"] = record.getInputMappingId();
    json["config_json"] = record.getConfigJson();
    json["created_at_us"] = record.getCreatedAtUs();
    return json;
}

void StreamViewerWebSocket::sendError(const WebSocketConnectionPtr& conn,
                                     const std::string& message,
                                     const std::string& requestId)
{
    nlohmann::json json;
    json["type"] = "error";
    json["message"] = message;
    if (!requestId.empty()) {
        json["request_id"] = requestId;
    }
    conn->send(json.dump());
}

void StreamViewerWebSocket::sendStatus(const WebSocketConnectionPtr& conn, StreamViewerClientContext* ctx)
{
    nlohmann::json json;
    json["type"] = "status";
    json["connected"] = true;
    json["subscribed_streams"] = nlohmann::json::array();
    
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);
        for (uint64_t id : ctx->subscribed_streams) {
            json["subscribed_streams"].push_back(std::to_string(id));
        }
    }

    conn->send(json.dump());
}

void StreamViewerWebSocket::sendPublishResult(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& session_id,
    size_t published_meta_records,
    size_t published_marker_events)
{
    nlohmann::json json;
    json["type"] = "publish_result";
    json["request_id"] = request_id;
    json["session_id"] = session_id;
    json["published_meta_records"] = published_meta_records;
    json["published_marker_events"] = published_marker_events;
    conn->send(json.dump());
}

void StreamViewerWebSocket::sendTransformCapabilities(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json json;
    json["type"] = "transform_capabilities";
    json["request_id"] = request_id;
    json["transforms"] = buildTransformCapabilitiesJson();
    conn->send(json.dump());
}

void StreamViewerWebSocket::sendNodeCatalog(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json json;
    json["type"] = "node_catalog";
    json["request_id"] = request_id;
    json["nodes"] = buildNodeCatalogJson();
    conn->send(json.dump());
}

void StreamViewerWebSocket::sendTransformResult(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    uint64_t source_stream_id,
    uint64_t output_stream_id,
    const std::string& output_identifier,
    const std::string& transform_kind,
    const std::string& input_mapping_id,
    const std::string& topic,
    const std::string& worker_id,
    const std::string& thread_slot_id,
    size_t slot_capacity,
    size_t active_count,
    bool already_exists)
{
    nlohmann::json json;
    json["type"] = "transform_result";
    json["request_id"] = request_id;
    json["source_stream_id"] = std::to_string(source_stream_id);
    json["output_stream_id"] = std::to_string(output_stream_id);
    json["output_identifier"] = output_identifier;
    json["transform_kind"] = transform_kind;
    json["input_mapping_id"] = input_mapping_id;
    json["topic"] = topic;
    json["worker_id"] = worker_id;
    json["thread_slot_id"] = thread_slot_id;
    json["slot_capacity"] = slot_capacity;
    json["active_count"] = active_count;
    json["already_exists"] = already_exists;
    conn->send(json.dump());
}

void StreamViewerWebSocket::sendTransformList(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json response;
    response["type"] = "transform_list";
    response["request_id"] = request_id;
    response["worker_id"] = "natkit-local-transform-worker";
    response["slot_capacity"] = g_transform_slot_capacity;
    response["active_count"] = 0;
    response["available_slot_count"] = g_transform_slot_capacity;
    response["utilization_ratio"] = 0.0;
    response["last_heartbeat_us"] = nowUs();
    response["worker_status"] = "idle";
    response["transforms"] = nlohmann::json::array();

    {
        std::lock_guard<std::mutex> lock(g_transform_mutex);
        const size_t active_count = g_transform_workers.size();
        uint64_t last_heartbeat_us = 0;
        response["active_count"] = active_count;
        response["available_slot_count"] =
            g_transform_slot_capacity > active_count
                ? g_transform_slot_capacity - active_count
                : 0;
        if (g_transform_slot_capacity > 0) {
            response["utilization_ratio"] =
                static_cast<double>(active_count) /
                static_cast<double>(g_transform_slot_capacity);
        }
        for (const auto& entry : g_transform_workers) {
            const auto& worker = entry.second;
            if (!worker) {
                continue;
            }
            const uint64_t worker_heartbeat_us = worker->getLastFrameAtUs() > 0
                ? worker->getLastFrameAtUs()
                : worker->getStartedAtUs();
            if (worker_heartbeat_us > last_heartbeat_us) {
                last_heartbeat_us = worker_heartbeat_us;
            }
            nlohmann::json item;
            item["source_stream_id"] = std::to_string(worker->getSourceStreamId());
            item["output_stream_id"] = std::to_string(worker->getOutputStreamId());
            item["output_identifier"] = worker->getOutputIdentifier();
            item["transform_kind"] = worker->getTransformKind();
            item["input_mapping_id"] = worker->getInputMappingId();
            item["topic"] = worker->getOutputTopic();
            item["worker_id"] = "natkit-local-transform-worker";
            item["slot_index"] = worker->getSlotIndex();
            item["thread_slot_id"] = worker->getThreadSlotId();
            item["status"] = "running";
            item["started_at_us"] = worker->getStartedAtUs();
            item["last_frame_at_us"] = worker->getLastFrameAtUs();
            item["frames_processed"] = worker->getFramesProcessed();
            response["transforms"].push_back(item);
        }
        if (active_count == 0) {
            last_heartbeat_us = nowUs();
        }
        response["last_heartbeat_us"] = last_heartbeat_us;
        response["worker_status"] =
            classifyTransformWorkerStatus(active_count, last_heartbeat_us);
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::sendStreamGraphList(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json response;
    response["type"] = "stream_graph_list";
    response["request_id"] = request_id;
    response["graphs"] = nlohmann::json::array();
    response["statuses"] = nlohmann::json::object();

    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    for (const auto& entry : g_stream_graphs) {
        response["graphs"].push_back(entry.second);
        response["statuses"][entry.first] = makeGraphStatusJson(entry.second);
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::sendStreamGraphSaved(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const nlohmann::json& graph_json)
{
    nlohmann::json response;
    response["type"] = "stream_graph_saved";
    response["request_id"] = request_id;
    response["graph"] = graph_json;
    response["graph_id"] = graph_json.value("graph_id", std::string{});
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendProfileList(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json response;
    response["type"] = "profile_list";
    response["request_id"] = request_id;
    response["profiles"] = nlohmann::json::array();
    std::lock_guard<std::mutex> lock(g_profile_mutex);
    for (const auto& entry : g_profiles) {
        response["profiles"].push_back(entry.second);
    }
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendProfileSaved(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const nlohmann::json& profile_json)
{
    nlohmann::json response;
    response["type"] = "profile_saved";
    response["request_id"] = request_id;
    response["profile"] = profile_json;
    response["participant_id"] = profile_json.value("participant_id", std::string{});
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendProfileDeleted(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& participant_id)
{
    nlohmann::json response;
    response["type"] = "profile_deleted";
    response["request_id"] = request_id;
    response["participant_id"] = participant_id;
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendInstanceReplayState(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& replay_id,
    const std::string& graph_id,
    const natkit::tools::ReplayPlan& plan,
    const natkit::tools::ReplayProgress& progress,
    const std::string& state)
{
    nlohmann::json response =
        makeReplayStateJson(replay_id, graph_id, plan, progress, state);
    response["type"] = "instance_replay";
    response["request_id"] = request_id;
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

// Progress arrives from the replay thread, long after the request; broadcast it.
void StreamViewerWebSocket::broadcastInstanceReplayState(
    const std::string& replay_id,
    const std::string& graph_id,
    const natkit::tools::ReplayPlan& plan,
    const natkit::tools::ReplayProgress& progress,
    const std::string& state)
{
    nlohmann::json response =
        makeReplayStateJson(replay_id, graph_id, plan, progress, state);
    response["type"] = "instance_replay";
    response["request_id"] = "";
    const auto payload = response.dump();
    std::vector<WebSocketConnectionPtr> connections;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        connections.reserve(clients_.size());
        for (const auto& entry : clients_) {
            connections.push_back(entry.first);
        }
    }
    for (const auto& conn : connections) {
        if (conn && conn->connected()) {
            conn->send(payload);
        }
    }
}

void StreamViewerWebSocket::sendExperimentInstance(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const nlohmann::json& instance_json)
{
    nlohmann::json response;
    response["type"] = "experiment_instance";
    response["request_id"] = request_id;
    response["graph"] = instance_json;
    response["graph_id"] = instance_json.value("graph_id", std::string{});
    response["instance_id"] = instance_json.value("instance_id", std::string{});
    if (conn && conn->connected()) {
        conn->send(response.dump());
    }
}

// Materialization runs on its own thread and can finish long after the request
// that started it, so its outcome goes to EVERY client rather than back down the
// originating connection (which may be gone).
void StreamViewerWebSocket::broadcastExperimentInstance(
    const nlohmann::json& instance_json)
{
    nlohmann::json response;
    response["type"] = "experiment_instance";
    response["request_id"] = "";
    response["graph"] = instance_json;
    response["graph_id"] = instance_json.value("graph_id", std::string{});
    response["instance_id"] = instance_json.value("instance_id", std::string{});
    const auto payload = response.dump();

    std::vector<WebSocketConnectionPtr> connections;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        connections.reserve(clients_.size());
        for (const auto& entry : clients_) {
            connections.push_back(entry.first);
        }
    }
    for (const auto& conn : connections) {
        if (conn && conn->connected()) {
            conn->send(payload);
        }
    }
}

void StreamViewerWebSocket::sendExperimentList(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json response;
    response["type"] = "experiment_list";
    response["request_id"] = request_id;
    response["experiments"] = nlohmann::json::array();
    std::lock_guard<std::mutex> lock(g_experiment_mutex);
    for (const auto& entry : g_experiments) {
        response["experiments"].push_back(entry.second);
    }
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendExperimentSaved(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const nlohmann::json& experiment_json)
{
    nlohmann::json response;
    response["type"] = "experiment_saved";
    response["request_id"] = request_id;
    response["experiment"] = experiment_json;
    response["experiment_id"] = experiment_json.value("experiment_id", std::string{});
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendExperimentDeleted(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& experiment_id)
{
    nlohmann::json response;
    response["type"] = "experiment_deleted";
    response["request_id"] = request_id;
    response["experiment_id"] = experiment_id;
    conn->send(response.dump());
}

// --- Workspace handlers (TEC-NATKIT-56) ----------------------------------

void StreamViewerWebSocket::handleListWorkspaces(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    {
        std::lock_guard<std::mutex> lock(g_workspace_mutex);
        ensureWorkspaceStoreLoadedLocked();
        if (!g_workspace_store_error.empty()) {
            sendError(conn, g_workspace_store_error, request_id);
            return;
        }
    }
    sendWorkspaceList(conn, request_id);
}

void StreamViewerWebSocket::handleSaveWorkspace(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    if (!json.contains("workspace")) {
        sendError(conn, "save_workspace requires a workspace payload", request_id);
        return;
    }

    Workspace workspace;
    try {
        workspace = json.at("workspace").get<Workspace>();
    } catch (const std::exception& exception) {
        sendError(conn, "Malformed workspace: " + std::string(exception.what()),
                  request_id);
        return;
    }
    if (workspace.workspaceId.empty()) {
        sendError(conn, "save_workspace requires a non-empty workspace_id",
                  request_id);
        return;
    }

    std::string persist_error;
    nlohmann::json saved;
    {
        std::lock_guard<std::mutex> lock(g_workspace_mutex);
        ensureWorkspaceStoreLoadedLocked();
        if (!g_workspace_store_error.empty()) {
            sendError(conn, g_workspace_store_error, request_id);
            return;
        }
        // Preserve the original creation timestamp on update, as the experiment
        // store does: a rename must not look like a new workspace.
        const auto existing = g_workspaces.find(workspace.workspaceId);
        if (existing != g_workspaces.end() && existing->second.createdAtUs != 0) {
            workspace.createdAtUs = existing->second.createdAtUs;
        }
        if (workspace.createdAtUs == 0) {
            workspace.createdAtUs = nowUs();
        }
        workspace.updatedAtUs = nowUs();
        g_workspaces[workspace.workspaceId] = workspace;
        if (!persistWorkspaceStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
        saved = workspace;
    }
    sendWorkspaceSaved(conn, request_id, saved);
}

void StreamViewerWebSocket::handleDeleteWorkspace(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    const std::string request_id = json.value("request_id", std::string{});
    const std::string workspace_id = json.value("workspace_id", std::string{});
    if (workspace_id.empty()) {
        sendError(conn, "delete_workspace requires a workspace_id", request_id);
        return;
    }

    std::string persist_error;
    {
        std::lock_guard<std::mutex> lock(g_workspace_mutex);
        ensureWorkspaceStoreLoadedLocked();
        if (!g_workspace_store_error.empty()) {
            sendError(conn, g_workspace_store_error, request_id);
            return;
        }
        if (g_workspaces.find(workspace_id) == g_workspaces.end()) {
            sendError(conn, "No workspace found for workspace_id " + workspace_id,
                      request_id);
            return;
        }
        // Un-file the members BEFORE dropping the record: if this fails, the
        // workspace is still there and its members are still consistently filed,
        // rather than pointing at a workspace that no longer exists.
        if (!unfileWorkspaceMembersLocked(workspace_id, persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
        g_workspaces.erase(workspace_id);
        if (!persistWorkspaceStoreLocked(persist_error)) {
            sendError(conn, persist_error, request_id);
            return;
        }
    }
    sendWorkspaceDeleted(conn, request_id, workspace_id);
}

void StreamViewerWebSocket::sendWorkspaceList(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id)
{
    nlohmann::json response;
    response["type"] = "workspace_list";
    response["request_id"] = request_id;
    response["workspaces"] = nlohmann::json::array();
    std::lock_guard<std::mutex> lock(g_workspace_mutex);
    for (const auto& entry : g_workspaces) {
        response["workspaces"].push_back(entry.second);
    }
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendWorkspaceSaved(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const nlohmann::json& workspace_json)
{
    nlohmann::json response;
    response["type"] = "workspace_saved";
    response["request_id"] = request_id;
    response["workspace"] = workspace_json;
    response["workspace_id"] = workspace_json.value("workspace_id", std::string{});
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendWorkspaceDeleted(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& workspace_id)
{
    nlohmann::json response;
    response["type"] = "workspace_deleted";
    response["request_id"] = request_id;
    response["workspace_id"] = workspace_id;
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendStreamGraphValidation(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& graph_id,
    bool valid,
    const nlohmann::json& graph_diagnostics,
    const nlohmann::json& node_diagnostics,
    const nlohmann::json& edge_diagnostics)
{
    nlohmann::json response;
    response["type"] = "stream_graph_validation";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["valid"] = valid;
    response["graph_diagnostics"] = graph_diagnostics;
    response["node_diagnostics"] = node_diagnostics;
    response["edge_diagnostics"] = edge_diagnostics;
    conn->send(response.dump());
}

void StreamViewerWebSocket::sendStreamGraphStatus(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& graph_id)
{
    nlohmann::json response;
    response["type"] = "stream_graph_status";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["status"] = nlohmann::json::object();

    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    const auto search = g_stream_graphs.find(graph_id);
    if (search != g_stream_graphs.end()) {
        response["status"] = makeGraphStatusJson(search->second);
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::sendStreamGraphStarted(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& graph_id)
{
    nlohmann::json response;
    response["type"] = "stream_graph_started";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["graph_run_id"] = nullptr;
    response["node_statuses"] = nlohmann::json::object();

    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    const auto runtime_search = g_stream_graph_runtime.find(graph_id);
    if (runtime_search != g_stream_graph_runtime.end()) {
        response["graph_run_id"] = runtime_search->second.activeRunId;
        for (const auto& entry : runtime_search->second.nodeStatuses) {
            response["node_statuses"][entry.first] = entry.second;
        }
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::sendStreamGraphStopped(
    const WebSocketConnectionPtr& conn,
    const std::string& request_id,
    const std::string& graph_id)
{
    nlohmann::json response;
    response["type"] = "stream_graph_stopped";
    response["request_id"] = request_id;
    response["graph_id"] = graph_id;
    response["graph_run_id"] = nullptr;
    response["node_statuses"] = nlohmann::json::object();

    std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
    const auto runtime_search = g_stream_graph_runtime.find(graph_id);
    if (runtime_search != g_stream_graph_runtime.end()) {
        response["graph_run_id"] =
            runtime_search->second.activeRunId.empty()
                ? nlohmann::json(nullptr)
                : nlohmann::json(runtime_search->second.activeRunId);
        for (const auto& entry : runtime_search->second.nodeStatuses) {
            response["node_statuses"][entry.first] = entry.second;
        }
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::broadcastTransformList()
{
    std::vector<WebSocketConnectionPtr> connections;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        connections.reserve(clients_.size());
        for (const auto& entry : clients_) {
            connections.push_back(entry.first);
        }
    }
    for (const auto& conn : connections) {
        sendTransformList(conn, std::string{});
    }
}

// --- ML control-plane proxy (Phase 5, decision #3) -------------------------

void StreamViewerWebSocket::broadcastMlControlPlaneMessage(
    const std::string& raw_message)
{
    // Wrap so the control-plane message can't collide with stream_viewer's own
    // message types, then fan it out to every connected browser client.
    nlohmann::json envelope;
    envelope["type"] = "ml_control_plane";
    try {
        envelope["message"] = nlohmann::json::parse(raw_message);
    } catch (const std::exception&) {
        return;  // ignore non-JSON frames
    }
    const std::string payload = envelope.dump();

    std::vector<WebSocketConnectionPtr> connections;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        connections.reserve(clients_.size());
        for (const auto& entry : clients_) {
            connections.push_back(entry.first);
        }
    }
    for (const auto& conn : connections) {
        if (conn && conn->connected()) {
            conn->send(payload);
        }
    }
}

void StreamViewerWebSocket::ensureMlControlPlaneClient()
{
    // The lock is held ONLY while inspecting/updating the client state. It must not
    // still be held when connectToServer() is called below:
    // WebSocketClient::connectToServer invokes its completion callback SYNCHRONOUSLY
    // when the connection fails outright (an unreachable control plane), and that
    // callback re-locks this same non-recursive mutex. Holding it across the call
    // self-deadlocked the thread, and because every ml_proxy action starts by calling
    // this function, the deadlock then consumed each drogon event loop in turn until
    // the whole backend stopped answering -- HTTP included. Restarting the control
    // plane container was enough to trigger it.
    std::string url;
    std::string service_user;
    drogon::WebSocketClientPtr client;
    {
    std::lock_guard<std::mutex> lock(ml_client_mutex_);
    if (ml_client_ && ml_client_->getConnection() &&
        ml_client_->getConnection()->connected()) {
        return;
    }
    if (ml_client_connecting_) {
        return;
    }
    ml_client_connecting_ = true;

    const char* url_env = std::getenv("NATKIT_ML_CONTROL_PLANE_URL");
    url = (url_env != nullptr && std::string(url_env).size() > 0)
              ? std::string(url_env)
              : std::string("ws://127.0.0.1:8786");
    const char* user_env = std::getenv("NATKIT_ML_PROXY_USERNAME");
    service_user =
        (user_env != nullptr && std::string(user_env).size() > 0)
            ? std::string(user_env)
            : std::string("admin");

    client = drogon::WebSocketClient::newWebSocketClient(url);
    ml_client_ = client;
    }  // ml_client_mutex_ released before any callback can fire

    const auto token = AuthManager::instance().createServiceSession(service_user);
    if (!token.has_value()) {
        LOG_ERROR << "ML proxy: could not mint a service session for '"
                  << service_user << "'; control-plane proxy unavailable.";
        std::lock_guard<std::mutex> lock(ml_client_mutex_);
        ml_client_connecting_ = false;
        return;
    }

    client->setMessageHandler(
        [this](std::string&& message,
               const drogon::WebSocketClientPtr&,
               const drogon::WebSocketMessageType& msg_type) {
            if (msg_type == drogon::WebSocketMessageType::Text) {
                broadcastMlControlPlaneMessage(message);
            }
        });

    client->setConnectionClosedHandler(
        [this](const drogon::WebSocketClientPtr&) {
            LOG_WARN << "ML proxy: control-plane connection closed; reconnecting.";
            {
                std::lock_guard<std::mutex> relock(ml_client_mutex_);
                ml_client_connecting_ = false;
            }
            if (auto* loop = drogon::app().getLoop()) {
                loop->runAfter(2.0, [this]() { ensureMlControlPlaneClient(); });
            }
        });

    auto req = drogon::HttpRequest::newHttpRequest();
    req->setPath("/");
    req->addHeader("Cookie",
                   AuthManager::instance().cookieName() + "=" + token.value());

    LOG_INFO << "ML proxy: connecting to control plane at " << url;
    client->connectToServer(
        req,
        [this](drogon::ReqResult result,
               const drogon::HttpResponsePtr&,
               const drogon::WebSocketClientPtr&) {
            std::lock_guard<std::mutex> relock(ml_client_mutex_);
            ml_client_connecting_ = false;
            if (result != drogon::ReqResult::Ok) {
                LOG_ERROR << "ML proxy: control-plane connect failed ("
                          << static_cast<int>(result) << "); retrying.";
                if (auto* loop = drogon::app().getLoop()) {
                    loop->runAfter(2.0,
                                   [this]() { ensureMlControlPlaneClient(); });
                }
                return;
            }
            LOG_INFO << "ML proxy: connected to control plane.";
        });
}

void StreamViewerWebSocket::handleMlProxyAction(
    const WebSocketConnectionPtr& conn,
    const nlohmann::json& json)
{
    ensureMlControlPlaneClient();

    if (!json.contains("message") || !json["message"].is_object()) {
        sendError(conn, "ml_proxy requires an object 'message' payload");
        return;
    }

    drogon::WebSocketClientPtr client;
    {
        std::lock_guard<std::mutex> lock(ml_client_mutex_);
        client = ml_client_;
    }
    if (!client || !client->getConnection() ||
        !client->getConnection()->connected()) {
        // Connect is async; tell the browser it can retry (the MlPipeline UI
        // already polls list_* on an interval, so it recovers on the next tick).
        nlohmann::json envelope;
        envelope["type"] = "ml_control_plane";
        envelope["message"] = {
            {"type", "error"},
            {"error", "ML control plane is connecting; retry shortly."},
            {"request_id", json["message"].value("request_id", std::string{})}};
        conn->send(envelope.dump());
        return;
    }
    // Instance-backed training (Phase 6): the browser names INSTANCES by graph id;
    // the backend swaps them for their materialized artifact paths before the job
    // leaves. It does that here because the backend owns the instance store — the
    // browser has no business knowing container paths, and the control plane has no
    // business reading the graph store.
    nlohmann::json message = json["message"];
    if (message.value("action", std::string{}) == "start_train_validate_job") {
        std::string resolve_error;
        if (!resolveTrainInstanceDatasets(message, resolve_error)) {
            sendError(conn, resolve_error,
                      message.value("request_id", std::string{}));
            return;
        }
    }
    client->getConnection()->send(message.dump());
}

// Replace `train_instances`/`eval_instances` entries (instance graph ids) with the
// concrete dataset the trainer needs: the Parquet + markers paths, plus the
// provenance that makes an instance a better training input than a run selector.
//
// Refuses rather than silently degrading: a job that quietly trains on nothing, or
// on a different recording than the operator picked, is worse than one that fails.
bool StreamViewerWebSocket::resolveTrainInstanceDatasets(
    nlohmann::json& message, std::string& error)
{
    const auto resolveList = [&](const char* key) -> bool {
        if (!message.contains(key) || !message[key].is_array()) {
            return true;
        }
        nlohmann::json resolved = nlohmann::json::array();
        for (const auto& entry : message[key]) {
            // Already-resolved entries (an object with a parquet path) pass through.
            if (entry.is_object() && entry.contains("parquet")) {
                resolved.push_back(entry);
                continue;
            }
            const auto graph_id = entry.is_string()
                                      ? entry.get<std::string>()
                                      : entry.value("graph_id", std::string{});
            if (graph_id.empty()) {
                error = "train_instances entries must be an instance graph_id";
                return false;
            }

            std::string experiment_id;
            std::string instance_id;
            nlohmann::json recording;
            {
                std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
                ensureStreamGraphStoreLoadedLocked();
                const auto stored = g_stream_graphs.find(graph_id);
                if (stored == g_stream_graphs.end()) {
                    error = "Unknown instance: " + graph_id;
                    return false;
                }
                if (stored->second.instanceId.empty()) {
                    error = "'" + graph_id +
                            "' is a live board, not a recorded instance — there are "
                            "no materialized files to train on.";
                    return false;
                }
                experiment_id = stored->second.experimentId;
                instance_id = stored->second.instanceId;
                recording = stored->second.recording;
            }

            const auto status = recording.value("status", std::string{});
            if (status != "complete") {
                error = "Instance " + instance_id + " is '" + status +
                        "', not complete — it has no verified artifacts to train on.";
                return false;
            }
            const auto artifacts =
                recording.value("artifacts", nlohmann::json::object());
            const auto directory = std::filesystem::path(artifacts.value(
                "directory",
                instanceArtifactDir(experiment_id, instance_id).string()));
            const auto data = artifacts.value("data", nlohmann::json::array());
            if (data.empty()) {
                error = "Instance " + instance_id + " lists no data artifacts.";
                return false;
            }
            // One entry per recorded source: a multi-sensor recording trains on all
            // of them, which is what recording them together was for.
            for (const auto& artifact : data) {
                nlohmann::json dataset;
                dataset["session_id"] = recording.value("session_id", experiment_id);
                dataset["instance_id"] = instance_id;
                dataset["graph_id"] = graph_id;
                dataset["run_index"] = static_cast<int>(resolved.size() + 1);
                dataset["parquet"] =
                    (directory / artifact.value("path", std::string{})).string();
                if (artifacts.contains("markers")) {
                    dataset["markers"] =
                        (directory / artifacts.value("markers", std::string{}))
                            .string();
                }
                dataset["window_start_us"] =
                    recording.value("window_start_us", static_cast<int64_t>(0));
                dataset["window_end_us"] =
                    recording.value("window_end_us", static_cast<int64_t>(0));
                dataset["device_id"] = artifact.value("stream_id", std::string{});
                dataset["rows"] = artifact.value("rows", 0);
                dataset["sha256"] = artifact.value("sha256", std::string{});
                resolved.push_back(std::move(dataset));
            }
        }
        message[key] = std::move(resolved);
        return true;
    };
    return resolveList("train_instances") && resolveList("eval_instances");
}

// --- Cohort export input collection (TEC-NATKIT-411) ----------------------
//
// Lives here because this translation unit owns the workspace, experiment and
// graph stores. The archive building itself is in CohortExport.cpp, which is pure
// and therefore testable without any of them.
namespace natkit::tools {

CohortExportInputs collectCohortInputs(const std::string& workspaceId,
                                       std::string& error)
{
    CohortExportInputs inputs;
    inputs.workspaceId = workspaceId;

    // Lock order is workspace -> experiment -> graph, as established by
    // unfileWorkspaceMembersLocked. Nothing here takes them the other way round.
    std::lock_guard<std::mutex> workspace_lock(g_workspace_mutex);
    ensureWorkspaceStoreLoadedLocked();
    if (!g_workspace_store_error.empty()) {
        error = g_workspace_store_error;
        return inputs;
    }
    if (!workspaceId.empty()) {
        const auto workspace = g_workspaces.find(workspaceId);
        if (workspace == g_workspaces.end()) {
            error = "No workspace found for workspace_id " + workspaceId;
            return inputs;
        }
        inputs.workspaceLabel = workspace->second.label;
    } else {
        // The empty id is the Unfiled pseudo-workspace, which is a real view and
        // exportable like any other -- everything recorded before workspaces
        // existed lives there.
        inputs.workspaceLabel = "Unfiled";
    }

    std::lock_guard<std::mutex> experiment_lock(g_experiment_mutex);
    ensureExperimentStoreLoadedLocked();
    if (!g_experiment_store_error.empty()) {
        error = g_experiment_store_error;
        return inputs;
    }
    std::set<std::string> experiment_ids;
    for (const auto& entry : g_experiments) {
        if (entry.second.workspaceId == workspaceId) {
            experiment_ids.insert(entry.first);
        }
    }

    std::lock_guard<std::mutex> graph_lock(g_stream_graph_mutex);
    ensureStreamGraphStoreLoadedLocked();
    if (!g_stream_graph_store_error.empty()) {
        error = g_stream_graph_store_error;
        return inputs;
    }

    // Deterministic order (TEC-NATKIT-411 asks for reproducible re-export), so walk
    // a sorted view rather than the hash map's iteration order.
    std::vector<const StreamGraphDefinition*> instances;
    for (const auto& entry : g_stream_graphs) {
        const auto& graph = entry.second;
        if (graph.instanceId.empty()) continue;  // a live board is not a run
        if (experiment_ids.count(graph.experimentId) == 0) continue;
        instances.push_back(&graph);
    }
    std::sort(instances.begin(), instances.end(),
              [](const StreamGraphDefinition* left, const StreamGraphDefinition* right) {
                  if (left->experimentId != right->experimentId) {
                      return left->experimentId < right->experimentId;
                  }
                  return left->instanceId < right->instanceId;
              });

    for (const auto* graph : instances) {
        const auto& recording = graph->recording;
        const auto status = recording.value("status", std::string{});
        if (status != "complete") {
            // ⚠️ Named, never dropped: an in-flight or failed run absent from the
            // archive with no explanation reads as a cohort that never had it.
            inputs.skips.push_back({graph->experimentId, graph->instanceId,
                                    status.empty() ? "no recording status"
                                                   : "status is " + status});
            continue;
        }

        CohortInstance instance;
        instance.experimentId = graph->experimentId;
        instance.instanceId = graph->instanceId;
        instance.participantId = recording.value("participant_id", std::string{});
        instance.participantBackfilled = recording.value("participant_backfilled", false);
        instance.participantUnrecorded = recording.value("participant_unrecorded", false);
        instance.calibrationOverride =
            recording.value("calibration_override", std::string{});
        // The clock record, summarised for the manifest (TEC-NATKIT-77). ⚠️ Left
        // EMPTY when the run has no clock_quality, which is what every run made
        // before this existed looks like — and empty is documented in the manifest
        // header as "predates the record", explicitly NOT as clean.
        if (recording.contains("clock_quality") &&
            recording["clock_quality"].is_object()) {
            const auto& devices =
                recording["clock_quality"].value("devices", nlohmann::json::array());
            size_t troubled = 0;
            for (const auto& device : devices) {
                const auto status = device.value("status", std::string{});
                if (status != "reported" || !device.value("valid", true) ||
                    device.value("epoch_changed_during_run", false)) {
                    troubled += 1;
                }
            }
            if (devices.empty()) {
                instance.clockSummary = "no devices";
            } else if (troubled == 0) {
                instance.clockSummary = "held";
            } else {
                instance.clockSummary =
                    std::to_string(troubled) + " of " +
                    std::to_string(devices.size()) + " in question";
            }
        }
        if (recording.contains("sensor_positions") &&
            recording["sensor_positions"].is_array()) {
            for (const auto& entry : recording["sensor_positions"]) {
                instance.sensorPositions.emplace_back(
                    entry.value("stream_id", std::string{}),
                    entry.value("position", std::string{}));
            }
        }

        const auto artifacts = recording.value("artifacts", nlohmann::json::object());
        instance.totalRows = artifacts.value("total_rows", static_cast<uint64_t>(0));
        const auto directory = artifacts.value("directory", std::string{});
        if (directory.empty()) {
            inputs.skips.push_back({graph->experimentId, graph->instanceId,
                                    "complete but records no artifact directory"});
            continue;
        }

        // ⚠️ `path` is a FILE NAME relative to `directory`, not an absolute path.
        // Reading it as absolute makes every artifact look missing, which is a
        // mistake already made once while verifying TEC-NATKIT-54.
        const std::string prefix =
            (instance.participantId.empty() ? std::string("unattributed")
                                            : instance.participantId) +
            "/" + graph->experimentId + "/" + graph->instanceId + "/";

        for (const auto& file : artifacts.value("data", nlohmann::json::array())) {
            const auto name = file.value("path", std::string{});
            if (name.empty()) continue;
            CohortArtifact artifact;
            artifact.absolutePath = (std::filesystem::path(directory) / name).string();
            artifact.archivePath = prefix + name;
            artifact.recordedSha256 = file.value("sha256", std::string{});
            artifact.truncated = file.value("truncated", false);
            instance.artifacts.push_back(std::move(artifact));
        }
        const auto markers = artifacts.value("markers", std::string{});
        if (!markers.empty()) {
            CohortArtifact artifact;
            artifact.absolutePath = (std::filesystem::path(directory) / markers).string();
            artifact.archivePath = prefix + markers;
            artifact.recordedSha256 = artifacts.value("markers_sha256", std::string{});
            instance.artifacts.push_back(std::move(artifact));
        }

        if (instance.artifacts.empty()) {
            inputs.skips.push_back({graph->experimentId, graph->instanceId,
                                    "complete but lists no artifacts"});
            continue;
        }
        inputs.instances.push_back(std::move(instance));
    }

    return inputs;
}

}  // namespace natkit::tools
