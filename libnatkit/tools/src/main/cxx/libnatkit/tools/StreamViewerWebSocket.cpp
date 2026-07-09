#include "StreamViewerWebSocket.hpp"

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
#include <regex>
#include <sstream>
#include <deque>
#include <unordered_map>
#include <unordered_set>
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
    const uint64_t stream_id =
        nat::core::stableStreamId(topic_namespace, identifier);
    const std::string topic =
        nat::core::toString(stream_type) + "-" + std::to_string(stream_id) +
        "-Json-" + schema_name;
    auto topic_info_maybe = nat::core::BasicTopicInformation::create(topic);
    if (!topic_info_maybe.has_value()) {
        return nullptr;
    }

    std::unique_ptr<nat::core::BasicTopicInformation> topic_info_unique =
        std::move(topic_info_maybe.value());
    return std::shared_ptr<nat::core::BasicTopicInformation>(
        topic_info_unique.release());
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
                {"TP10", "eeg.tp10"}}}};
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
    } else if (config.kind == "lda_classify") {
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
        node["input_ports"] = singleInputPort();
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

    // Session — records N upstream sensor streams under one protocol/marker
    // timeline and publishes a labeled session bundle (client-driven). Produces
    // no output stream; its protocol config lives on the node.
    nodes.push_back(
        {{"node_type", "session"},
         {"kind", "session"},
         {"category", "session"},
         {"runner", "frontend"},
         {"label", "Session"},
         {"description",
          "Records one or more upstream sensor streams under a structured "
          "protocol (ordered cues + labels), producing a labeled dataset for "
          "training."},
         {"config_fields", nlohmann::json::array()},
         {"input_ports", singleInputPort()},
         {"output_ports", nlohmann::json::array()},
         {"variadic_inputs", true}});

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
};

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

struct StreamGraphNodeRuntimeStatus {
    std::string state{"draft"};
    std::optional<uint64_t> outputStreamId{};
    std::optional<std::string> workerId{};
    std::optional<std::string> threadSlotId{};
    uint64_t framesProcessed = 0;
    uint64_t lastFrameAtUs = 0;
    std::optional<std::string> message{};
};

struct StreamGraphRuntimeState {
    std::string graphId{};
    std::string activeRunId{};
    std::string runState{"stopped"};
    std::unordered_map<std::string, StreamGraphNodeRuntimeStatus> nodeStatuses{};
    std::vector<uint64_t> outputStreamIds{};
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
}

void from_json(const nlohmann::json& json, StreamGraphEdge& value)
{
    value.id = json.at("id").get<std::string>();
    value.sourceNodeId = json.at("source_node_id").get<std::string>();
    value.sourcePort = json.at("source_port").get<std::string>();
    value.targetNodeId = json.at("target_node_id").get<std::string>();
    value.targetPort = json.at("target_port").get<std::string>();
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
    value.notes = json.value("notes", std::vector<std::string>{});
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
    if (node.kind == "stream_source") {
        if (node.outputPortIds.empty()) {
            node.outputPortIds = {"data"};
        }
        node.inputPortIds.clear();
    } else if (node.kind == "transform") {
        if (node.inputPortIds.empty()) {
            node.inputPortIds = {"input"};
        }
        if (node.outputPortIds.empty()) {
            node.outputPortIds = {"output"};
        }
        if (node.config.is_null()) {
            node.config = nlohmann::json::object();
        }
    } else if (node.kind == "viewer" || node.kind == "sink") {
        if (node.inputPortIds.empty()) {
            node.inputPortIds = {"input"};
        }
        node.outputPortIds.clear();
    } else if (node.kind == "session") {
        // A session node records N upstream sensor streams under one protocol;
        // it may have several input ports and never produces an output stream.
        // Multiple input ports (set by the frontend) are preserved here.
        if (node.inputPortIds.empty()) {
            node.inputPortIds = {"in1"};
        }
        node.outputPortIds.clear();
        if (node.config.is_null()) {
            node.config = nlohmann::json::object();
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
            json["node_statuses"][entry.first] = entry.second;
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
            if (!value.is_string() || value.get_ref<const std::string&>().empty()) {
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

StreamGraphValidationResult validateStreamGraphDefinition(
    const StreamGraphDefinition& graph,
    std::shared_ptr<nat::kafka::BrokerManager> broker_manager)
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
            node.kind != "combine" && node.kind != "session") {
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
        } else if (node.kind == "session") {
            // A session node records its upstream sensor streams; it produces
            // markers/metadata (published client-side), never an output stream.
            if (!node.outputPortIds.empty()) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "invalid_session_output_ports",
                    "session nodes do not expose output ports.");
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
                    return edge.targetNodeId == node.id;
                });
            if (input_count < 2) {
                addGraphDiagnostic(
                    result,
                    result.nodeDiagnostics[node.id],
                    "too_few_inputs",
                    "combine nodes require at least two connected inputs.");
            }

            bool all_inputs_resolved = true;
            for (const auto& edge : graph.edges) {
                if (edge.targetNodeId != node.id) {
                    continue;
                }
                const auto descriptor_search =
                    resolved_output_descriptors.find(edge.sourceNodeId);
                if (descriptor_search == resolved_output_descriptors.end() ||
                    descriptor_search->second == nullptr ||
                    !descriptorSupportsNumericChannelFrame(*descriptor_search->second)) {
                    all_inputs_resolved = false;
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "unresolved_input_descriptor",
                        "Upstream descriptor could not be resolved for one of combine's inputs.");
                }
            }

            if (all_inputs_resolved) {
                auto output_descriptor_maybe =
                    nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                        nat::core::NatSignalFrameDataSchemaV1::name);
                if (output_descriptor_maybe.has_value()) {
                    resolved_output_descriptors[node.id] = output_descriptor_maybe.value();
                    node_output_schema_names[node.id] =
                        std::string(nat::core::NatSignalFrameDataSchemaV1::name);
                }
            }
            continue;
        }

        if (node.kind != "transform") {
            if (node.kind == "viewer" || node.kind == "sink") {
                const auto input_count = std::count_if(
                    graph.edges.begin(),
                    graph.edges.end(),
                    [&node](const StreamGraphEdge& edge) {
                        return edge.targetNodeId == node.id;
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
            } else if (node.kind == "session") {
                // A session records one or more sensor streams — at least one
                // input must be connected; there is no upper bound.
                const auto input_count = std::count_if(
                    graph.edges.begin(),
                    graph.edges.end(),
                    [&node](const StreamGraphEdge& edge) {
                        return edge.targetNodeId == node.id;
                    });
                if (input_count == 0) {
                    addGraphDiagnostic(
                        result,
                        result.nodeDiagnostics[node.id],
                        "missing_input",
                        "session node must record at least one connected stream.");
                }
            }
            continue;
        }

        const auto input_count = std::count_if(
            graph.edges.begin(),
            graph.edges.end(),
            [&node](const StreamGraphEdge& edge) {
                return edge.targetNodeId == node.id;
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
                return edge.targetNodeId == node.id;
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
            if (!topic ||
                topic->serializationType != nat::core::SerializationType::Json) {
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
    const std::optional<std::string>& graph_node_id = std::nullopt)
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

    const auto source_topic =
        findTransformSourceTopicForStream(broker_manager, source_stream_id);
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

    auto source_messenger = broker_manager->createMessenger(source_topic);
    auto output_messenger = broker_manager->createMessenger(output_topic);
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

class CombineWorker {
public:
    CombineWorker(
        const std::string& output_identifier,
        size_t slot_index,
        std::vector<CombineInputState>&& inputs,
        const std::shared_ptr<nat::core::BasicTopicInformation>& output_topic,
        std::unique_ptr<nat::core::TopicMessenger>&& output_messenger)
        : outputIdentifier(output_identifier),
          slotIndex(slot_index),
          inputs(std::move(inputs)),
          outputTopic(output_topic),
          outputMessenger(std::move(output_messenger))
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
        return outputTopic ? outputTopic->id : 0;
    }

    std::string getOutputTopic() const
    {
        return outputTopic ? outputTopic->toTopicString() : std::string{};
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

    std::string outputIdentifier;
    size_t slotIndex;
    std::vector<CombineInputState> inputs;
    std::shared_ptr<nat::core::BasicTopicInformation> outputTopic;
    std::unique_ptr<nat::core::TopicMessenger> outputMessenger;
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

                bool all_ready = !inputs.empty();
                for (const auto& input : inputs) {
                    if (input.queue.empty()) {
                        all_ready = false;
                        break;
                    }
                }

                if (all_ready) {
                    std::vector<NormalizedNumericChannelFrame> aligned{};
                    aligned.reserve(inputs.size());
                    for (auto& input : inputs) {
                        aligned.push_back(input.queue.front());
                        input.queue.pop_front();
                    }
                    outputMessenger->sendMessage(concatenate(aligned));
                    framesProcessed.fetch_add(1);
                    lastFrameAtUs.store(nowUs());
                    made_progress = true;
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
};

CreateCombineWorkerResult createCombineWorker(
    const std::shared_ptr<nat::kafka::BrokerManager>& broker_manager,
    const std::vector<uint64_t>& source_stream_ids,
    const std::string& output_identifier)
{
    CreateCombineWorkerResult result;
    result.outputIdentifier = output_identifier;

    if (!broker_manager) {
        result.error = "Broker manager not available";
        return result;
    }
    if (source_stream_ids.size() < 2U) {
        result.error = "combine nodes require at least two connected inputs";
        return result;
    }

    const auto output_topic = createTopicInfo(
        nat::core::StreamType::DATA,
        "combine",
        output_identifier,
        nat::core::NatSignalFrameDataSchemaV1::name);
    if (output_topic == nullptr) {
        result.error = "Failed to create Kafka topic information for combine";
        return result;
    }
    const uint64_t output_stream_id = output_topic->id;
    result.outputStreamId = output_stream_id;

    {
        std::lock_guard<std::mutex> lock(g_combine_mutex);
        const auto search = g_combine_workers.find(output_stream_id);
        if (search != g_combine_workers.end()) {
            result.ok = true;
            result.alreadyExists = true;
            result.threadSlotId = search->second->getThreadSlotId();
            return result;
        }
    }

    std::vector<CombineInputState> inputs{};
    inputs.reserve(source_stream_ids.size());
    for (const auto source_stream_id : source_stream_ids) {
        const auto source_topic =
            findTransformSourceTopicForStream(broker_manager, source_stream_id);
        if (source_topic == nullptr) {
            result.error =
                "Could not locate a compatible JSON numeric channel topic for one of combine's source streams";
            return result;
        }
        auto descriptor_maybe =
            nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
                source_topic->schemaName);
        if (!descriptor_maybe.has_value()) {
            result.error =
                "No descriptor is available for one of combine's source streams";
            return result;
        }

        CombineInputState input{};
        input.sourceStreamId = source_stream_id;
        input.sourceTopic = source_topic;
        input.sourceMessenger = broker_manager->createMessenger(source_topic);
        input.descriptorMaybe = descriptor_maybe.value();
        inputs.push_back(std::move(input));
    }

    auto output_messenger = broker_manager->createMessenger(output_topic);
    std::shared_ptr<CombineWorker> worker;
    {
        std::lock_guard<std::mutex> lock(g_combine_mutex);
        const auto duplicate = g_combine_workers.find(output_stream_id);
        if (duplicate != g_combine_workers.end()) {
            result.ok = true;
            result.alreadyExists = true;
            result.threadSlotId = duplicate->second->getThreadSlotId();
            return result;
        }

        const size_t slot_index = g_combine_workers.size();
        worker = std::make_shared<CombineWorker>(
            output_identifier,
            slot_index,
            std::move(inputs),
            output_topic,
            std::move(output_messenger));
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
            handleSubscribe(conn, ctx, stream_ids);
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
                                             const std::vector<uint64_t>& stream_ids)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(ctx->mutex);

        // Get all available streams
        auto rawStreams = broker_manager_->getAllStreams();

        for (uint64_t stream_id : stream_ids) {
            if (ctx->subscribed_streams.count(stream_id) > 0) {
                continue; // Already subscribed
            }

            // Find the stream and create messengers for its DATA topics
            for (const auto& stream : rawStreams) {
                if (stream->getId() == stream_id) {
                    auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
                    auto dataTopic = choosePreferredDataTopic(dataTopics);
                    if (dataTopic != nullptr) {
                        ctx->messengers.push_back(broker_manager_->createMessenger(dataTopic));
                    }
                    break;
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

        // Start streaming thread if not already running
        if (!ctx->streaming_thread.joinable() && !ctx->subscribed_streams.empty()) {
            ctx->active = true;
            ctx->streaming_thread = std::thread(&StreamViewerWebSocket::streamingThreadFunc, this, conn, ctx);
        }
    } // Release lock before calling sendStatus

    sendStatus(conn, ctx);
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
            "transform_kind must be one of: rectify, lowpass_envelope, bandpass_iir, notch_iir, rms_window, sliding_window, highpass_iir, mav, rms, wl, zc, ssc, ar_coeffs, lda_classify");
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
void executeStreamGraphStart(
    std::shared_ptr<nat::kafka::BrokerManager> broker_manager,
    WebSocketConnectionPtr conn,
    std::string request_id,
    StreamGraphDefinition graph,
    std::string active_run_id)
{
    std::unordered_map<std::string, const StreamGraphNode*> nodes_by_id;
    std::unordered_map<std::string, StreamGraphNode*> mutable_nodes_by_id;
    std::unordered_map<std::string, uint64_t> resolved_output_stream_ids;
    for (auto& node : graph.nodes) {
        nodes_by_id[node.id] = &node;
        mutable_nodes_by_id[node.id] = &node;
        if (node.kind == "stream_source" && node.streamId.has_value()) {
            resolved_output_stream_ids[node.id] = node.streamId.value();
        }
    }

    struct ResolvedInput {
        std::optional<uint64_t> streamId;
        std::optional<std::string> upstreamNodeId;
    };

    const auto resolveInputStreamId = [&](const StreamGraphNode& node) -> ResolvedInput {
        const auto upstream_edge = std::find_if(
            graph.edges.begin(),
            graph.edges.end(),
            [&node](const StreamGraphEdge& edge) {
                return edge.targetNodeId == node.id;
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
        if (node.kind == "session") {
            // Recording is driven client-side (the browser runs the protocol
            // timeline and publishes the session bundle via
            // publish_session_bundle). The runtime just marks the node ready;
            // it consumes its upstream streams but produces no output stream.
            StreamGraphNodeRuntimeStatus status{
                "running",
                std::nullopt,
                std::nullopt,
                std::nullopt,
                0,
                0,
                std::optional<std::string>(
                    "Session node is ready to record its upstream streams.")};
            if (!commitNodeStatus(node.id, status, std::nullopt)) {
                aborted = true;
                break;
            }
            pushStreamGraphStatusMessage(conn, request_id, graph.graphId);
            continue;
        }
        if (node.kind == "combine") {
            std::vector<uint64_t> input_stream_ids{};
            std::optional<std::string> blocking_upstream{};
            bool all_resolved = true;
            for (const auto& edge : graph.edges) {
                if (edge.targetNodeId != node.id) {
                    continue;
                }
                const auto upstream_node_search = nodes_by_id.find(edge.sourceNodeId);
                std::optional<uint64_t> resolved_stream_id{};
                if (upstream_node_search != nodes_by_id.end() &&
                    upstream_node_search->second->kind == "stream_source" &&
                    upstream_node_search->second->streamId.has_value()) {
                    resolved_stream_id = upstream_node_search->second->streamId.value();
                } else {
                    const auto resolved_search =
                        resolved_output_stream_ids.find(edge.sourceNodeId);
                    if (resolved_search != resolved_output_stream_ids.end()) {
                        resolved_stream_id = resolved_search->second;
                    }
                }
                if (!resolved_stream_id.has_value()) {
                    all_resolved = false;
                    blocking_upstream = edge.sourceNodeId;
                    break;
                }
                input_stream_ids.push_back(resolved_stream_id.value());
            }

            if (!all_resolved || input_stream_ids.size() < 2U) {
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
                broker_manager, input_stream_ids, node.outputIdentifier.value_or(node.id));
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
            const StreamGraphNodeRuntimeStatus status{
                "running",
                create_result.outputStreamId,
                create_result.workerId,
                create_result.threadSlotId,
                0, 0,
                create_result.alreadyExists
                    ? std::optional<std::string>("Reused existing combine worker.")
                    : std::nullopt,
            };
            if (!commitNodeStatus(node.id, status, create_result.outputStreamId)) {
                aborted = true;
                break;
            }
            resolved_output_stream_ids[node.id] = create_result.outputStreamId;
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

        const auto create_result = createTransformWorker(
            broker_manager,
            resolved_input.streamId.value(),
            node.outputIdentifier.value_or(node.id),
            config_maybe.value(),
            node.inputMappingId.value_or(std::string{}),
            graph.graphId,
            active_run_id,
            node.id);
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
        const StreamGraphNodeRuntimeStatus status{
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
        if (!commitNodeStatus(node.id, status, create_result.outputStreamId)) {
            aborted = true;
            break;
        }
        resolved_output_stream_ids[node.id] = create_result.outputStreamId;
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

            g_stream_graphs[graph.graphId] = graph;
            std::string persist_error;
            if (!persistStreamGraphStoreLocked(persist_error)) {
                LOG_ERROR << "Failed to persist stream graph runtime outputs for "
                          << graph.graphId << ": " << persist_error;
            }
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
        if (runtime_search != g_stream_graph_runtime.end() &&
            runtime_search->second.runState != "stopped") {
            sendError(conn, "Graph is already running; stop it before starting again");
            return;
        }
        graph = search->second;
    }

    const auto validation = validateStreamGraphDefinition(graph, broker_manager_);
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

    // Seed a "starting" runtime up front — sources are immediately available,
    // every other node is pending — so a status snapshot reflects the click at
    // once. The slow per-node worker creation then runs on a detached thread
    // (executeStreamGraphStart) that streams incremental status updates. Doing
    // this inline previously froze the WebSocket handler for the full Kafka
    // topic-creation time (tens of seconds) before the client saw anything.
    StreamGraphRuntimeState runtime;
    runtime.graphId = graph.graphId;
    runtime.activeRunId = graph.graphId + ":" + std::to_string(nowUs());
    runtime.runState = "starting";
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
        } else {
            runtime.nodeStatuses[node.id] = StreamGraphNodeRuntimeStatus{
                "starting",
                std::nullopt,
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

    // Immediate feedback before the (slow) worker creation begins.
    sendStreamGraphStatus(conn, request_id, graph.graphId);

    std::thread(
        executeStreamGraphStart,
        broker_manager_,
        conn,
        request_id,
        std::move(graph),
        active_run_id)
        .detach();
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

    StreamGraphRuntimeState runtime;
    {
        std::lock_guard<std::mutex> lock(g_stream_graph_mutex);
        const auto search = g_stream_graph_runtime.find(graph_id);
        if (search == g_stream_graph_runtime.end()) {
            sendError(conn, "No active graph runtime exists for graph_id");
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
        for (auto& entry : active_runtime.nodeStatuses) {
            if (entry.second.state == "running" || entry.second.state == "stalled" ||
                entry.second.state == "starting" || entry.second.state == "blocked" ||
                entry.second.state == "error") {
                entry.second.state =
                    entry.second.outputStreamId.has_value() ? "stopped" : entry.second.state;
            }
        }
    }

    sendStreamGraphStopped(conn, request_id, graph_id);
    broadcastTransformList();
}

void StreamViewerWebSocket::handleUnsubscribe(const WebSocketConnectionPtr& conn,
                                               StreamViewerClientContext* ctx,
                                               const std::vector<uint64_t>& stream_ids)
{
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);

        for (uint64_t stream_id : stream_ids) {
            ctx->subscribed_streams.erase(stream_id);
            
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
        }
    } // Release lock before calling sendStatus

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

            // Lazily bind messengers for any subscription whose stream wasn't
            // discoverable at subscribe time (e.g. a transform/combine output
            // from a graph that had only just been started). Without this, such
            // a subscription would never deliver data — the "Waiting for
            // EMG data…" that showed up whenever the viewer was opened before
            // the output stream had propagated. Throttled so we don't call
            // getAllStreams() on every 10 ms poll.
            if (broker_manager_ &&
                ctx->messengers.size() < ctx->subscribed_streams.size()) {
                std::set<uint64_t> resolved_ids;
                for (const auto& messenger : ctx->messengers) {
                    resolved_ids.insert(messenger->getId());
                }
                const uint64_t now_us = nowUs();
                if (now_us - last_pending_resolve_us > 500000ULL) {
                    last_pending_resolve_us = now_us;
                    auto rawStreams = broker_manager_->getAllStreams();
                    for (uint64_t stream_id : ctx->subscribed_streams) {
                        if (resolved_ids.count(stream_id) > 0) {
                            continue;
                        }
                        for (const auto& stream : rawStreams) {
                            if (stream->getId() != stream_id) {
                                continue;
                            }
                            auto dataTopics = stream->getTopicsByType(
                                nat::core::StreamType::DATA);
                            auto dataTopic = choosePreferredDataTopic(dataTopics);
                            if (dataTopic != nullptr) {
                                ctx->messengers.push_back(
                                    broker_manager_->createMessenger(dataTopic));
                                LOG_INFO << "StreamViewer: Lazily bound messenger "
                                            "for stream "
                                         << stream_id;
                            }
                            break;
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

    json["accuracies"]["accelerometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getAccelerationAccuracy());
    json["accuracies"]["gyroscope"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getGyroscopeAccuracy());
    json["accuracies"]["rotation"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getRotationAccuracy());

    json["has_data"]["accelerometer"] = data.wasDataSetForAcceleration();
    json["has_data"]["gyroscope"] = data.wasDataSetForGryoscope();
    json["has_data"]["rotation"] = data.wasDataSetForRotation();

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

            sample["accuracies"]["accelerometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getAccelerationAccuracy());
            sample["accuracies"]["gyroscope"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getGyroscopeAccuracy());
            sample["accuracies"]["rotation"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getRotationAccuracy());

            sample["has_data"]["accelerometer"] = record.wasDataSetForAcceleration();
            sample["has_data"]["gyroscope"] = record.wasDataSetForGryoscope();
            sample["has_data"]["rotation"] = record.wasDataSetForRotation();

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

void StreamViewerWebSocket::sendError(const WebSocketConnectionPtr& conn, const std::string& message)
{
    nlohmann::json json;
    json["type"] = "error";
    json["message"] = message;
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
