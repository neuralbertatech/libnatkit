#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include <libnatkit-core.hpp>

namespace nat::tools {

// Deterministic topic construction. A transform/combine node's output stream id
// is a pure function of its (namespace, identifier) via stableStreamId, and the
// topic string is fully determined by (type, id, schema). That determinism is
// what lets the graph runtime address an upstream node's output without waiting
// for the broker to observe (materialize) the topic. StreamViewerWebSocket's
// createTopicInfo() delegates here so production and tests share one definition.
inline std::shared_ptr<nat::core::BasicTopicInformation> makeTopicInfo(
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

// Resolves the source DATA topic for a graph node's input.
//
// Kafka discovery is the primary path: a hardware source stream is already
// flowing, so its topic exists in broker metadata. But an intra-graph edge's
// upstream is a transform/combine node created microseconds earlier in the same
// topological startup pass; it has not produced its first frame yet, so its
// output topic is not in broker metadata. Discovery returns null, and we fall
// back to the deterministic output topic of the already-created upstream worker.
//
// Before this fallback existed, deep composite pipelines failed at the first
// stage whose upstream had not yet produced ("Could not locate a compatible JSON
// numeric channel topic for source_stream_id"), cascading into blocked
// downstream nodes. The two lookups are injected so this decision stays pure and
// testable without a live broker or running worker threads.
inline std::shared_ptr<nat::core::BasicTopicInformation> resolveGraphSourceTopic(
    uint64_t source_stream_id,
    const std::function<
        std::shared_ptr<nat::core::BasicTopicInformation>(uint64_t)>&
        discoverFromBroker,
    const std::function<
        std::shared_ptr<nat::core::BasicTopicInformation>(uint64_t)>&
        resolveGraphInternalOutput)
{
    if (auto discovered = discoverFromBroker(source_stream_id)) {
        return discovered;
    }
    return resolveGraphInternalOutput(source_stream_id);
}

} // namespace nat::tools
