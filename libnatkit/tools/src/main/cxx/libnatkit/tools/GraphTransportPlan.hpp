#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Edge classification and colocation proof for the in-process transport (ADR 005,
// plans/in-process-transport-plan.html). Given the FLATTENED graph the runtime
// actually executes, decide which producer outputs may skip Kafka and travel over
// an in-process channel instead.
//
// This is deliberately a pure function over a minimal, transport-relevant view of
// the graph (node id + kind, edges) so it can be unit-tested without the rest of
// the stream-graph runtime, and so the decision is asked as an explicit scheduler
// question rather than baked into worker creation. Composite membership is never
// an input: the classification runs post-flattening, so grouping/ungrouping a
// subgraph cannot change transport semantics.
namespace nat::tools {

struct TransportGraphNode {
    std::string id;
    std::string kind;  // stream_source | transform | combine | viewer | sink | session | train
};

struct TransportGraphEdge {
    std::string sourceNodeId;
    std::string targetNodeId;
};

// A node kind that produces an addressable output stream, and is therefore a
// candidate to be a colocated consumer on the in-process fast path.
inline bool isTransportProducerKind(const std::string& kind)
{
    return kind == "transform" || kind == "combine";
}

// A node kind that taps a stream for observation, recording, replay, or training
// and thus forces its upstream output onto the published (Kafka) path, where it
// stays discoverable / tap-able / recordable.
inline bool isPublishedConsumerKind(const std::string& kind)
{
    return kind == "viewer" || kind == "sink" || kind == "session" ||
           kind == "train";
}

// Colocation proof. Until ADR 001 Phase 4 moves transforms onto independently
// scheduled worker slots, every graph node shares the backend process, so this is
// trivially true. It is still asked as a question so that, once slot placement
// exists, cross-slot edges answer false here and fall back to Kafka automatically
// with no new transport — "Kafka is the cross-node data plane."
inline bool defaultColocationProof(
    const std::string& /*producerNodeId*/,
    const std::string& /*consumerNodeId*/)
{
    return true;
}

// Returns the set of producer node ids whose output stream is PRIVATE and may use
// the in-process transport. Everything not in the set stays on Kafka (the
// default; when in doubt, stay on Kafka).
//
// An output is private iff the producer is a transform/combine, has at least one
// consuming edge, every consumer is itself a transform/combine (no viewer/sink/
// session/train tap), and the scheduler proves every consumer is colocated with
// the producer. An output with no downstream consumer stays on Kafka: there is no
// private edge to optimize and it may yet be tapped.
inline std::unordered_set<std::string> classifyPrivateOutputs(
    const std::vector<TransportGraphNode>& nodes,
    const std::vector<TransportGraphEdge>& edges,
    const std::function<bool(
        const std::string& producerNodeId,
        const std::string& consumerNodeId)>& areColocated =
        defaultColocationProof)
{
    std::unordered_map<std::string, std::string> kindByNodeId;
    kindByNodeId.reserve(nodes.size());
    for (const auto& node : nodes) {
        kindByNodeId.emplace(node.id, node.kind);
    }

    std::unordered_set<std::string> privateOutputs;
    for (const auto& node : nodes) {
        if (!isTransportProducerKind(node.kind)) {
            continue;
        }
        bool hasConsumer = false;
        bool allConsumersPrivate = true;
        for (const auto& edge : edges) {
            if (edge.sourceNodeId != node.id) {
                continue;
            }
            hasConsumer = true;
            const auto consumerKind = kindByNodeId.find(edge.targetNodeId);
            const bool consumerIsProducer =
                consumerKind != kindByNodeId.end() &&
                isTransportProducerKind(consumerKind->second);
            if (!consumerIsProducer ||
                !areColocated(node.id, edge.targetNodeId)) {
                allConsumersPrivate = false;
                break;
            }
        }
        if (hasConsumer && allConsumersPrivate) {
            privateOutputs.insert(node.id);
        }
    }
    return privateOutputs;
}

}  // namespace nat::tools
