#include <gtest/gtest.h>

#include <string>
#include <vector>

#include <libnatkit/tools/GraphTransportPlan.hpp>

namespace {

using nat::tools::classifyPrivateOutputs;
using nat::tools::TransportGraphEdge;
using nat::tools::TransportGraphNode;

// source -> t1 -> t2 -> viewer. Only t1's output is private: its sole consumer
// (t2) is a transform. t2 feeds a viewer, so t2's output must stay on Kafka.
TEST(GraphTransportPlan, PrivateChainStopsAtTap)
{
    const std::vector<TransportGraphNode> nodes{
        {"src", "stream_source"},
        {"t1", "transform"},
        {"t2", "transform"},
        {"v", "viewer"},
    };
    const std::vector<TransportGraphEdge> edges{
        {"src", "t1"},
        {"t1", "t2"},
        {"t2", "v"},
    };
    const auto priv = classifyPrivateOutputs(nodes, edges);
    EXPECT_TRUE(priv.count("t1"));
    EXPECT_FALSE(priv.count("t2"));
    EXPECT_FALSE(priv.count("src"));  // hardware source is never in-process
}

// An output tapped by BOTH a transform and a viewer is published: any published
// consumer forces Kafka.
TEST(GraphTransportPlan, MixedConsumersForceKafka)
{
    const std::vector<TransportGraphNode> nodes{
        {"t1", "transform"},
        {"t2", "transform"},
        {"v", "viewer"},
    };
    const std::vector<TransportGraphEdge> edges{
        {"t1", "t2"},
        {"t1", "v"},
    };
    const auto priv = classifyPrivateOutputs(nodes, edges);
    EXPECT_FALSE(priv.count("t1"));
}

// A session/train tap also forces Kafka so recordings only ever read published
// edges (Phase 4).
TEST(GraphTransportPlan, SessionAndTrainTapsForceKafka)
{
    const std::vector<TransportGraphNode> nodes{
        {"a", "transform"}, {"session", "session"},
        {"b", "transform"}, {"train", "train"},
    };
    const std::vector<TransportGraphEdge> edges{
        {"a", "session"},
        {"b", "train"},
    };
    const auto priv = classifyPrivateOutputs(nodes, edges);
    EXPECT_FALSE(priv.count("a"));
    EXPECT_FALSE(priv.count("b"));
}

// A terminal transform with no downstream consumer stays on Kafka: nothing to
// optimize and it may yet be tapped.
TEST(GraphTransportPlan, TerminalOutputStaysKafka)
{
    const std::vector<TransportGraphNode> nodes{{"t", "transform"}};
    const std::vector<TransportGraphEdge> edges{};
    EXPECT_FALSE(classifyPrivateOutputs(nodes, edges).count("t"));
}

// Combine with two private transform inputs feeding a downstream transform: the
// combine output is private, and both feeder outputs are private.
TEST(GraphTransportPlan, CombineFanInIsPrivate)
{
    const std::vector<TransportGraphNode> nodes{
        {"a", "transform"},
        {"b", "transform"},
        {"c", "combine"},
        {"d", "transform"},
    };
    const std::vector<TransportGraphEdge> edges{
        {"a", "c"},
        {"b", "c"},
        {"c", "d"},
    };
    const auto priv = classifyPrivateOutputs(nodes, edges);
    EXPECT_TRUE(priv.count("a"));
    EXPECT_TRUE(priv.count("b"));
    EXPECT_TRUE(priv.count("c"));
    EXPECT_FALSE(priv.count("d"));  // terminal
}

// The colocation proof gates the in-process path: a consumer proven NOT colocated
// forces its upstream output to Kafka (the Phase 5 cross-slot fallback).
TEST(GraphTransportPlan, NonColocatedConsumerForcesKafka)
{
    const std::vector<TransportGraphNode> nodes{
        {"t1", "transform"},
        {"t2", "transform"},
        {"t3", "transform"},
    };
    const std::vector<TransportGraphEdge> edges{
        {"t1", "t2"},
        {"t2", "t3"},
    };
    // t2 lives on a different slot than t1; everything else colocated.
    const auto priv = classifyPrivateOutputs(
        nodes, edges,
        [](const std::string& producer, const std::string& consumer) {
            return !(producer == "t1" && consumer == "t2");
        });
    EXPECT_FALSE(priv.count("t1"));  // cross-slot -> Kafka
    EXPECT_TRUE(priv.count("t2"));   // t2 -> t3 colocated -> in-process
}

}  // namespace
