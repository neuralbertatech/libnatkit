#include <gtest/gtest.h>

#include <cstdint>
#include <memory>

#include <libnatkit/tools/GraphTopicResolution.hpp>

namespace {

using nat::tools::makeTopicInfo;
using nat::tools::resolveGraphSourceTopic;

using TopicPtr = std::shared_ptr<nat::core::BasicTopicInformation>;

TopicPtr transformOutput(const std::string& identifier)
{
    return makeTopicInfo(
        nat::core::StreamType::DATA,
        "transform",
        identifier,
        nat::core::NatSignalFrameDataSchemaV1::name);
}

TopicPtr never(uint64_t) { return nullptr; }

// A transform/combine node's output stream id is a pure function of its
// identifier, so a downstream node can address its upstream's output topic
// without the broker having observed (materialized) that topic yet. This
// determinism is the precondition that makes the startup-race fix possible.
TEST(GraphTopicResolution, OutputTopicIdentityIsDeterministic)
{
    const auto a = transformOutput("notch");
    const auto b = transformOutput("notch");
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(a->id, b->id);
    EXPECT_EQ(a->id, nat::core::stableStreamId("transform", "notch"));
    EXPECT_NE(a->id, transformOutput("detrend")->id);
}

// Regression for the composite EMG pipeline failure: the detrend stage's
// upstream (notch) worker had been created but had not produced its first frame,
// so Kafka discovery could not see its topic. Resolution must fall back to the
// deterministic graph-internal output topic instead of returning null (which
// surfaced as "Could not locate a compatible JSON numeric channel topic ...").
TEST(GraphTopicResolution, FallsBackToGraphInternalWhenBrokerHasNotMaterializedTopic)
{
    const uint64_t upstream_id = nat::core::stableStreamId("transform", "notch");
    const auto internal = transformOutput("notch");

    const auto resolved = resolveGraphSourceTopic(
        upstream_id,
        /*discoverFromBroker=*/never,
        /*resolveGraphInternalOutput=*/[&](uint64_t id) {
            return id == upstream_id ? internal : TopicPtr{nullptr};
        });

    ASSERT_NE(resolved, nullptr);
    EXPECT_EQ(resolved->id, upstream_id);
}

// A hardware source stream is already flowing, so broker discovery succeeds and
// the in-process fallback must not be consulted.
TEST(GraphTopicResolution, PrefersBrokerDiscoveryWhenAvailable)
{
    const auto discovered = transformOutput("bandpass");
    bool fallback_called = false;

    const auto resolved = resolveGraphSourceTopic(
        discovered->id,
        [&](uint64_t) { return discovered; },
        [&](uint64_t) {
            fallback_called = true;
            return TopicPtr{nullptr};
        });

    ASSERT_NE(resolved, nullptr);
    EXPECT_EQ(resolved->id, discovered->id);
    EXPECT_FALSE(fallback_called);
}

// A genuinely unconnected/unknown source resolves through neither path.
TEST(GraphTopicResolution, ReturnsNullWhenNeitherPathResolves)
{
    EXPECT_EQ(resolveGraphSourceTopic(123U, never, never), nullptr);
}

} // namespace
