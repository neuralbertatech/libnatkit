#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <libnatkit/tools/GraphTopicResolution.hpp>
#include <libnatkit/tools/InProcessTransport.hpp>

namespace {

using nat::tools::InProcessChannel;
using nat::tools::InProcessChannelPolicy;
using nat::tools::InProcessChannelRegistry;
using nat::tools::InProcessMessagingQueue;
using nat::tools::InProcessOverflowPolicy;
using nat::tools::InProcessRole;

using Frame = std::shared_ptr<nat::core::message_t>;

Frame frame(const std::string& text)
{
    return std::make_shared<nat::core::message_t>(text.begin(), text.end());
}

std::string text(const Frame& f)
{
    return std::string(f->begin(), f->end());
}

// A producer and its single consumer rendezvous on one channel id and every
// published frame is delivered in order.
TEST(InProcessTransport, SingleProducerSingleConsumerDeliversInOrder)
{
    InProcessChannelRegistry registry;
    InProcessMessagingQueue producer(42, InProcessRole::Producer, registry);
    InProcessMessagingQueue consumer(42, InProcessRole::Consumer, registry);

    EXPECT_FALSE(consumer.tryGetNextMessage().has_value());

    producer.enqueueMessageToSend(
        std::make_unique<nat::core::message_t>(*frame("a")));
    producer.enqueueMessageToSend(
        std::make_unique<nat::core::message_t>(*frame("b")));

    const auto first = consumer.tryGetNextMessage();
    const auto second = consumer.tryGetNextMessage();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(text(first.value()), "a");
    EXPECT_EQ(text(second.value()), "b");
    EXPECT_FALSE(consumer.tryGetNextMessage().has_value());
}

// One output fans out to several downstream nodes; each sees every frame.
TEST(InProcessTransport, FanOutDeliversToEveryConsumer)
{
    InProcessChannelRegistry registry;
    InProcessMessagingQueue producer(7, InProcessRole::Producer, registry);
    InProcessMessagingQueue consumerA(7, InProcessRole::Consumer, registry);
    InProcessMessagingQueue consumerB(7, InProcessRole::Consumer, registry);

    producer.enqueueMessageToSend(
        std::make_unique<nat::core::message_t>(*frame("x")));

    const auto a = consumerA.tryGetNextMessage();
    const auto b = consumerB.tryGetNextMessage();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(text(a.value()), "x");
    EXPECT_EQ(text(b.value()), "x");
    // Same reference-counted handle, no copy of the payload.
    EXPECT_EQ(a.value().get(), b.value().get());
}

// A frame published before a consumer joins is not retroactively delivered
// (channels have no broker-side backlog); frames after it are.
TEST(InProcessTransport, LateConsumerOnlySeesSubsequentFrames)
{
    InProcessChannelRegistry registry;
    InProcessMessagingQueue producer(9, InProcessRole::Producer, registry);
    producer.enqueueMessageToSend(
        std::make_unique<nat::core::message_t>(*frame("early")));

    InProcessMessagingQueue consumer(9, InProcessRole::Consumer, registry);
    EXPECT_FALSE(consumer.tryGetNextMessage().has_value());

    producer.enqueueMessageToSend(
        std::make_unique<nat::core::message_t>(*frame("late")));
    const auto got = consumer.tryGetNextMessage();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(text(got.value()), "late");
}

// DropOldest evicts the stalest frames and counts the drops for observability.
TEST(InProcessTransport, DropOldestBoundsDepthAndCountsDrops)
{
    InProcessChannelRegistry registry;
    InProcessChannelPolicy policy;
    policy.capacity = 2;
    policy.overflow = InProcessOverflowPolicy::DropOldest;

    InProcessMessagingQueue producer(11, InProcessRole::Producer, registry, policy);
    InProcessMessagingQueue consumer(11, InProcessRole::Consumer, registry, policy);

    for (const std::string& value : {"1", "2", "3", "4"}) {
        producer.enqueueMessageToSend(
            std::make_unique<nat::core::message_t>(*frame(value)));
    }

    // Only the two freshest survive.
    const auto first = consumer.tryGetNextMessage();
    const auto second = consumer.tryGetNextMessage();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(text(first.value()), "3");
    EXPECT_EQ(text(second.value()), "4");
    EXPECT_FALSE(consumer.tryGetNextMessage().has_value());

    const auto channel = registry.find(11);
    ASSERT_NE(channel, nullptr);
    const auto metrics = channel->metrics();
    EXPECT_EQ(metrics.published, 4u);
    EXPECT_EQ(metrics.dropped, 2u);
}

// Block policy throttles the producer until the consumer drains, and close()
// unblocks a waiting producer so teardown never wedges.
TEST(InProcessTransport, BlockPolicyThrottlesThenCloseUnblocks)
{
    InProcessChannelRegistry registry;
    InProcessChannelPolicy policy;
    policy.capacity = 1;
    policy.overflow = InProcessOverflowPolicy::Block;

    auto channel = registry.getOrCreate(21, policy);
    auto subscriber = channel->addSubscriber();

    channel->publish(frame("first"));  // fills the single slot

    std::atomic<bool> secondPublished{false};
    std::thread producerThread([&] {
        channel->publish(frame("second"));  // blocks until drained
        secondPublished.store(true);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(secondPublished.load());  // still blocked

    const auto drained = channel->tryPop(*subscriber);
    ASSERT_TRUE(drained.has_value());
    EXPECT_EQ(text(drained.value()), "first");

    producerThread.join();
    EXPECT_TRUE(secondPublished.load());
}

TEST(InProcessTransport, CloseUnblocksBlockedProducer)
{
    InProcessChannelRegistry registry;
    InProcessChannelPolicy policy;
    policy.capacity = 1;
    policy.overflow = InProcessOverflowPolicy::Block;

    auto channel = registry.getOrCreate(31, policy);
    channel->addSubscriber();
    channel->publish(frame("full"));

    std::thread producerThread([&] { channel->publish(frame("blocked")); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    channel->close();  // teardown wakes the blocked producer
    producerThread.join();
    SUCCEED();
}

// Producer teardown drops the channel from the registry so the next run starts
// clean; a consumer leaving removes only its slot.
// End-to-end through the real TopicMessenger + Registry: a producer encodes a
// concrete schema, the in-process channel carries the bytes, and the consumer
// decodes it back to the same record. This is the exact path a TransformWorker
// takes, so node code is transport-agnostic — an in-process edge behaves like a
// Kafka edge minus the broker round-trip.
TEST(InProcessTransport, RoundTripsSchemaThroughTopicMessenger)
{
    std::shared_ptr<nat::core::Registry> registry(
        nat::core::Registry::createDefaultInitalizeRegistry().release());
    const auto topic = nat::tools::makeTopicInfo(
        nat::core::StreamType::DATA, "transform", "inproc-roundtrip",
        nat::core::NatSignalFrameDataSchemaV1::name);
    ASSERT_NE(topic, nullptr);

    InProcessChannelRegistry channels;
    auto producer = nat::tools::createInProcessMessenger(
        topic, registry, InProcessRole::Producer, channels);
    auto consumer = nat::tools::createInProcessMessenger(
        topic, registry, InProcessRole::Consumer, channels);
    ASSERT_NE(producer, nullptr);
    ASSERT_NE(consumer, nullptr);

    const nat::core::NatSignalFrameDataSchemaV1 sent(
        "dev-1", 7, 123456, 500, {"ch0", "ch1"},
        {1.0f, 2.0f, 3.0f, 4.0f}, 2);
    producer->sendMessage(sent);

    const auto received = consumer->tryGetNexMessage();
    ASSERT_TRUE(received.has_value());
    const auto* decoded =
        dynamic_cast<nat::core::NatSignalFrameDataSchemaV1*>(
            received.value().get());
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->getDeviceId(), "dev-1");
    EXPECT_EQ(decoded->getSeqNo(), 7u);
    EXPECT_EQ(decoded->getSamplesPerChannel(), 2u);
    EXPECT_EQ(decoded->getSamples(),
              std::vector<float>({1.0f, 2.0f, 3.0f, 4.0f}));
}

TEST(InProcessTransport, TeardownRemovesChannelAndSubscribers)
{
    InProcessChannelRegistry registry;
    {
        InProcessMessagingQueue producer(51, InProcessRole::Producer, registry);
        InProcessMessagingQueue consumer(51, InProcessRole::Consumer, registry);
        const auto channel = registry.find(51);
        ASSERT_NE(channel, nullptr);
        EXPECT_EQ(channel->metrics().subscriberCount, 1u);
    }
    EXPECT_EQ(registry.find(51), nullptr);
}

}  // namespace
