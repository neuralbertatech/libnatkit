#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <libnatkit-core.hpp>

// In-process transport for colocated, private graph edges (see ADR 005 and
// plans/in-process-transport-plan.html). This is a second nat::core::MessagingQueue
// implementation living behind the exact same interface as the Kafka-backed
// BrokerMessagingQueue, so node code (TransformWorker / CombineWorker) never sees
// which transport carries a frame.
//
// A channel maps 1:1 to what would have been a Kafka topic: it is keyed by the
// deterministic stableStreamId (== BasicTopicInformation::id) so a producer and
// its consumer(s) rendezvous by channel id without a broker. Frames still cross
// the messenger boundary as message_t (the same record bytes that cross the Kafka
// boundary today); the in-process path just moves the shared handle instead of
// paying a broker round-trip, so an edge can be swapped back to Kafka without
// touching node code.
namespace nat::tools {

// Overflow policy for a bounded in-process channel. Real-time signal edges use
// DropOldest (matches live-monitoring semantics); Block is for the rare edge that
// requires lossless handoff and can tolerate throttling its producer.
enum class InProcessOverflowPolicy {
    DropOldest,
    Block,
};

// The messenger's role on a channel. A graph worker uses one messenger per
// direction (its output is Producer, each input is Consumer), so a single
// InProcessMessagingQueue is only ever driven in one direction even though the
// MessagingQueue interface is symmetric.
enum class InProcessRole {
    Producer,
    Consumer,
};

struct InProcessChannelPolicy {
    // Per-subscriber frame capacity. Buffering is per frame, not per sample, so a
    // rate-changing stage (e.g. sliding window emitting one frame per N samples)
    // needs no special case beyond sizing.
    size_t capacity = 256;
    InProcessOverflowPolicy overflow = InProcessOverflowPolicy::DropOldest;
};

// A point-in-time view of a channel's health, surfaced into the same
// worker-summary/status surface Kafka transforms already report so an overloaded
// in-process edge is observable.
struct InProcessChannelMetrics {
    uint64_t channelId = 0;
    size_t subscriberCount = 0;
    uint64_t published = 0;   // frames handed to publish()
    uint64_t dropped = 0;     // frames evicted across all subscribers (DropOldest)
    size_t maxDepth = 0;      // deepest subscriber queue right now
};

// A single-producer / multi-consumer bounded channel. Each consumer gets its own
// FIFO slot so a slow consumer cannot starve its siblings; publish() fans one
// reference-counted frame handle out to every slot.
class InProcessChannel {
public:
    struct Subscriber {
        std::mutex mutex;
        std::condition_variable notFull;
        std::deque<std::shared_ptr<nat::core::message_t>> queue;
        uint64_t dropped = 0;
    };

    InProcessChannel(uint64_t channelId, InProcessChannelPolicy policy)
        : channelId_(channelId), policy_(policy) {}

    uint64_t channelId() const { return channelId_; }

    std::shared_ptr<Subscriber> addSubscriber();
    void removeSubscriber(const std::shared_ptr<Subscriber>& subscriber);

    // Fan one frame out to every subscriber. Under DropOldest a full slot evicts
    // its oldest frame; under Block the calling (producer) thread waits until the
    // slot drains or the channel is closed.
    void publish(const std::shared_ptr<nat::core::message_t>& message);

    // Pop the next frame for a specific subscriber, or nullopt if its slot is
    // empty. Non-blocking: graph workers poll.
    nat::core::Optional<std::shared_ptr<nat::core::message_t>>
    tryPop(Subscriber& subscriber);

    // Wake any producer blocked in publish() and refuse further blocking. Used on
    // teardown so a stopped producer never wedges on a consumer that is gone.
    void close();

    void clearSubscriber(Subscriber& subscriber);

    InProcessChannelMetrics metrics() const;

private:
    const uint64_t channelId_;
    const InProcessChannelPolicy policy_;
    std::atomic<bool> closed_{false};
    std::atomic<uint64_t> published_{0};
    mutable std::mutex subscribersMutex_;
    std::vector<std::shared_ptr<Subscriber>> subscribers_;
};

// Process-local rendezvous point. A producer and its consumer(s) find the same
// InProcessChannel by channel id. There is one global registry for the running
// backend process; tests instantiate their own.
class InProcessChannelRegistry {
public:
    std::shared_ptr<InProcessChannel> getOrCreate(
        uint64_t channelId,
        InProcessChannelPolicy policy = {});
    std::shared_ptr<InProcessChannel> find(uint64_t channelId) const;
    // Close and forget a channel. Any messenger still holding a shared_ptr keeps
    // it alive until it is destroyed; new lookups will miss and (for a fresh run)
    // create a new channel.
    void remove(uint64_t channelId);
    std::vector<InProcessChannelMetrics> snapshot() const;

    static InProcessChannelRegistry& global();

private:
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<InProcessChannel>> channels_;
};

// A MessagingQueue backed by an InProcessChannel. Producer role publishes; the
// Consumer role registers a subscriber slot on construction and pops from it.
class InProcessMessagingQueue : public nat::core::MessagingQueue {
public:
    InProcessMessagingQueue(
        uint64_t channelId,
        InProcessRole role,
        InProcessChannelRegistry& registry,
        InProcessChannelPolicy policy = {});

    ~InProcessMessagingQueue() override;

    void enqueueMessageToSend(
        std::unique_ptr<nat::core::message_t>&& message) override;
    void enqueueMessageToReceive(
        const std::shared_ptr<nat::core::message_t> message) override;
    nat::core::Optional<std::shared_ptr<nat::core::message_t>>
    tryGetNextMessage() override;
    void clearAllMessages() override;
    // In-process delivery is synchronous (publish immediately reaches the
    // subscriber slots), so there is nothing to drain.
    void flush() override {}

    uint64_t channelId() const { return channelId_; }
    InProcessRole role() const { return role_; }

private:
    const uint64_t channelId_;
    const InProcessRole role_;
    InProcessChannelRegistry& registry_;
    std::shared_ptr<InProcessChannel> channel_;
    std::shared_ptr<InProcessChannel::Subscriber> subscriber_;  // Consumer only
};

// Build a TopicMessenger whose transport is the in-process channel for topicInfo,
// using the same Registry-driven TopicTranslator a Kafka messenger would use. The
// returned messenger is a drop-in for BrokerManager::createMessenger(topicInfo).
std::unique_ptr<nat::core::TopicMessenger> createInProcessMessenger(
    const std::shared_ptr<nat::core::BasicTopicInformation>& topicInfo,
    const std::shared_ptr<nat::core::Registry>& registry,
    InProcessRole role,
    InProcessChannelRegistry& registryOfChannels,
    InProcessChannelPolicy policy = {});

}  // namespace nat::tools
