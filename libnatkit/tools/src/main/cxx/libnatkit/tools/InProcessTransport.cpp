#include "InProcessTransport.hpp"

#include <algorithm>

namespace nat::tools {

std::shared_ptr<InProcessChannel::Subscriber> InProcessChannel::addSubscriber()
{
    auto subscriber = std::make_shared<Subscriber>();
    const std::lock_guard<std::mutex> lock(subscribersMutex_);
    subscribers_.push_back(subscriber);
    return subscriber;
}

void InProcessChannel::removeSubscriber(
    const std::shared_ptr<Subscriber>& subscriber)
{
    const std::lock_guard<std::mutex> lock(subscribersMutex_);
    subscribers_.erase(
        std::remove(subscribers_.begin(), subscribers_.end(), subscriber),
        subscribers_.end());
}

void InProcessChannel::publish(
    const std::shared_ptr<nat::core::message_t>& message)
{
    std::vector<std::shared_ptr<Subscriber>> subscribers;
    {
        const std::lock_guard<std::mutex> lock(subscribersMutex_);
        subscribers = subscribers_;
    }
    published_.fetch_add(1);

    for (const auto& subscriber : subscribers) {
        std::unique_lock<std::mutex> lock(subscriber->mutex);
        if (policy_.overflow == InProcessOverflowPolicy::Block) {
            subscriber->notFull.wait(lock, [&] {
                return closed_.load() ||
                       subscriber->queue.size() < policy_.capacity;
            });
            if (closed_.load()) {
                return;
            }
            subscriber->queue.push_back(message);
        } else {
            // DropOldest: keep the freshest frames for a real-time consumer.
            while (subscriber->queue.size() >= policy_.capacity) {
                subscriber->queue.pop_front();
                subscriber->dropped += 1;
            }
            subscriber->queue.push_back(message);
        }
    }
}

nat::core::Optional<std::shared_ptr<nat::core::message_t>>
InProcessChannel::tryPop(Subscriber& subscriber)
{
    std::shared_ptr<nat::core::message_t> message;
    {
        const std::lock_guard<std::mutex> lock(subscriber.mutex);
        if (subscriber.queue.empty()) {
            return {};
        }
        message = std::move(subscriber.queue.front());
        subscriber.queue.pop_front();
    }
    // A blocked producer may now have room.
    subscriber.notFull.notify_one();
    return {std::move(message)};
}

void InProcessChannel::close()
{
    closed_.store(true);
    const std::lock_guard<std::mutex> lock(subscribersMutex_);
    for (const auto& subscriber : subscribers_) {
        subscriber->notFull.notify_all();
    }
}

void InProcessChannel::clearSubscriber(Subscriber& subscriber)
{
    {
        const std::lock_guard<std::mutex> lock(subscriber.mutex);
        subscriber.queue.clear();
    }
    subscriber.notFull.notify_all();
}

InProcessChannelMetrics InProcessChannel::metrics() const
{
    InProcessChannelMetrics metrics;
    metrics.channelId = channelId_;
    metrics.published = published_.load();

    const std::lock_guard<std::mutex> lock(subscribersMutex_);
    metrics.subscriberCount = subscribers_.size();
    for (const auto& subscriber : subscribers_) {
        const std::lock_guard<std::mutex> subscriberLock(subscriber->mutex);
        metrics.dropped += subscriber->dropped;
        metrics.maxDepth = std::max(metrics.maxDepth, subscriber->queue.size());
    }
    return metrics;
}

std::shared_ptr<InProcessChannel> InProcessChannelRegistry::getOrCreate(
    uint64_t channelId,
    InProcessChannelPolicy policy)
{
    const std::lock_guard<std::mutex> lock(mutex_);
    const auto search = channels_.find(channelId);
    if (search != channels_.end()) {
        return search->second;
    }
    auto channel = std::make_shared<InProcessChannel>(channelId, policy);
    channels_.emplace(channelId, channel);
    return channel;
}

std::shared_ptr<InProcessChannel> InProcessChannelRegistry::find(
    uint64_t channelId) const
{
    const std::lock_guard<std::mutex> lock(mutex_);
    const auto search = channels_.find(channelId);
    if (search == channels_.end()) {
        return nullptr;
    }
    return search->second;
}

void InProcessChannelRegistry::remove(uint64_t channelId)
{
    std::shared_ptr<InProcessChannel> channel;
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        const auto search = channels_.find(channelId);
        if (search == channels_.end()) {
            return;
        }
        channel = search->second;
        channels_.erase(search);
    }
    if (channel) {
        channel->close();
    }
}

std::vector<InProcessChannelMetrics> InProcessChannelRegistry::snapshot() const
{
    std::vector<std::shared_ptr<InProcessChannel>> channels;
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        channels.reserve(channels_.size());
        for (const auto& entry : channels_) {
            channels.push_back(entry.second);
        }
    }
    std::vector<InProcessChannelMetrics> metrics;
    metrics.reserve(channels.size());
    for (const auto& channel : channels) {
        metrics.push_back(channel->metrics());
    }
    return metrics;
}

InProcessChannelRegistry& InProcessChannelRegistry::global()
{
    static InProcessChannelRegistry registry;
    return registry;
}

InProcessMessagingQueue::InProcessMessagingQueue(
    uint64_t channelId,
    InProcessRole role,
    InProcessChannelRegistry& registry,
    InProcessChannelPolicy policy)
    : channelId_(channelId), role_(role), registry_(registry)
{
    channel_ = registry_.getOrCreate(channelId, policy);
    if (role_ == InProcessRole::Consumer) {
        subscriber_ = channel_->addSubscriber();
    }
}

InProcessMessagingQueue::~InProcessMessagingQueue()
{
    if (role_ == InProcessRole::Consumer) {
        if (channel_ && subscriber_) {
            channel_->removeSubscriber(subscriber_);
        }
        return;
    }
    // Producer teardown: close the channel and drop it from the registry so a
    // later run creates a fresh one. Consumers holding a shared_ptr keep the
    // channel alive until they drain and are destroyed.
    registry_.remove(channelId_);
}

void InProcessMessagingQueue::enqueueMessageToSend(
    std::unique_ptr<nat::core::message_t>&& message)
{
    if (!channel_ || !message) {
        return;
    }
    channel_->publish(
        std::shared_ptr<nat::core::message_t>(std::move(message)));
}

void InProcessMessagingQueue::enqueueMessageToReceive(
    const std::shared_ptr<nat::core::message_t> message)
{
    // Directly inject into this consumer's own slot (used by tests / direct
    // wiring). No-op for a producer, which has no slot of its own.
    if (role_ != InProcessRole::Consumer || !channel_ || !subscriber_ ||
        !message) {
        return;
    }
    const std::lock_guard<std::mutex> lock(subscriber_->mutex);
    subscriber_->queue.push_back(message);
}

nat::core::Optional<std::shared_ptr<nat::core::message_t>>
InProcessMessagingQueue::tryGetNextMessage()
{
    if (role_ != InProcessRole::Consumer || !channel_ || !subscriber_) {
        return {};
    }
    return channel_->tryPop(*subscriber_);
}

void InProcessMessagingQueue::clearAllMessages()
{
    if (role_ == InProcessRole::Consumer && channel_ && subscriber_) {
        channel_->clearSubscriber(*subscriber_);
    }
}

std::unique_ptr<nat::core::TopicMessenger> createInProcessMessenger(
    const std::shared_ptr<nat::core::BasicTopicInformation>& topicInfo,
    const std::shared_ptr<nat::core::Registry>& registry,
    InProcessRole role,
    InProcessChannelRegistry& registryOfChannels,
    InProcessChannelPolicy policy)
{
    if (topicInfo == nullptr) {
        return nullptr;
    }
    auto translator =
        std::make_shared<nat::core::TopicTranslator>(topicInfo, registry);
    auto queue = std::make_unique<InProcessMessagingQueue>(
        topicInfo->id, role, registryOfChannels, policy);
    return std::make_unique<nat::core::TopicMessenger>(
        std::move(queue), translator);
}

}  // namespace nat::tools
