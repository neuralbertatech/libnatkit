#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

#include <libnatkit-core.hpp>

#include <librdkafka/rdkafkacpp.h>

namespace nat::kafka {

// Time-introspection result for a stream/topic (Phase 3). earliest/latest are
// the retained offset bounds (query_watermark_offsets); offsetForTimestamp is the
// first offset at or after a queried timestamp (offsets_for_times), or -1 when no
// timestamp was queried / none is at/after it.
struct StreamTimeExtent {
  bool valid = false;
  int64_t earliestOffset = -1;
  int64_t latestOffset = -1;
  int64_t offsetForTimestamp = -1;
};

class BrokerManager {
public:
  virtual bool isConnected() const = 0;

  // Query a topic's retained offset bounds and, when timestampUs >= 0, the
  // offset at/after that timestamp. Default returns an invalid extent so mocks
  // and alternate implementations need not override it.
  virtual StreamTimeExtent queryStreamTime(const std::string& /*topicName*/,
                                           int64_t /*timestampUs*/ = -1) const {
    return StreamTimeExtent{};
  }

  virtual std::shared_ptr<RdKafka::Producer> createProducer() const = 0;

  virtual std::shared_ptr<RdKafka::Consumer> createConsumer() const = 0;

  virtual void deleteTopic(const std::string&) = 0;

  virtual std::vector<std::string>
  getAllTopicStrings(bool includeHiddenTopics = false) const = 0;

  virtual std::vector<std::unique_ptr<core::BasicTopicInformation>>
  getAllTopics() const = 0;

  virtual std::vector<std::unique_ptr<core::RawStream>> getAllStreams() const = 0;

  // startOffset selects where the messenger's consumer begins reading:
  // RdKafka::Topic::OFFSET_END (-1, live tail, the default) or
  // OFFSET_BEGINNING (-2, historical replay), or a concrete offset >= 0.
  virtual std::unique_ptr<core::TopicMessenger> createMessenger(
      const std::shared_ptr<core::BasicTopicInformation> &topicInfo,
      int64_t startOffset = -1) const = 0;

  virtual std::shared_ptr<core::Registry> getRegistry() const = 0;

  virtual std::unique_ptr<RdKafka::Topic>
  createTopicHandle(const std::string &topicName,
                    RdKafka::Consumer &consumer) const = 0;
};

std::unique_ptr<BrokerManager> createBrokerManager(const std::string &address,
                                                   const std::string &port);


class BrokerMessagingQueue : public core::MessagingQueue {
  class ConsumerCallback : public RdKafka::ConsumeCb {
    BrokerMessagingQueue
        &messagingQueue; // NOTE: This is a reference because we need to access
                         // the outter parent;
                         //
    static core::message_t stringToMessageType(const std::string &string);

  public:
    ConsumerCallback(BrokerMessagingQueue &messagingQueue);
    void consume_cb(RdKafka::Message &msg, void *opaque);
  };

  std::queue<std::shared_ptr<core::message_t>> receivingQueue{};
  std::queue<std::unique_ptr<core::message_t>> sendingQueue{};
  mutable std::mutex receivingQueueLock{};
  mutable std::mutex sendingQueueLock{};
  std::string topicName;
  std::shared_ptr<RdKafka::Producer> producer;
  std::shared_ptr<RdKafka::Consumer> consumer;
  std::shared_ptr<RdKafka::Topic> topicHandle;
  std::unique_ptr<ConsumerCallback> consumerCallback;
  std::function<void(std::unique_ptr<core::message_t> &&)> onMessageRecieved;
  std::jthread thread;
  int partition{0};
  bool doesBrokerHaveMoreMessagesForReading{false};
  bool running{true};

public:
  BrokerMessagingQueue(
      const std::string &topicName,
      const std::shared_ptr<RdKafka::Producer> &producer,
      const std::shared_ptr<RdKafka::Consumer> &consumer,
      const std::shared_ptr<RdKafka::Topic> &topicHandle,
      std::optional<std::function<void(std::unique_ptr<core::message_t> &&)>>
          onMessageRecievedHandlerMaybe = {},
      int64_t startOffset = -1);

  virtual ~BrokerMessagingQueue();
  virtual void enqueueMessageToSend(std::unique_ptr<core::message_t> &&message) override;
  virtual void enqueueMessageToReceive(const std::shared_ptr<core::message_t> message) override;
  virtual nat::core::Optional<std::shared_ptr<core::message_t>> tryGetNextMessage() override;
  virtual void clearAllMessages() override;
  virtual void flush() override;

private:
  static std::string byteArrayToString(const std::vector<uint8_t> &byteArray);
  void handleMessages();
  void pollResources();
  void sendMessages();
  void sendMessage(std::unique_ptr<core::message_t> message);
  void readMessages();
  void startConsumer(int64_t startOffset = -1);  // -1 = OFFSET_END (start from latest)
  void stopConsumer();
  void defaultOnMessageRecieved(std::unique_ptr<core::message_t> &&msg);
};

} // namespace nat::kafka
