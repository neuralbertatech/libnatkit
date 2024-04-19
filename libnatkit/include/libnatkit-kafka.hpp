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

class BrokerManager {
public:
  virtual bool isConnected() const = 0;

  virtual std::shared_ptr<RdKafka::Producer> createProducer() const = 0;

  virtual std::shared_ptr<RdKafka::Consumer> createConsumer() const = 0;

  virtual void deleteTopic(const std::string&) = 0;

  virtual std::vector<std::string>
  getAllTopicStrings(bool includeHiddenTopics = false) const = 0;

  virtual std::vector<std::unique_ptr<core::BasicTopicInformation>>
  getAllTopics() const = 0;

  virtual std::vector<std::unique_ptr<core::RawStream>> getAllStreams() const = 0;

  virtual std::unique_ptr<core::TopicMessenger> createMessenger(
      const std::shared_ptr<core::BasicTopicInformation> &topicInfo) const = 0;

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
  std::mutex receivingQueueLock;
  std::mutex sendingQueueLock;
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
          onMessageRecievedHandlerMaybe = {});

  virtual ~BrokerMessagingQueue();
  virtual void enqueueMessageToSend(std::unique_ptr<core::message_t> &&message) override;
  virtual void enqueueMessageToReceive(const std::shared_ptr<core::message_t> message) override;
  virtual nat::core::Optional<std::shared_ptr<core::message_t>> tryGetNextMessage() override;

private:
  static std::string byteArrayToString(const std::vector<uint8_t> &byteArray);
  void handleMessages();
  void pollResources();
  void sendMessages();
  void sendMessage(std::unique_ptr<core::message_t> message);
  void readMessages();
  void startConsumer(int startOffset = 0);
  void stopConsumer();
  void defaultOnMessageRecieved(std::unique_ptr<core::message_t> &&msg);
};

} // namespace nat::kafka
