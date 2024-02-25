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

#include <librdkafka/rdkafkacpp.h>

#include <libnatkit-core.hpp>

namespace nat::kafka {

using namespace std::chrono_literals;

class BrokerMessagingQueue : public core::MessagingQueue {

  class ConsumerCallback : public RdKafka::ConsumeCb {
    BrokerMessagingQueue
        &messagingQueue; // NOTE: This is a reference because we need to access
                         // the outter parent;

    static core::message_t stringToMessageType(const std::string &string) {
      return core::message_t{string.begin(), string.end()};
    }

  public:
    ConsumerCallback(BrokerMessagingQueue &messagingQueue)
        : messagingQueue(messagingQueue) {}

    void consume_cb(RdKafka::Message &msg, void *opaque) {
      const RdKafka::Headers *headers;
      switch (msg.err()) {
      case RdKafka::ERR__TIMED_OUT:
        break;

      case RdKafka::ERR_NO_ERROR:
        /* Real message */
        std::cout << "Read msg at offset " << msg.offset() << std::endl;
        if (msg.key()) {
          std::cout << "Key: " << *msg.key() << std::endl;
        }
        headers = msg.headers();
        if (headers) {
          std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
          for (size_t i = 0; i < hdrs.size(); i++) {
            const RdKafka::Headers::Header hdr = hdrs[i];

            if (hdr.value() != NULL)
              printf(" Header: %s = \"%.*s\"\n", hdr.key().c_str(),
                     (int)hdr.value_size(), (const char *)hdr.value());
            else
              printf(" Header:  %s = NULL\n", hdr.key().c_str());
          }
        }
        messagingQueue.onMessageRecieved(
            std::make_unique<core::message_t>(stringToMessageType(
                std::string{static_cast<const char *>(msg.payload())})));
        printf("%.*s\n", static_cast<int>(msg.len()),
               static_cast<const char *>(msg.payload()));

        break;

      case RdKafka::ERR__PARTITION_EOF:
        /* Last message */
        messagingQueue.doesBrokerHaveMoreMessagesForReading = true;
        break;

      case RdKafka::ERR__UNKNOWN_TOPIC:
      case RdKafka::ERR__UNKNOWN_PARTITION:
        std::cerr << "Consume failed: " << msg.errstr() << std::endl;
        messagingQueue.doesBrokerHaveMoreMessagesForReading = false;
        break;

      default:
        /* Errors */
        std::cerr << "Consume failed: " << msg.errstr() << std::endl;
        messagingQueue.doesBrokerHaveMoreMessagesForReading = false;
      }
    }
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
          onMessageRecievedHandlerMaybe = {})
      : topicName(topicName), producer(producer), consumer(consumer),
        topicHandle(topicHandle) {
    if (onMessageRecievedHandlerMaybe.has_value()) {
      onMessageRecieved = onMessageRecievedHandlerMaybe.value();
    } else {
      onMessageRecieved = [this](auto &&msg) {
        this->defaultOnMessageRecieved(std::move(msg));
      };
    }
    consumerCallback = std::make_unique<ConsumerCallback>(*this);
    startConsumer();
    thread = std::jthread{&BrokerMessagingQueue::handleMessages, this};
  }

  virtual ~BrokerMessagingQueue() { running = false; }

  virtual void enqueueMessageToSend(std::unique_ptr<core::message_t> &&message) override {
    const std::lock_guard<std::mutex> lock(sendingQueueLock);
    sendingQueue.push(std::move(message));
  }

  virtual void enqueueMessageToReceive(const std::shared_ptr<core::message_t> message) override {
    const std::lock_guard<std::mutex> lock(receivingQueueLock);
    receivingQueue.push(std::move(message));
  }

  virtual std::optional<std::shared_ptr<core::message_t>> tryGetNextMessage() override {
    const std::lock_guard<std::mutex> lock(receivingQueueLock);
    if (receivingQueue.empty()) {
      return {};
    } else {
      auto message = std::move(receivingQueue.front());
      receivingQueue.pop();
      return std::move(message);
    }
  }

private:
  static std::string byteArrayToString(const std::vector<uint8_t> &byteArray) {
    return std::string{byteArray.begin(), byteArray.end()};
  }

  void handleMessages() {
    while (running) {
      pollResources();
      readMessages();
      sendMessages();

      std::this_thread::sleep_for(10ms);
    }
  }

  void pollResources() {
    producer->poll(0);
    consumer->poll(0);
  }

  void sendMessages() {
    while (!sendingQueue.empty()) {
      auto message = std::move(sendingQueue.front());
      sendingQueue.pop();
      sendMessage(std::move(message));
    }
  }

  void sendMessage(std::unique_ptr<core::message_t> message) {
    const auto stringMessage = byteArrayToString(*message);
    std::cout << "Attempting to send the following message to broker: "
              << stringMessage << '\n';
    const auto err = producer->produce(
        topicName, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
        const_cast<char *>(stringMessage.c_str()), stringMessage.size(), NULL,
        0, 0, NULL, NULL);
    if (err != RdKafka::ERR_NO_ERROR) {
      std::cout << "Error: " << RdKafka::err2str(err) << '\n';
    } else {
      std::cerr << "% Enqueued message (" << stringMessage.size() << " bytes) "
                << "for topic " << stringMessage << std::endl;
    }
    pollResources();
    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000 /* wait for max 10 seconds */);
    if (producer->outq_len() > 0)
      std::cerr << "% " << producer->outq_len()
                << " message(s) were not delivered" << std::endl;
  }

  void readMessages() {
    doesBrokerHaveMoreMessagesForReading = true;
    do {
      if (consumer->consume_callback(topicHandle.get(), partition, 500,
                                     consumerCallback.get(), nullptr) < 1) {
        doesBrokerHaveMoreMessagesForReading = false;
      }
    } while (doesBrokerHaveMoreMessagesForReading);
    pollResources();
  }

  void startConsumer(int startOffset = 0) {
    RdKafka::ErrorCode resp =
        consumer->start(topicHandle.get(), partition, startOffset);
    if (resp != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
                << std::endl;
      exit(1);
    }
  }

  void stopConsumer() { consumer->stop(topicHandle.get(), partition); }

  void defaultOnMessageRecieved(std::unique_ptr<core::message_t> &&msg) {
    {
      const std::lock_guard<std::mutex> lock(receivingQueueLock);
      receivingQueue.push(std::move(msg));
    }
  }
};

} // namespace nat::kafka
