#pragma once

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

#include <libnatkit/kafkit/core/registry/Message.hpp>

namespace nat::kafkit {

class BrokerMessagingQueue {
  class ConsumerCallback : public RdKafka::ConsumeCb {
    BrokerMessagingQueue
        &messagingQueue; // NOTE: This is a reference because we need to access
                         // the outter parent;
    
    static message_t stringToMessageType(const std::string& string) {
      return message_t{string.begin(), string.end()};
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
        messagingQueue.receivingQueue.push(std::make_unique<message_t>(stringToMessageType(std::string{static_cast<const char*>(msg.payload())})));
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

  std::queue<std::shared_ptr<message_t>> receivingQueue{};
  std::queue<std::unique_ptr<message_t>> sendingQueue{};
  std::string topicName;
  std::shared_ptr<RdKafka::Producer> producer;
  std::shared_ptr<RdKafka::Consumer> consumer;
  std::shared_ptr<RdKafka::Topic> topicHandle;
  std::unique_ptr<ConsumerCallback> consumerCallback;
  int partition{0};
  bool doesBrokerHaveMoreMessagesForReading{false};

public:
  BrokerMessagingQueue(const std::string &topicName,
                       const std::shared_ptr<RdKafka::Producer> &producer,
                       const std::shared_ptr<RdKafka::Consumer> &consumer,
                       const std::shared_ptr<RdKafka::Topic> &topicHandle)
      : topicName(topicName), producer(producer), consumer(consumer),
        topicHandle(topicHandle) {
    consumerCallback = std::make_unique<ConsumerCallback>(*this);
    startConsumer();
  }

  void enqueueMessageToSend(std::unique_ptr<message_t> &&message) {
    sendingQueue.push(std::move(message));
    sendMessages(); // TODO: Deleteme and replace with threading
  }

  void enqueueMessageToReceive(const std::shared_ptr<message_t> message) {
    receivingQueue.push(std::move(message));
  }

  std::optional<std::shared_ptr<message_t>> tryGetNextMessage() {
    if (receivingQueue.empty()) {
      readMessages();
    }

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

  void sendMessage(std::unique_ptr<message_t> message) {
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
    RdKafka::ErrorCode resp = consumer->start(topicHandle.get(), partition, startOffset);
    if (resp != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
                << std::endl;
      exit(1);
    }
  }

  void stopConsumer() {
    consumer->stop(topicHandle.get(), partition);
  }
};

} // namespace nat::kafkit
