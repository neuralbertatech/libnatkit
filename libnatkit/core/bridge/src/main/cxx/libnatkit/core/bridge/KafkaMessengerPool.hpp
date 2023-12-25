#pragma once

#include <chrono>
#include <map>
#include <set>
#include <thread>

#include <libnatkit/core/kafka/broker/BrokerManager.hpp>
#include <libnatkit/core/streams/stream/BasicTopicInformation.hpp>
#include <libnatkit/util/Casting.hpp>

namespace nat::bridge {

using namespace std::chrono_literals;

class KafkaMessengerPool {
  std::shared_ptr<kafka::BrokerManager> kafkaManager;
  std::map<std::string, std::unique_ptr<kafka::BrokerMessagingQueue>>
      messengers;
  std::set<std::string> monitoredTopics{};
  std::jthread newTopicMonitor{};
  std::function<void(const std::string &, std::unique_ptr<kafka::message_t> &&)>
      onMessageRecievedCallback;
  bool running{true};

public:
  KafkaMessengerPool(std::shared_ptr<kafka::BrokerManager> kafkaManager,
                     std::function<void(const std::string &,
                                        std::unique_ptr<kafka::message_t> &&)>
                         onMessageRecievedCallback)
      : kafkaManager(kafkaManager),
        onMessageRecievedCallback(onMessageRecievedCallback) {
    std::cout << "% KafkaMessengerPool ctor\n";
    newTopicMonitor = std::jthread(&KafkaMessengerPool::monitorTopics, this);
  }

  ~KafkaMessengerPool() { running = false; }

  void sendMessage(const std::string &topicName,
                   std::unique_ptr<kafka::message_t> &&msg) {
    std::cout << "% Kafka attempting to send message to: " << topicName << '\n';
    if (auto it = messengers.find(topicName); it != messengers.end()) {
      it->second->enqueueMessageToSend(std::move(msg));
    } else {
      std::cout << "% Kafka topic " << topicName << " does not exist yet. Creating now\n";
      createNewMessenger(topicName);
      if (auto it = messengers.find(topicName); it != messengers.end()) {
        it->second->enqueueMessageToSend(std::move(msg));
      } else {
        std::cout
            << "Error: Newly created Kafka messenger could not be found!\n";
      }
    }
  }

private:
  void monitorTopics() {
    while (running) {
      std::cout << "% Checking for new topics...\n";
      searchForNewKafakTopics();
      std::this_thread::sleep_for(1s);
    }
  }

  void searchForNewKafakTopics() {
    const auto allTopicStrings = kafkaManager->getAllTopicStrings();
    for (const auto &topicString : allTopicStrings) {
      if (!monitoredTopics.contains(topicString)) {
        const auto topicInfoMaybe =
            kafka::BasicTopicInformation::create(topicString);
        if (topicInfoMaybe.has_value()) {
          createNewMessenger(topicInfoMaybe.value());
        }
      }
    }
  }

  void createNewMessenger(
      const std::unique_ptr<kafka::BasicTopicInformation> &basicTopicInfo) {
    createNewMessenger(basicTopicInfo->toTopicString());
  }

  void createNewMessenger(const std::string &topicName) {
    monitoredTopics.insert(topicName);

    const auto producer = kafkaManager->createProducer();
    const auto consumer = kafkaManager->createConsumer();
    const auto topicHandle = nat::util::asShared(
        kafkaManager->createTopicHandle(topicName, *consumer));
    auto messagingQueue = std::make_unique<kafka::BrokerMessagingQueue>(
        topicName, producer, consumer, std::move(topicHandle),
        [this, topicName](auto &&msg) {
          this->onMessageRecievedCallback(topicName, std::move(msg));
        });
    messengers.insert(std::pair{topicName, std::move(messagingQueue)});
    std::cout << "% Registered new topic: " << topicName << '\n';
  }

  void defaultOnMessageReceived(const std::string &topicString,
                                std::unique_ptr<kafka::message_t> &&msg) {
    const std::string messageString{msg->begin(), msg->end()};
    std::cout << "KafkaMessengerPool Received the following message from "
              << topicString << ": " << messageString << '\n';
  }
};

} // namespace nat::bridge
