#pragma once

#include <mutex>

#include <chrono>
#include <map>
#include <set>
#include <thread>

#include <libnatkit-core.hpp>
#include <libnatkit-kafka.hpp>
#include <libnatkit-mqtt.hpp>

namespace nat::bridge {

class KafkaMessengerPool {
  std::shared_ptr<kafka::BrokerManager> kafkaManager;
  std::map<std::string, std::unique_ptr<kafka::BrokerMessagingQueue>>
      messengers;
  std::set<std::string> monitoredTopics{};
  // Guards `messengers` + `monitoredTopics`. They are touched from TWO threads: the
  // topic monitor (which creates and now also destroys messengers) and whichever
  // thread calls sendMessage (the MQTT loop). Unsynchronised concurrent insert +
  // lookup was already a latent race; adding erase would have made it a
  // use-after-free.
  mutable std::mutex messengersLock{};
  std::jthread newTopicMonitor{};
  std::function<void(const std::string &, std::unique_ptr<core::message_t> &&)>
      onMessageRecievedCallback;
  bool running{true};

public:
  KafkaMessengerPool(std::shared_ptr<kafka::BrokerManager> kafkaManager,
                     std::function<void(const std::string &,
                                        std::unique_ptr<core::message_t> &&)>
                         onMessageRecievedCallback);

  ~KafkaMessengerPool();

  void sendMessage(const std::string &topicName,
                   std::unique_ptr<core::message_t> &&msg);

private:
  void monitorTopics();
  void searchForNewKafakTopics();
  // Both assume messengersLock is already held by the caller.
  void createNewMessenger(
      const std::unique_ptr<core::BasicTopicInformation> &basicTopicInfo);
  void createNewMessenger(const std::string &topicName);
  // Tear down messengers whose topic no longer exists on the broker.
  void dropVanishedTopics(const std::set<std::string> &existingTopics);
  void defaultOnMessageReceived(const std::string &topicString,
                                std::unique_ptr<core::message_t> &&msg);
};

class KafkaMosquittoBridge {
  std::shared_ptr<kafka::BrokerManager> kafkaBroker;
  std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker;
  std::unique_ptr<core::Registry> registry;
  std::unique_ptr<mosquitto::MosquittoPublisher> mosquittoPublisher;
  std::unique_ptr<mosquitto::MosquittoClient> mosquittoClient;
  std::unique_ptr<KafkaMessengerPool> kafkaMessengerPool;
  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
      kafkaBrokerReceivingQueue;
  std::mutex kafkaBrokerRecievingQueueLock;
  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
      mosquittoBrokerSendingQueue;
  std::mutex mosquittoBrokerSendingQueueLock;
  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
      kafkaBrokerSendingQueue;
  std::mutex kafkaBrokerSendingQueueLock;
  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
      mosquittoBrokerReceivingQueue;
  std::mutex mosquittoBrokerRecievingQueueLock;
  std::map<std::string, std::unique_ptr<mosquitto::MosquittoPublisher>>
      mosquittoPublishers{};

  std::jthread messageMover{};
  std::jthread kafkaMessageSender{};
  std::jthread mosquittoMessageSender{};
  bool running{true};

  KafkaMosquittoBridge(
      std::shared_ptr<kafka::BrokerManager> kafkaBroker,
      std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker);

  bool initMosquittoClient();
  void start();

public:
  ~KafkaMosquittoBridge();
  static std::optional<std::unique_ptr<KafkaMosquittoBridge>> create(
      std::shared_ptr<kafka::BrokerManager> kafkaBroker,
      std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker);

  std::optional<std::shared_ptr<core::message_t>> getNextKafkaMessage();

private:
  void tryCreateMosquittoPublisher(const std::string &topicName);
  void onKafkaMessageReceived(const std::string &topicName,
                              std::unique_ptr<core::message_t> &&msg);
  void onMosquittoMessageReceived(mosquitto::MosquittoClient *,
                                  const std::string &topic,
                                  const std::string &message, int qos);
  void moveKafkaMessagesThreadSafe();
  void moveMosquittoMessagesThreadSafe();
  void sendMosquittoMessagesThreadSafe();
  void sendKafkaMessagesThreadSafe();
  void messageMoverDaemonThreadSafe();
  void mosquittoMessageSendingDaemonThreadSafe();
  void kafkaMessageSendingDaemonThreadSafe();
};


};
