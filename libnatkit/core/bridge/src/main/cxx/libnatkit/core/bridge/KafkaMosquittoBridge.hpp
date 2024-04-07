//#pragma once
//
//#include <chrono>
//#include <memory>
//#include <mutex>
//#include <queue>
//#include <thread>
//
//#include <libnatkit/core/bridge/KafkaMessengerPool.hpp>
////#include <libnatkit/core/kafka/broker/BrokerManager.hpp>
//#include <libnatkit/core/mqtt/MosquittoBroker.hpp>
//#include <libnatkit-core.hpp> // TODO: Remove
//#include <libnatkit-kafka.hpp>
//#include <libnatkit/util/Strings.hpp>
//
//namespace nat::bridge {
//
//using namespace std::chrono_literals;
//
//class KafkaMosquittoBridge {
//  std::shared_ptr<kafka::BrokerManager> kafkaBroker;
//  std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker;
//  std::unique_ptr<mosquitto::MosquittoPublisher> mosquittoPublisher;
//  std::unique_ptr<mosquitto::MosquittoClient> mosquittoClient;
//  std::unique_ptr<KafkaMessengerPool> kafkaMessengerPool;
//  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
//      kafkaBrokerReceivingQueue;
//  std::mutex kafkaBrokerRecievingQueueLock;
//  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
//      mosquittoBrokerSendingQueue;
//  std::mutex mosquittoBrokerSendingQueueLock;
//  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
//      kafkaBrokerSendingQueue;
//  std::mutex kafkaBrokerSendingQueueLock;
//  std::queue<std::pair<std::string, std::unique_ptr<core::message_t>>>
//      mosquittoBrokerReceivingQueue;
//  std::mutex mosquittoBrokerRecievingQueueLock;
//  std::map<std::string, std::unique_ptr<mosquitto::MosquittoPublisher>>
//      mosquittoPublishers{};
//
//  std::jthread messageMover{};
//  std::jthread kafkaMessageSender{};
//  std::jthread mosquittoMessageSender{};
//  bool running{true};
//
//  KafkaMosquittoBridge(
//      std::shared_ptr<kafka::BrokerManager> kafkaBroker,
//      std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker)
//      : kafkaBroker(kafkaBroker), mosquittoBroker(mosquittoBroker) {}
//
//  bool initMosquittoClient() {
//    auto clientMaybe = mosquittoBroker->createClient(
//        "natKit/sending/#",
//        [this](auto *client, const auto &topic, const auto &message, auto qos) {
//          this->onMosquittoMessageReceived(client, topic, message, qos);
//        });
//    if (clientMaybe.has_value()) {
//      mosquittoClient = std::move(clientMaybe.value());
//      return true;
//    } else {
//      std::cout << "Failed to create mosquitto client!\n";
//      return false;
//    }
//  }
//
//    void start() {
//      kafkaMessengerPool = std::make_unique<KafkaMessengerPool>(
//          kafkaBroker, [this](const auto &topicName, auto &&msg) {
//            onKafkaMessageReceived(topicName, std::move(msg));
//          });
//      messageMover =
//          std::jthread(&KafkaMosquittoBridge::messageMoverDaemon, this);
//      kafkaMessageSender = std::jthread(
//          &KafkaMosquittoBridge::kafkaMessageSendingDaemon, this);
//      mosquittoMessageSender = std::jthread(
//          &KafkaMosquittoBridge::mosquittoMessageSendingDaemon, this);
//    }
//
//  public:
//    ~KafkaMosquittoBridge() { running = false; }
//    static std::optional<std::unique_ptr<KafkaMosquittoBridge>> create(
//        std::shared_ptr<kafka::BrokerManager> kafkaBroker,
//        std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker);
//
//    std::optional<std::shared_ptr<core::message_t>> getNextKafkaMessage();
//
//  private:
//    void tryCreateMosquittoPublisher(const std::string &topicName) {
//      const auto topicString = "natKit/reciving/" + topicName;
//      auto publisherMaybe = mosquittoBroker->createPublisher(topicString);
//      if (publisherMaybe.has_value()) {
//        mosquittoPublishers.insert(
//            std::pair{topicName, std::move(publisherMaybe.value())});
//      }
//    }
//
//    void onKafkaMessageReceived(const std::string &topicName,
//                                std::unique_ptr<core::message_t> &&msg) {
//      {
//        const std::lock_guard<std::mutex> lock(kafkaBrokerRecievingQueueLock);
//        kafkaBrokerReceivingQueue.emplace(topicName, std::move(msg));
//      }
//    }
//
//    void onMosquittoMessageReceived(mosquitto::MosquittoClient *,
//                                    const std::string &topic,
//                                    const std::string &message, int qos) {
//      auto encodedMessage =
//          std::make_unique<core::message_t>(message.begin(), message.end());
//      const auto splitTopic = util::Strings::split(topic, '/');
//      if (splitTopic.size() == 0) {
//        std::cout << "Error: Mosquitto recieved unrecognized topic " << topic
//                  << '\n';
//        return;
//      }
//      const auto topicName = splitTopic[splitTopic.size() - 1];
//      {
//        const std::lock_guard<std::mutex> lock(
//            mosquittoBrokerRecievingQueueLock);
//        kafkaBrokerSendingQueue.emplace(topicName,
//                                            std::move(encodedMessage));
//      }
//    }
//
//    void moveKafkaMessages() {
//      {
//        std::lock_guard<std::mutex> kafkaQueueLock(
//            kafkaBrokerRecievingQueueLock);
//        std::lock_guard<std::mutex> mosquittoQueueLock(
//            mosquittoBrokerSendingQueueLock);
//        while (!kafkaBrokerReceivingQueue.empty()) {
//          mosquittoBrokerSendingQueue.push(
//              std::move(kafkaBrokerReceivingQueue.front()));
//          kafkaBrokerReceivingQueue.pop();
//        }
//      }
//    }
//
//    void moveMosquittoMessages() {
//      {
//        std::lock_guard<std::mutex> kafkaQueueLock(kafkaBrokerSendingQueueLock);
//        std::lock_guard<std::mutex> mosquittoQueueLock(
//            mosquittoBrokerRecievingQueueLock);
//        while (!mosquittoBrokerReceivingQueue.empty()) {
//          kafkaBrokerSendingQueue.push(
//              std::move(mosquittoBrokerReceivingQueue.front()));
//          mosquittoBrokerReceivingQueue.pop();
//        }
//      }
//    }
//
//    void sendMosquittoMessages() {
//      {
//        std::lock_guard<std::mutex> lock(mosquittoBrokerSendingQueueLock);
//        while (!mosquittoBrokerSendingQueue.empty()) {
//          auto [topicName, msg] =
//              std::move(mosquittoBrokerSendingQueue.front());
//          mosquittoBrokerSendingQueue.pop();
//          const std::string messageString{msg->begin(), msg->end()};
//          if (auto it = mosquittoPublishers.find(topicName);
//              it != mosquittoPublishers.end()) {
//            it->second->sendMessage(messageString);
//          } else {
//            tryCreateMosquittoPublisher(topicName);
//            if (auto it = mosquittoPublishers.find(topicName);
//                it != mosquittoPublishers.end()) {
//              it->second->sendMessage(messageString);
//            } else {
//              std::cout << "Error: Newly created Mosquitto publisher could not "
//                           "be found!\n";
//            }
//          }
//        }
//      }
//    }
//
//    void sendKafkaMessages() {
//      {
//        std::lock_guard<std::mutex> lock(kafkaBrokerSendingQueueLock);
//        while (!kafkaBrokerSendingQueue.empty()) {
//          auto [topicName, msg] = std::move(kafkaBrokerSendingQueue.front());
//          kafkaBrokerSendingQueue.pop();
//          kafkaMessengerPool->sendMessage(topicName, std::move(msg));
//        }
//      }
//    }
//
//    void messageMoverDaemon() {
//      while (running) {
//        if (!kafkaBrokerReceivingQueue.empty()) {
//          moveKafkaMessages();
//        }
//        if (!mosquittoBrokerReceivingQueue.empty()) {
//          moveMosquittoMessages();
//        }
//
//        std::this_thread::sleep_for(1ms);
//      }
//    }
//
//    void mosquittoMessageSendingDaemon() {
//      while (running) {
//        if (!mosquittoBrokerSendingQueue.empty()) {
//          sendMosquittoMessages();
//        }
//        std::this_thread::sleep_for(1ms);
//      }
//    }
//
//    void kafkaMessageSendingDaemon() {
//      while (running) {
//        if (!kafkaBrokerSendingQueue.empty()) {
//          sendKafkaMessages();
//        }
//        std::this_thread::sleep_for(1ms);
//      }
//    }
//  };
//
//} // namespace nat::bridge
