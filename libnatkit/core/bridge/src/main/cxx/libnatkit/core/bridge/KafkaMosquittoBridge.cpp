#include <libnatkit-bridge.hpp>

#include <libnatkit/util/Casting.hpp>
#include <libnatkit/util/Strings.hpp>


using namespace std::chrono_literals;

namespace nat::bridge {

KafkaMosquittoBridge::KafkaMosquittoBridge(
    std::shared_ptr<kafka::BrokerManager> kafkaBroker,
    std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker)
    : kafkaBroker(kafkaBroker), mosquittoBroker(mosquittoBroker) {}

// Sets up the MQTT client to write messages recieved to the recieving queue.
bool KafkaMosquittoBridge::initMosquittoClient() {
  std::cout << "  [Bridge] Requesting Mosquitto client from broker...\n";
  auto clientMaybe = mosquittoBroker->createClient(
      "natKit/sending/#",
      [this](auto *client, const auto &topic, const auto &message, auto qos) {
        this->onMosquittoMessageReceived(client, topic, message, qos);
      });
  if (clientMaybe.has_value()) {
    std::cout << "  [Bridge] ✓ Mosquitto client created\n";
    mosquittoClient = std::move(clientMaybe.value());
    return true;
  } else {
    std::cout << "  [Bridge] ✗ Failed to create Mosquitto client!\n";
    std::cout << "  [Bridge] This usually indicates the MQTT broker connection failed\n";
    return false;
  }
}

// Start all threads from sending and recieving data.
void KafkaMosquittoBridge::start() {
  std::cout << "  [Bridge] Creating Kafka messenger pool...\n";
  kafkaMessengerPool = std::make_unique<KafkaMessengerPool>(
      kafkaBroker, [this](const auto &topicName, auto &&msg) {
        onKafkaMessageReceived(topicName, std::move(msg));
      });
  std::cout << "  [Bridge] ✓ Kafka messenger pool created\n";
  
  // Ensure pool is fully initialized before starting threads
  std::cout << "  [Bridge] Waiting for pool initialization...\n";
  std::this_thread::sleep_for(100ms);
  
  std::cout << "  [Bridge] Starting worker threads:\n";
  std::cout << "  [Bridge]   - Message mover thread...\n";
  messageMover =
      std::jthread(&KafkaMosquittoBridge::messageMoverDaemonThreadSafe, this);
  std::cout << "  [Bridge]   - Kafka message sender thread...\n";
  kafkaMessageSender = std::jthread(
      &KafkaMosquittoBridge::kafkaMessageSendingDaemonThreadSafe, this);
  std::cout << "  [Bridge]   - Mosquitto message sender thread...\n";
  mosquittoMessageSender = std::jthread(
      &KafkaMosquittoBridge::mosquittoMessageSendingDaemonThreadSafe, this);
  std::cout << "  [Bridge] ✓ All worker threads started\n";
}

KafkaMosquittoBridge::~KafkaMosquittoBridge() { running = false; }

std::optional<std::unique_ptr<KafkaMosquittoBridge>> KafkaMosquittoBridge::create(
    std::shared_ptr<kafka::BrokerManager> kafkaBroker,
    std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker) {
  std::cout << "  [Bridge] Creating bridge instance...\n";
  auto bridge = std::unique_ptr<KafkaMosquittoBridge>(new KafkaMosquittoBridge(kafkaBroker, mosquittoBroker));
  
  const auto mosquittoTopicString = "natKit/#";
  std::cout << "  [Bridge] Initializing Mosquitto client...\n";
  std::cout << "  [Bridge] Subscribing to topic: " << mosquittoTopicString << "\n";
  
  bool wasMosquittoClientCreated = bridge->initMosquittoClient();
  if (wasMosquittoClientCreated) {
    std::cout << "  [Bridge] ✓ Mosquitto client initialized successfully\n";
    std::cout << "  [Bridge] Starting bridge threads...\n";
    bridge->start();
    std::cout << "  [Bridge] ✓ Bridge threads started\n";
    return std::move(bridge);
  } else {
    std::cout << "  [Bridge] ✗ Failed to initialize Mosquitto client\n";
    std::cout << "  [Bridge] Bridge creation aborted\n";
    return {};
  }
}

std::optional<std::shared_ptr<core::message_t>> KafkaMosquittoBridge::getNextKafkaMessage() {
  //return kafkaMessenger->tryGetNextMessage(); 
  return {};
}

void KafkaMosquittoBridge::tryCreateMosquittoPublisher(const std::string &topicName) {
  const auto topicString = "natKit/reciving/" + topicName;
  auto publisherMaybe = mosquittoBroker->createPublisher(topicString);
  if (publisherMaybe.has_value()) {
    mosquittoPublishers.insert(
        std::pair{topicName, std::move(publisherMaybe.value())});
  }
}

// Kafka message received default callback
void KafkaMosquittoBridge::onKafkaMessageReceived(const std::string &topicName,
                            std::unique_ptr<core::message_t> &&msg) {
  {
    const std::lock_guard<std::mutex> lock(kafkaBrokerRecievingQueueLock);
    kafkaBrokerReceivingQueue.emplace(topicName, std::move(msg));
  }
}

// MQTT message received default callback
void KafkaMosquittoBridge::onMosquittoMessageReceived(mosquitto::MosquittoClient *,
                                const std::string &topic,
                                const std::string &message, int qos) {
    std::cout << "Recieved an MQTT message!\n";
  auto encodedMessage =
      std::make_unique<core::message_t>(message.begin(), message.end());
  const auto splitTopic = util::Strings::split(topic, '/');
  if (splitTopic.size() == 0) {
    std::cout << "Error: Mosquitto recieved unrecognized topic " << topic
              << '\n';
    return;
  }
  const auto topicName = splitTopic[splitTopic.size() - 1];
  {
    const std::lock_guard<std::mutex> lock(
        mosquittoBrokerRecievingQueueLock);
    kafkaBrokerSendingQueue.emplace(topicName,
                                        std::move(encodedMessage));
  }
}

// Moves messages from the Kafka recieving queue to the sending queue.
void KafkaMosquittoBridge::moveKafkaMessagesThreadSafe() {
  {
    std::lock_guard<std::mutex> kafkaQueueLock(
        kafkaBrokerRecievingQueueLock);
    std::lock_guard<std::mutex> mosquittoQueueLock(
        mosquittoBrokerSendingQueueLock);
    while (!kafkaBrokerReceivingQueue.empty()) {
      mosquittoBrokerSendingQueue.push(
          std::move(kafkaBrokerReceivingQueue.front()));
      kafkaBrokerReceivingQueue.pop();
    }
  }
}

// Moves messages from the MQTT recieving queue to the sending queue.
void KafkaMosquittoBridge::moveMosquittoMessagesThreadSafe() {
  {
    std::lock_guard<std::mutex> mosquittoQueueLock(
        mosquittoBrokerRecievingQueueLock);
    if (mosquittoBrokerReceivingQueue.empty()) {
        return;
    }

    std::lock_guard<std::mutex> kafkaQueueLock(kafkaBrokerSendingQueueLock);
    while (!mosquittoBrokerReceivingQueue.empty()) {
      kafkaBrokerSendingQueue.push(
          std::move(mosquittoBrokerReceivingQueue.front()));
      mosquittoBrokerReceivingQueue.pop();
    }
  }
}

// Sends all available messages to the MQTT broker.
void KafkaMosquittoBridge::sendMosquittoMessagesThreadSafe() {
  {
    std::lock_guard<std::mutex> lock(mosquittoBrokerSendingQueueLock);
    while (!mosquittoBrokerSendingQueue.empty()) {
      auto [topicName, msg] =
          std::move(mosquittoBrokerSendingQueue.front());
      mosquittoBrokerSendingQueue.pop();
      const std::string messageString{msg->begin(), msg->end()};
      if (auto it = mosquittoPublishers.find(topicName);
          it != mosquittoPublishers.end()) {
        it->second->sendMessage(messageString);
      } else {
        tryCreateMosquittoPublisher(topicName);
        if (auto it = mosquittoPublishers.find(topicName);
            it != mosquittoPublishers.end()) {
          it->second->sendMessage(messageString);
        } else {
          std::cout << "Error: Newly created Mosquitto publisher could not "
                       "be found!\n";
        }
      }
    }
  }
}

// Sends all available messages to the Kafka broker.
void KafkaMosquittoBridge::sendKafkaMessagesThreadSafe() {
  {
    std::lock_guard<std::mutex> lock(kafkaBrokerSendingQueueLock);
    while (!kafkaBrokerSendingQueue.empty()) {
      auto [topicName, msg] = std::move(kafkaBrokerSendingQueue.front());
      kafkaBrokerSendingQueue.pop();
      if (kafkaMessengerPool) {
        kafkaMessengerPool->sendMessage(topicName, std::move(msg));
      } else {
        std::cerr << "Error: Kafka messenger pool is null!\n";
      }
    }
  }
}

// Continuously move messages from the reciving MQTT and Kafka recieiving queues
// to their corrisponding sending queues so that they may be sent out.
//
// Note: The rational for moving messages for both MQTT and Kafka here is because this
//       is a simple in memory move and does not reach out at all to the networking
//       stack. As such it should be substancially faster than any of the other daemons
void KafkaMosquittoBridge::messageMoverDaemonThreadSafe() {
  while (running) {
    moveKafkaMessagesThreadSafe();
    moveMosquittoMessagesThreadSafe();
    std::this_thread::sleep_for(1ms);
  }
}

// Continuously send any messages recieved from the Kafka broker to the MQTT broker.
void KafkaMosquittoBridge::mosquittoMessageSendingDaemonThreadSafe() {
  while (running) {
    sendMosquittoMessagesThreadSafe();
    std::this_thread::sleep_for(1ms);
  }
}

// Continuously send any messages recieved from the MQTT broker to the Kafka broker.
void KafkaMosquittoBridge::kafkaMessageSendingDaemonThreadSafe() {
  while (running) {
    sendKafkaMessagesThreadSafe();
    std::this_thread::sleep_for(1ms);
  }
}

} // namespace nat::bridge
