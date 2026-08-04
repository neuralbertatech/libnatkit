#include <libnatkit-core.hpp>
#include <libnatkit-bridge.hpp>

#include <libnatkit/util/Casting.hpp>


using namespace std::chrono_literals;

namespace nat::bridge {

KafkaMessengerPool::KafkaMessengerPool(std::shared_ptr<kafka::BrokerManager> kafkaManager,
                   std::function<void(const std::string &,
                                      std::unique_ptr<core::message_t> &&)>
                       onMessageRecievedCallback)
    : kafkaManager(kafkaManager),
      onMessageRecievedCallback(onMessageRecievedCallback) {
  std::cout << "% KafkaMessengerPool ctor\n";
  newTopicMonitor = std::jthread(&KafkaMessengerPool::monitorTopics, this);
}

KafkaMessengerPool::~KafkaMessengerPool() { running = false; }

void KafkaMessengerPool::sendMessage(const std::string &topicName,
                 std::unique_ptr<core::message_t> &&msg) {
  //std::cout << "% Kafka attempting to send message to: " << topicName << '\n';
  const std::lock_guard<std::mutex> lock(messengersLock);
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

void KafkaMessengerPool::monitorTopics() {
  while (running) {
    //std::cout << "% Checking for new topics...\n";
    searchForNewKafakTopics();
    std::this_thread::sleep_for(1s);
  }
}

void KafkaMessengerPool::searchForNewKafakTopics() {
  const auto allTopicStrings = kafkaManager->getAllTopicStrings();
  const std::set<std::string> existingTopics(allTopicStrings.begin(),
                                             allTopicStrings.end());

  const std::lock_guard<std::mutex> lock(messengersLock);
  for (const auto &topicString : allTopicStrings) {
    if (!monitoredTopics.contains(topicString)) {
      const auto topicInfoMaybe =
          core::BasicTopicInformation::create(topicString);
      if (topicInfoMaybe.has_value()) {
        createNewMessenger(topicInfoMaybe.value());
      }
    }
  }
  dropVanishedTopics(existingTopics);
}

// A topic can DISAPPEAR, and this pool used to be insert-only: monitoredTopics never
// shrank, so a deleted topic left a consumer polling a partition that no longer
// exists, logging "Consume failed: topic does not exist" for the lifetime of the
// process. The ghosts accumulate -- one per deleted topic -- and the only cure was a
// bridge restart.
//
// That went from "rare" to "routine" when instance replay landed: every replay
// creates a scratch Marker/<replay-id> topic and deletes it when the replay ends, so
// each replay used to leak one permanently-failing consumer.
void KafkaMessengerPool::dropVanishedTopics(
    const std::set<std::string> &existingTopics) {
  for (auto iterator = monitoredTopics.begin();
       iterator != monitoredTopics.end();) {
    if (existingTopics.contains(*iterator)) {
      ++iterator;
      continue;
    }
    std::cout << "% Topic no longer exists; dropping its messenger: " << *iterator
              << '\n';
    // ~BrokerMessagingQueue clears its `running` flag and joins its consumer
    // thread, so this both stops the polling and releases the client handles.
    messengers.erase(*iterator);
    iterator = monitoredTopics.erase(iterator);
  }
}

void KafkaMessengerPool::createNewMessenger(
    const std::unique_ptr<core::BasicTopicInformation> &basicTopicInfo) {
  createNewMessenger(basicTopicInfo->toTopicString());
}

void KafkaMessengerPool::createNewMessenger(const std::string &topicName) {
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

void KafkaMessengerPool::defaultOnMessageReceived(const std::string &topicString,
                              std::unique_ptr<core::message_t> &&msg) {
  const std::string messageString{msg->begin(), msg->end()};
  std::cout << "KafkaMessengerPool Received the following message from "
            << topicString << ": " << messageString << '\n';
}

} // namespace nat::bridge
