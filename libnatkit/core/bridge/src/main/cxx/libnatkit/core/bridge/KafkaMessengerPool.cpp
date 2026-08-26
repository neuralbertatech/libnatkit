#include <libnatkit-core.hpp>
#include <libnatkit-bridge.hpp>

#include <libnatkit/util/Casting.hpp>

#include <chrono>


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
        const int64_t startOffset =
            initialSweepDone ? startOffsetForNewTopic(topicString) : -1;
        createNewMessenger(topicInfoMaybe.value(), startOffset);
      }
    }
  }
  initialSweepDone = true;
  dropVanishedTopics(existingTopics);
}

// Discovery is a POLL, so there is a window between a topic being created and
// this pool attaching a consumer to it -- and a consumer attaching at the live
// tail never sees what was produced inside that window. It is silent: nothing
// fails, the message simply is not forwarded to MQTT.
//
// Measured on the running rig rather than reasoned about: producing to a topic
// immediately after creating it delivered NOTHING to natKit/receiving/, while the
// same produce five seconds after creation arrived normally. That is the whole
// mechanism behind commands to a device timing out the first time and working on
// the retry -- the backend creates Command-<id>-... and produces to it, and
// whether the command survives depends on which side of this poll it lands.
//
// The cure is to attach slightly BEFORE now for a topic that has only just
// appeared. It has to be a bounded look-back rather than OFFSET_BEGINNING: this
// same code path runs for topics that already existed when the bridge started,
// where the beginning is hours of retained frames that would be replayed into
// MQTT in one burst. Hence `initialSweepDone` -- only topics that appear while we
// are watching get the look-back, and for those the window is the only thing
// there is to find.
constexpr int64_t kDiscoveryLookbackUs = 15000000;  // comfortably > the 1s poll

int64_t KafkaMessengerPool::startOffsetForNewTopic(
    const std::string &topicName) const {
  const auto nowUs = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  const auto extent =
      kafkaManager->queryStreamTime(topicName, nowUs - kDiscoveryLookbackUs);
  if (!extent.valid || extent.offsetForTimestamp < 0) {
    // No answer is not the same as "start at the beginning". Fall back to the
    // live tail, which is the behaviour this replaces: a lost message in the
    // discovery window beats replaying a topic of unknown size.
    return -1;
  }
  return extent.offsetForTimestamp;
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
    const std::unique_ptr<core::BasicTopicInformation> &basicTopicInfo,
    int64_t startOffset) {
  createNewMessenger(basicTopicInfo->toTopicString(), startOffset);
}

void KafkaMessengerPool::createNewMessenger(const std::string &topicName,
                                            int64_t startOffset) {
  monitoredTopics.insert(topicName);

  const auto producer = kafkaManager->createProducer();
  const auto consumer = kafkaManager->createConsumer();
  const auto topicHandle = nat::util::asShared(
      kafkaManager->createTopicHandle(topicName, *consumer));
  auto messagingQueue = std::make_unique<kafka::BrokerMessagingQueue>(
      topicName, producer, consumer, std::move(topicHandle),
      [this, topicName](auto &&msg) {
        this->onMessageRecievedCallback(topicName, std::move(msg));
      },
      startOffset);
  messengers.insert(std::pair{topicName, std::move(messagingQueue)});
  std::cout << "% Registered new topic: " << topicName;
  if (startOffset >= 0) {
    std::cout << " (catching up from offset " << startOffset << ")";
  }
  std::cout << '\n';
}

void KafkaMessengerPool::defaultOnMessageReceived(const std::string &topicString,
                              std::unique_ptr<core::message_t> &&msg) {
  const std::string messageString{msg->begin(), msg->end()};
  std::cout << "KafkaMessengerPool Received the following message from "
            << topicString << ": " << messageString << '\n';
}

} // namespace nat::bridge
