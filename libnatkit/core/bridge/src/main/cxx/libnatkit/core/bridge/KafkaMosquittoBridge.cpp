#include <libnatkit-bridge.hpp>

#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <set>

#include <libnatkit/util/Casting.hpp>
#include <libnatkit/util/Strings.hpp>
#include <nlohmann/json.hpp>


using namespace std::chrono_literals;

namespace {

bool is_supported_natvr_schema(const std::string& schema_version) {
  return schema_version == nat::core::ExgPillEmgDataSchemaV1::schemaVersion ||
         schema_version == "device.firmware.status.v1" ||
         schema_version == "device.status.v1" ||
         schema_version == "hand.state.v1";
}

uint64_t stable_stream_id(const std::string& namespace_name,
                          const std::string& identifier) {
  constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  constexpr uint64_t kInt64Max = 0x7FFFFFFFFFFFFFFFULL;

  const auto key = namespace_name + ":" + identifier;
  uint64_t digest = kFnvOffsetBasis;
  for (const unsigned char byte : key) {
    digest ^= byte;
    digest *= kFnvPrime;
  }

  const auto stream_id = digest & kInt64Max;
  return stream_id == 0 ? 1 : stream_id;
}

std::string canonical_topic_name(nat::core::StreamType stream_type,
                                 const std::string& namespace_name,
                                 const std::string& identifier,
                                 const std::string& schema_name) {
  return nat::core::toString(stream_type) + "-" +
         std::to_string(stable_stream_id(namespace_name, identifier)) + "-" +
         nat::core::toString(nat::core::SerializationType::Json) + "-" +
         schema_name;
}

std::optional<std::string> get_json_string(const nlohmann::json& payload,
                                           const char* key) {
  const auto it = payload.find(key);
  if (it == payload.end() || !it->is_string()) {
    return std::nullopt;
  }

  return it->get<std::string>();
}

bool is_canonical_kafka_topic_name(const std::string& topic_name) {
  const auto split_topic = nat::core::Strings::split(topic_name, '-');
  if (split_topic.size() != 4) {
    return false;
  }

  const auto stream_type = nat::core::streamTypeFromString(split_topic[0]);
  const auto serialization_type =
      nat::core::serializationTypeFromString(split_topic[2]);
  if (!stream_type.has_value() || !serialization_type.has_value()) {
    return false;
  }

  try {
    std::stoll(split_topic[1]);
  } catch (const std::exception&) {
    return false;
  }

  return !split_topic[3].empty();
}

bool is_printable_payload(const nat::core::message_t& message) {
  return std::all_of(message.begin(), message.end(), [](const uint8_t byte) {
    return std::isprint(byte) || std::isspace(byte);
  });
}

std::optional<std::string> try_format_legacy_schema(
    const std::string& topic_name,
    const nat::core::message_t& message,
    const nat::core::Registry* registry) {
  if (registry == nullptr) {
    return std::nullopt;
  }

  const auto split_topic = nat::core::Strings::split(topic_name, '-');
  if (split_topic.size() != 4) {
    return std::nullopt;
  }

  const auto topic_info_maybe = nat::core::BasicTopicInformation::create(topic_name);
  if (!topic_info_maybe.has_value()) {
    return std::nullopt;
  }

  auto decoded_maybe = registry->tryDecode(message, *topic_info_maybe.value());
  if (!decoded_maybe.has_value() || decoded_maybe.value() == nullptr) {
    return std::nullopt;
  }

  return decoded_maybe.value()->toString();
}

std::optional<std::string> try_format_natvr_schema(
    const nat::core::message_t& message) {
  try {
    const auto parsed = nlohmann::json::parse(message);
    if (!parsed.is_object()) {
      return std::nullopt;
    }

    const auto schema_it = parsed.find("schema_version");
    if (schema_it == parsed.end() || !schema_it->is_string()) {
      return std::nullopt;
    }

    const auto schema_version = schema_it->get<std::string>();
    if (!is_supported_natvr_schema(schema_version)) {
      return std::nullopt;
    }

    return schema_version + ": " + parsed.dump();
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

std::optional<std::string> try_translate_natvr_topic(
    const nat::core::message_t& message) {
  try {
    const auto parsed = nlohmann::json::parse(message);
    if (!parsed.is_object()) {
      return std::nullopt;
    }

    const auto schema_version = get_json_string(parsed, "schema_version");
    if (!schema_version.has_value()) {
      return std::nullopt;
    }

    if (schema_version.value() == nat::core::ExgPillEmgDataSchemaV1::schemaVersion) {
      const auto device_id = get_json_string(parsed, "device_id");
      if (!device_id.has_value()) {
        return std::nullopt;
      }
      return canonical_topic_name(nat::core::StreamType::DATA, "device_id",
                                  device_id.value(),
                                  nat::core::ExgPillEmgDataSchemaV1::name);
    }

    if (schema_version.value() == "hand.state.v1") {
      const auto session_id = get_json_string(parsed, "session_id");
      if (!session_id.has_value()) {
        return std::nullopt;
      }
      return canonical_topic_name(nat::core::StreamType::DATA, "session_id",
                                  session_id.value(), "HandStateV1");
    }

    if (schema_version.value() == "device.status.v1") {
      const auto device_id = get_json_string(parsed, "device_id");
      if (!device_id.has_value()) {
        return std::nullopt;
      }
      return canonical_topic_name(nat::core::StreamType::HARDWARE_STATUS,
                                  "device_id", device_id.value(),
                                  "DeviceStatusV1");
    }

    if (schema_version.value() == "device.firmware.status.v1") {
      const auto status_device_id = get_json_string(parsed, "status_device_id");
      const auto device_id = get_json_string(parsed, "device_id");
      const auto identifier = status_device_id.has_value() ? status_device_id.value()
                                                           : device_id.value_or("");
      if (identifier.empty()) {
        return std::nullopt;
      }
      return canonical_topic_name(nat::core::StreamType::LOGGING_HEARTBEAT,
                                  "status_device_id", identifier,
                                  "DeviceFirmwareStatusV1");
    }

    return std::nullopt;
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

std::string map_mqtt_topic_to_kafka_topic(const std::string& mqtt_topic,
                                          const nat::core::message_t& message) {
  const auto split_topic = nat::util::Strings::split(mqtt_topic, '/');
  if (split_topic.empty()) {
    return "";
  }

  const auto& last_segment = split_topic.back();
  if (is_canonical_kafka_topic_name(last_segment)) {
    return last_segment;
  }

  if (const auto translated = try_translate_natvr_topic(message);
      translated.has_value()) {
    return translated.value();
  }

  return last_segment;
}

std::string format_message_for_logging(const std::string& topic_name,
                                       const nat::core::message_t& message,
                                       const nat::core::Registry* registry) {
  if (const auto legacy = try_format_legacy_schema(topic_name, message, registry);
      legacy.has_value()) {
    return legacy.value();
  }

  if (const auto natvr = try_format_natvr_schema(message); natvr.has_value()) {
    return natvr.value();
  }

  if (is_printable_payload(message)) {
    return nat::core::toString(message);
  }

  return "<binary payload, " + std::to_string(message.size()) + " bytes>";
}

// Per-message tracing is OFF by default. It ran on every frame in both
// directions and printed the FULLY DECODED payload, which for a bulk IMU frame
// is ~100 lines -- it drowned every other service's output in `compose logs`
// and burned real CPU decoding records purely to throw the string away.
// Opt back in with NATKIT_BRIDGE_LOG_MESSAGES=1 when debugging the wire.
bool bridge_message_logging_enabled() {
  static const bool enabled = [] {
    const char* raw = std::getenv("NATKIT_BRIDGE_LOG_MESSAGES");
    if (raw == nullptr) {
      return false;
    }
    const std::string value(raw);
    return value == "1" || value == "true" || value == "TRUE" || value == "yes";
  }();
  return enabled;
}

// Complain about an unroutable MQTT topic the first time we see it, then stay
// quiet about that topic.
void warn_unrecognized_topic_once(const std::string& topic) {
  static std::mutex seen_lock;
  static std::set<std::string> seen;
  {
    const std::lock_guard<std::mutex> lock(seen_lock);
    if (!seen.insert(topic).second) {
      return;
    }
  }
  std::cout << "[Bridge] Ignoring unrecognized MQTT topic: " << topic
            << " (further messages on this topic are silent)\n";
}

void log_bridge_message(const char* direction, const std::string& topic_name,
                        const nat::core::message_t& message,
                        const nat::core::Registry* registry) {
  // Check the flag BEFORE formatting: format_message_for_logging decodes the
  // whole record, so doing it unconditionally would keep the cost even with
  // logging off.
  if (!bridge_message_logging_enabled()) {
    return;
  }
  std::cout << "[Bridge][" << direction << "] topic=" << topic_name
            << " payload=" << format_message_for_logging(topic_name, message, registry)
            << "\n";
}

}  // namespace

namespace nat::bridge {

KafkaMosquittoBridge::KafkaMosquittoBridge(
    std::shared_ptr<kafka::BrokerManager> kafkaBroker,
    std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker)
    : kafkaBroker(kafkaBroker),
      mosquittoBroker(mosquittoBroker),
      registry(core::Registry::createDefaultInitalizeRegistry()) {}

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
  log_bridge_message("Kafka->MQTT", topicName, *msg, registry.get());
  {
    const std::lock_guard<std::mutex> lock(kafkaBrokerRecievingQueueLock);
    kafkaBrokerReceivingQueue.emplace(topicName, std::move(msg));
  }
}

// MQTT message received default callback
void KafkaMosquittoBridge::onMosquittoMessageReceived(mosquitto::MosquittoClient *,
                                const std::string &topic,
                                const std::string &message, int qos) {
  auto encodedMessage =
      std::make_unique<core::message_t>(message.begin(), message.end());
  const auto topicName = map_mqtt_topic_to_kafka_topic(topic, *encodedMessage);
  if (topicName.empty()) {
    // Once per distinct topic: an unrecognized topic is usually a misconfigured
    // publisher streaming at full rate, so logging per message would flood just
    // as badly as the payload tracing did.
    warn_unrecognized_topic_once(topic);
    return;
  }
  log_bridge_message("MQTT->Kafka", topicName, *encodedMessage, registry.get());
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
