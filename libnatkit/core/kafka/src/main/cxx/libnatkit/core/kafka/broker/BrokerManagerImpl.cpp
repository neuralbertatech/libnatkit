#include <libnatkit-kafka.hpp>

#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <libnatkit/util/Casting.hpp>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

namespace nat::kafka {

static bool isTopicHidden(const std::string &topic_string) {
  if (topic_string.starts_with('_')) {
    return true;
  } else {
    return false;
  }
}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb(RdKafka::Message &message) {
    /* If message.err() is non-zero the message delivery failed permanently
     * for the message. */
      if (message.err()) {
          std::cerr << "% Message delivery failed: " << message.errstr()
              << std::endl;
      }
    else {
        /*std::cerr << "% Message delivered to topic " << message.topic_name()
                  << " [" << message.partition() << "] at offset "
                  << message.offset() << std::endl;*/
    }
  }
};

class BrokerManagerImpl : public BrokerManager {
  std::string address;
  std::string port;
  const std::unique_ptr<RdKafka::Conf> conf;
  const std::unique_ptr<RdKafka::Conf> topicConfig;
  bool connectedToBroker{false};
  std::shared_ptr<core::Registry> registry;
  mutable std::string errstr;
  std::shared_ptr<ExampleDeliveryReportCb> ex_dr_cb;

public:
  BrokerManagerImpl(const std::string &address, const std::string &port)
      : address(address), port(port),
        conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        topicConfig(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)),
        registry(core::Registry::createDefaultInitalizeRegistry()),
        ex_dr_cb(std::make_shared<ExampleDeliveryReportCb>()) {
    std::cout << "  [Kafka] Initializing Kafka Broker Manager...\n";
    std::cout << "  [Kafka] Target: " << address << ":" << port << "\n";
    
    const auto host = address + ":" + port;
    std::cout << "  [Kafka] Setting bootstrap.servers to: " << host << "\n";
    
    if (conf->set("bootstrap.servers", host, errstr) ==
        RdKafka::Conf::CONF_OK) {
      connectedToBroker = true;
      std::cout << "  [Kafka] ✓ Configuration accepted\n";
      std::cout << "  [Kafka] Note: Actual connection happens lazily on first use\n";
    } else {
      std::cout << "  [Kafka] ✗ Error setting broker configuration: " << errstr << '\n';
    }
    
    std::cout << "  [Kafka] Setting delivery report callback...\n";
    if (conf->set("dr_cb", ex_dr_cb.get(), errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << "  [Kafka] ✗ FATAL: Failed to set delivery callback: " << errstr << std::endl;
      exit(1);
    }
    std::cout << "  [Kafka] ✓ Delivery report callback set\n";

    std::cout << "  [Kafka] Setting low-latency consumer fetch policy...\n";
    if (conf->set("fetch.wait.max.ms", "10", errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << "  [Kafka] ✗ FATAL: Failed to set fetch.wait.max.ms: " << errstr
                << std::endl;
      exit(1);
    }
    if (conf->set("fetch.min.bytes", "1", errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << "  [Kafka] ✗ FATAL: Failed to set fetch.min.bytes: " << errstr
                << std::endl;
      exit(1);
    }
    std::cout << "  [Kafka] ✓ Consumer fetch policy set\n";
  }

  virtual bool isConnected() const override { return connectedToBroker; }

  virtual std::shared_ptr<RdKafka::Producer> createProducer() const override {
    std::shared_ptr<RdKafka::Producer> producer(
        RdKafka::Producer::create(conf.get(), errstr));
    if (!producer) {
      std::cout << "Error creating producer: " << errstr << '\n';
    }
    return std::move(producer);
  }

  virtual std::shared_ptr<RdKafka::Consumer> createConsumer() const override {
    std::shared_ptr<RdKafka::Consumer> consumer(
        RdKafka::Consumer::create(conf.get(), errstr));
    if (!consumer) {
      std::cout << "Error creating consumer: " << errstr << '\n';
    }
    return std::move(consumer);
  }

  virtual void deleteTopic(const std::string& topicName) override {
    // rd_kafka_DeleteTopics is ASYNCHRONOUS: the request goes to the cluster and
    // the outcome comes back as an event on a queue. The previous implementation
    // built a DeleteTopic handle and immediately destroyed it without ever issuing
    // the request (the call sat inside a comment block), so deleting a topic
    // silently did nothing -- which the delete-topic tool and, later, replay's
    // scratch-topic cleanup both quietly depended on.
    const auto producer = createProducer();
    if (!producer) {
      std::cerr << "  [Kafka] deleteTopic(" << topicName
                << "): no client handle\n";
      return;
    }
    rd_kafka_t *handle = producer->c_ptr();
    if (handle == nullptr) {
      return;
    }

    rd_kafka_DeleteTopic_t *topic = rd_kafka_DeleteTopic_new(topicName.c_str());
    rd_kafka_DeleteTopic_t *topics[1] = {topic};
    rd_kafka_queue_t *queue = rd_kafka_queue_new(handle);
    rd_kafka_DeleteTopics(handle, topics, 1, nullptr, queue);

    // Wait for the result so a caller that deletes then immediately re-lists sees a
    // consistent cluster. Bounded: a broker that never answers must not wedge us.
    constexpr int kTimeoutMs = 10000;
    rd_kafka_event_t *event = rd_kafka_queue_poll(queue, kTimeoutMs);
    if (event == nullptr) {
      std::cerr << "  [Kafka] deleteTopic(" << topicName
                << "): timed out waiting for the cluster\n";
    } else {
      const rd_kafka_DeleteTopics_result_t *result =
          rd_kafka_event_DeleteTopics_result(event);
      if (result == nullptr) {
        std::cerr << "  [Kafka] deleteTopic(" << topicName << "): "
                  << rd_kafka_event_error_string(event) << '\n';
      } else {
        size_t count = 0;
        const rd_kafka_topic_result_t **results =
            rd_kafka_DeleteTopics_result_topics(result, &count);
        for (size_t index = 0; index < count; ++index) {
          const auto error = rd_kafka_topic_result_error(results[index]);
          // UNKNOWN_TOPIC_OR_PART just means someone got there first.
          if (error != RD_KAFKA_RESP_ERR_NO_ERROR &&
              error != RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART) {
            std::cerr << "  [Kafka] deleteTopic("
                      << rd_kafka_topic_result_name(results[index]) << "): "
                      << rd_kafka_topic_result_error_string(results[index])
                      << '\n';
          }
        }
      }
      rd_kafka_event_destroy(event);
    }

    rd_kafka_queue_destroy(queue);
    rd_kafka_DeleteTopic_destroy(topic);
  }

  virtual StreamTimeExtent
  queryStreamTime(const std::string &topicName,
                  int64_t timestampUs = -1) const override {
    StreamTimeExtent extent{};
    const auto consumer = createConsumer();
    if (!consumer) {
      return extent;
    }
    constexpr int32_t kPartition = 0;
    constexpr int kTimeoutMs = 5000;

    int64_t low = -1;
    int64_t high = -1;
    const auto err = consumer->query_watermark_offsets(
        topicName, kPartition, &low, &high, kTimeoutMs);
    if (err != RdKafka::ERR_NO_ERROR) {
      std::cerr << "  [Kafka] query_watermark_offsets(" << topicName
                << ") failed: " << RdKafka::err2str(err) << '\n';
      return extent;
    }
    extent.valid = true;
    extent.earliestOffset = low;
    extent.latestOffset = high;

    // Map a timestamp -> offset (offsets_for_times). rdkafka takes the target
    // timestamp in MILLISECONDS in the TopicPartition offset field and replaces
    // it with the first offset at/after that time (or OFFSET_END if none).
    if (timestampUs >= 0) {
      std::vector<RdKafka::TopicPartition *> parts;
      auto *tp = RdKafka::TopicPartition::create(topicName, kPartition);
      tp->set_offset(timestampUs / 1000);  // us -> ms
      parts.push_back(tp);
      const auto ofterr = consumer->offsetsForTimes(parts, kTimeoutMs);
      if (ofterr == RdKafka::ERR_NO_ERROR && !parts.empty()) {
        extent.offsetForTimestamp = parts[0]->offset();
      } else if (ofterr != RdKafka::ERR_NO_ERROR) {
        std::cerr << "  [Kafka] offsetsForTimes(" << topicName
                  << ") failed: " << RdKafka::err2str(ofterr) << '\n';
      }
      for (auto *p : parts) {
        delete p;
      }
    }
    return extent;
  }

  virtual std::vector<std::string>
  getAllTopicStrings(bool includeHiddenTopics = false) const override {
    const auto consumer = createConsumer();
    if (!consumer) {
      return {};
    }
    const auto metadata = createMetadata(consumer);
    if (!metadata) {
      return {};
    }

    std::vector<std::string> topics{};
    for (const auto &topicIt : *metadata->topics()) {
      const auto topic = topicIt->topic();
      if (not includeHiddenTopics and isTopicHidden(topic)) {
        continue;
      } else {
        topics.emplace_back(topicIt->topic());
      }
    }

    return topics;
  }

  virtual std::vector<std::unique_ptr<core::BasicTopicInformation>>
  getAllTopics() const override {
    std::vector<std::unique_ptr<core::BasicTopicInformation>> topics{};
    for (const auto &topicString : getAllTopicStrings()) {
      auto topicMaybe = core::BasicTopicInformation::create(topicString);
      if (topicMaybe.has_value()) {
        topics.push_back(std::move(topicMaybe.value()));
      }
    }

    return topics;
  }

  virtual std::vector<std::unique_ptr<core::RawStream>>
  getAllStreams() const override {
    auto topics = getAllTopics();
    std::unordered_set<uint64_t> ids{};
    for (const auto &topic : topics) {
      ids.insert(topic->id);
    }
    std::unordered_map<uint64_t,
                       std::vector<std::unique_ptr<core::BasicTopicInformation>>>
        topicsById{};
    for (const auto &id : ids) {
      topicsById.insert(
          {id, std::vector<std::unique_ptr<core::BasicTopicInformation>>{}});
    }
    for (auto &topic : topics) {
      topicsById[topic->id].push_back(std::move(topic));
    }
    std::vector<std::unique_ptr<core::RawStream>> streams{};
    for (auto &[id, topicsForId] : topicsById) {
      auto streamMaybe = core::RawStream::create(std::move(topicsForId));
      if (streamMaybe.has_value()) {
        streams.push_back(std::move(streamMaybe.value()));
      }
    }

    return streams;
  }

  virtual std::unique_ptr<core::TopicMessenger> createMessenger(
      const std::shared_ptr<core::BasicTopicInformation> &topicInfo,
      int64_t startOffset = -1) const override {
    const auto topics = getAllTopics();
    bool doesTopicExist = false;
    for (const auto &topic : topics) {
      if (*topic == *topicInfo) {
        doesTopicExist = true;
        break;
      }
    }

    if (!doesTopicExist) {
      // TODO?
    }
    // const auto stream = std::make_shared<Stream>("TODO", topicInfo->type,
    // topicInfo->id, topicInfo->encoderName, topicInfo->schemaName);
    const auto producer = createProducer();
    const auto consumer = createConsumer();
    const auto topicHandle = nat::util::asShared(createTopicHandle(topicInfo->toTopicString(), *consumer));
    auto messagingQueue = std::unique_ptr<core::MessagingQueue>(new BrokerMessagingQueue(
        topicInfo->toTopicString(), producer, consumer, std::move(topicHandle), {},
        startOffset));
    auto translator = std::make_unique<core::TopicTranslator>(topicInfo, registry);
    return std::make_unique<core::TopicMessenger>(std::move(messagingQueue),
                                            std::move(translator));
  }

  virtual std::shared_ptr<core::Registry> getRegistry() const override {
    return registry;
  }

  virtual std::unique_ptr<RdKafka::Topic>
  createTopicHandle(const std::string &topicName,
                    RdKafka::Consumer &consumer) const override {
    std::string errorString;
    std::unique_ptr<RdKafka::Topic> topic{
        RdKafka::Topic::create(&consumer, topicName, topicConfig.get(), errorString)};
    if (!topic) {
      std::cerr << "Failed to create topic: " << errstr << std::endl;
      exit(1);
    }

    return std::move(topic);
  }

private:
  std::unique_ptr<RdKafka::Conf> createTopicConfig() const {
    return std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  }

  std::unique_ptr<RdKafka::Topic>
  createTopic(const std::shared_ptr<RdKafka::Producer> &producer,
              const std::string &name) const {
    const auto topicConfig = createTopicConfig();
    return std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(
        producer.get(), name, topicConfig.get(), errstr));
  }

  std::unique_ptr<RdKafka::Metadata>
  createMetadata(const std::shared_ptr<RdKafka::Consumer> &consumer) const {
    class RdKafka::Metadata *metadata = nullptr;
    RdKafka::ErrorCode err = consumer->metadata(true, nullptr, &metadata, 5000);
    if (err != RdKafka::ERR_NO_ERROR || metadata == nullptr) {
      return nullptr;
    }
    return std::unique_ptr<RdKafka::Metadata>(metadata);
  }
};

std::unique_ptr<BrokerManager> createBrokerManager(const std::string &address,
                                                   const std::string &port) {
  auto manager = std::make_unique<BrokerManagerImpl>(address, port);
  return manager;
}

} // namespace nat::kafka
