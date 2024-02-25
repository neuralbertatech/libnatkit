#include <libnatkit/core/kafka/broker/BrokerManager.hpp>

#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <libnatkit/util/Casting.hpp>
#include <libnatkit/core/kafka/broker/BrokerMessagingQueue.hpp>

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
    if (message.err())
      std::cerr << "% Message delivery failed: " << message.errstr()
                << std::endl;
    else
      std::cerr << "% Message delivered to topic " << message.topic_name()
                << " [" << message.partition() << "] at offset "
                << message.offset() << std::endl;
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
  ExampleDeliveryReportCb ex_dr_cb{};

public:
  BrokerManagerImpl(const std::string &address, const std::string &port)
      : address(address), port(port),
        conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
        topicConfig(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC)),
        registry(core::Registry::createDefaultInitalizeRegistry()) {
    if (conf->set("bootstrap.servers", address + ":" + port, errstr) ==
        RdKafka::Conf::CONF_OK) {
      connectedToBroker = true;
    } else {
      std::cout << "Error connecting to broker: " << errstr << '\n';
    }
    if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
      std::cerr << errstr << std::endl;
      exit(1);
    }

  }

  virtual bool isConnected() const override { return connectedToBroker; }

  virtual std::shared_ptr<RdKafka::Producer> createProducer() const override {
    const std::shared_ptr<RdKafka::Producer> producer(
        RdKafka::Producer::create(conf.get(), errstr));
    if (!producer) {
      std::cout << "Error creating producer: " << errstr << '\n';
    }
    return std::move(producer);
  }

  virtual std::shared_ptr<RdKafka::Consumer> createConsumer() const override {
    const std::shared_ptr<RdKafka::Consumer> consumer(
        RdKafka::Consumer::create(conf.get(), errstr));
    if (!consumer) {
      std::cout << "Error creating consumer: " << errstr << '\n';
    }
    return std::move(consumer);
  }

  virtual void deleteTopic(const std::string& topicName) override {
    auto topicDeleter = rd_kafka_DeleteTopic_new(topicName.c_str());
    rd_kafka_DeleteTopic_destroy(topicDeleter);
    /**
 * @brief Delete topics from cluster as specified by the \p topics
 *        array of size \p topic_cnt elements.
 *
 * @param topics Array of topics to delete.
 * @param topic_cnt Number of elements in \p topics array.
 * @param options Optional admin options, or NULL for defaults.
 * @param rkqu Queue to emit result on.
 *
 * @remark The result event type emitted on the supplied queue is of type
 *         \c RD_KAFKA_EVENT_DELETETOPICS_RESULT
RD_EXPORT
void rd_kafka_DeleteTopics (rd_kafka_t *rk,
                                  rd_kafka_DeleteTopic_t **del_topics,
                                  size_t del_topic_cnt,
                                  const rd_kafka_AdminOptions_t *options,
                                  rd_kafka_queue_t *rkqu);
                                  */
  }

  virtual std::vector<std::string>
  getAllTopicStrings(bool includeHiddenTopics = false) const override {
    const auto consumer = createConsumer();
    const auto metadata = createMetadata(consumer);

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
      const std::shared_ptr<core::BasicTopicInformation> &topicInfo) const override {
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
        topicInfo->toTopicString(), producer, consumer, std::move(topicHandle)));
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
    class RdKafka::Metadata *metadata;
    consumer->metadata(true, 0, &metadata, 5000);
    return std::unique_ptr<RdKafka::Metadata>(metadata);
  }
};

std::unique_ptr<BrokerManager> createBrokerManager(const std::string &address,
                                                   const std::string &port) {
  auto manager = std::make_unique<BrokerManagerImpl>(address, port);
  return manager;
}

} // namespace nat::kafka
