#include <libnatkit/kafkit/core/broker/BrokerManager.hpp>

#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

namespace nat::kafkit {

static bool isTopicHidden(const std::string &topic_string) {
  if (topic_string.starts_with('_')) {
    return true;
  } else {
    return false;
  }
}

class BrokerManagerImpl : public BrokerManager {
  std::string address;
  std::string port;
  const std::unique_ptr<RdKafka::Conf> conf;
  bool connectedToBroker{false};
  mutable std::string errstr;

public:
  BrokerManagerImpl(const std::string &address, const std::string &port)
      : address(address), port(port),
        conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)) {
    if (conf->set("bootstrap.servers", address + ":" + port, errstr) ==
        RdKafka::Conf::CONF_OK) {
      connectedToBroker = true;
    } else {
      std::cout << "Error connecting to broker: " << errstr << '\n';
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

  virtual std::vector<std::unique_ptr<BasicTopicInformation>>
  getAllTopics() const override {
    std::vector<std::unique_ptr<BasicTopicInformation>> topics{};
    for (const auto &topicString : getAllTopicStrings()) {
      auto topicMaybe = BasicTopicInformation::create(topicString);
      if (topicMaybe.has_value()) {
        topics.push_back(std::move(topicMaybe.value()));
      }
    }

    return topics;
  }

  virtual std::vector<std::unique_ptr<RawStream>> getAllStreams() const override {
    auto topics = getAllTopics();
    std::unordered_set<uint64_t> ids{};
    for (const auto &topic : topics) {
      ids.insert(topic->id);
    }
    std::unordered_map<uint64_t,
                       std::vector<std::unique_ptr<BasicTopicInformation>>>
        topicsById{};
    for (const auto &id : ids) {
      topicsById.insert(
          {id, std::vector<std::unique_ptr<BasicTopicInformation>>{}});
    }
    for (auto &topic : topics) {
      topicsById[topic->id].push_back(std::move(topic));
    }
    std::vector<std::unique_ptr<RawStream>> streams{};
    for (auto &[id, topicsForId] : topicsById) {
      auto streamMaybe = RawStream::create(std::move(topicsForId));
      if (streamMaybe.has_value()) {
        streams.push_back(std::move(streamMaybe.value()));
      }
    }

    return streams;
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
  return std::make_unique<BrokerManagerImpl>(address, port);
}

}
