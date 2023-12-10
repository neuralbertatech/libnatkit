#pragma once

#include <memory>

#include <libnatkit/kafkit/core/registry/Decoder.hpp>
#include <libnatkit/kafkit/core/registry/Registry.hpp>
#include <libnatkit/kafkit/core/registry/TopicMessenger.hpp>
#include <libnatkit/kafkit/core/stream/BasicTopicInformation.hpp>
#include <libnatkit/kafkit/core/stream/RawStream.hpp>

#include <librdkafka/rdkafkacpp.h>

namespace nat::kafkit {

class BrokerManager {
public:
  virtual bool isConnected() const = 0;

  virtual std::shared_ptr<RdKafka::Producer> createProducer() const = 0;

  virtual std::shared_ptr<RdKafka::Consumer> createConsumer() const = 0;

  virtual std::vector<std::string>
  getAllTopicStrings(bool includeHiddenTopics = false) const = 0;

  virtual std::vector<std::unique_ptr<BasicTopicInformation>>
  getAllTopics() const = 0;

  virtual std::vector<std::unique_ptr<RawStream>> getAllStreams() const = 0;

  virtual std::unique_ptr<TopicMessenger> createMessenger(
      const std::shared_ptr<BasicTopicInformation> &topicInfo) const = 0;

  virtual std::shared_ptr<Registry> getRegistry() const = 0;

  virtual std::unique_ptr<RdKafka::Topic>
  createTopicHandle(const std::string &topicName,
                    RdKafka::Consumer &consumer) const = 0;
};

std::unique_ptr<BrokerManager> createBrokerManager(const std::string &address,
                                                   const std::string &port);

} // namespace nat::kafkit
