#pragma once

#include <memory>

#include <libnatkit-core.hpp>

#include <librdkafka/rdkafkacpp.h>

namespace nat::kafka {

class BrokerManager {
public:
  virtual bool isConnected() const = 0;

  virtual std::shared_ptr<RdKafka::Producer> createProducer() const = 0;

  virtual std::shared_ptr<RdKafka::Consumer> createConsumer() const = 0;

  virtual void deleteTopic(const std::string&) = 0;

  virtual std::vector<std::string>
  getAllTopicStrings(bool includeHiddenTopics = false) const = 0;

  virtual std::vector<std::unique_ptr<core::BasicTopicInformation>>
  getAllTopics() const = 0;

  virtual std::vector<std::unique_ptr<core::RawStream>> getAllStreams() const = 0;

  virtual std::unique_ptr<core::TopicMessenger> createMessenger(
      const std::shared_ptr<core::BasicTopicInformation> &topicInfo) const = 0;

  virtual std::shared_ptr<core::Registry> getRegistry() const = 0;

  virtual std::unique_ptr<RdKafka::Topic>
  createTopicHandle(const std::string &topicName,
                    RdKafka::Consumer &consumer) const = 0;
};

std::unique_ptr<BrokerManager> createBrokerManager(const std::string &address,
                                                   const std::string &port);

} // namespace nat::kafka
