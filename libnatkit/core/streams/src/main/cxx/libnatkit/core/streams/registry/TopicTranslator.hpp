#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include <libnatkit/core/kafka/broker/BrokerMessagingQueue.hpp>
#include <libnatkit/core/streams/registry/Decoder.hpp>
#include <libnatkit/core/streams/registry/Encoder.hpp>
#include <libnatkit/core/streams/registry/Message.hpp>
#include <libnatkit/core/streams/registry/Registry.hpp>
#include <libnatkit/core/streams/stream/BasicTopicInformation.hpp>

namespace nat::kafka {

class TopicTranslator {
  const std::shared_ptr<Registry> registry;
  const std::shared_ptr<BasicTopicInformation> topicInfo;

public:
  TopicTranslator(const std::shared_ptr<BasicTopicInformation> &topicInfo,
                  const std::shared_ptr<Registry> &registry)
      : topicInfo(topicInfo), registry(registry) {}

  std::optional<std::unique_ptr<Schema>>
  tryDecodeMessage(const message_t &message) {
    return registry->tryDecode(message, *topicInfo);
  }

  std::optional<std::unique_ptr<message_t>>
  tryEncodeMessage(const Schema &schema) const {
    if (schema.isSerializationTypeSupported(topicInfo->serializationType)) {
      return schema.encodeToBytes(topicInfo->serializationType);
    } else {
      return {};
    }
  }
};

} // namespace nat::kafka
