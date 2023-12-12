#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

#include <libnatkit/kafkit/core/broker/BrokerMessagingQueue.hpp>
#include <libnatkit/kafkit/core/registry/Decoder.hpp>
#include <libnatkit/kafkit/core/registry/Encoder.hpp>
#include <libnatkit/kafkit/core/registry/Message.hpp>
#include <libnatkit/kafkit/core/registry/Registry.hpp>
#include <libnatkit/kafkit/core/stream/BasicTopicInformation.hpp>

namespace nat::kafkit {

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

} // namespace nat::kafkit
