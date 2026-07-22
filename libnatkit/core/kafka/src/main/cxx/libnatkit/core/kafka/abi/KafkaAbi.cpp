/*
 * KafkaAbi.cpp -- extern "C" shim over the librdkafka-backed Kafka transport,
 * declared in libnatkit-kafka-abi.h. Follows the same conventions as
 * lib/libnatkit-core/src/StreamStitch.cpp: opaque handles, int status codes
 * (NAT_OK / NAT_ERR_*), no C++ exception crosses the boundary, and the two-call
 * size/fill pattern for variable-length output.
 */
#include <libnatkit-kafka-abi.h>

#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

// Opaque handle definitions (declared incomplete in the pure-C header).
struct nat_kafka_broker {
  std::unique_ptr<nat::kafka::BrokerManager> broker;
};

struct nat_kafka_messenger {
  std::unique_ptr<nat::core::TopicMessenger> messenger;
  // The two-call receive peeks a message into `pending` on the sizing call and
  // only consumes it once it has been copied into a large-enough buffer, so a
  // NAT_ERR_BUFFER_TOO_SMALL retry never drops data.
  std::shared_ptr<nat::core::message_t> pending;
};

extern "C" {

int nat_kafka_v1_broker_create(const char *host,
                               const char *port,
                               nat_kafka_broker_t **out_broker) {
  if (host == nullptr || port == nullptr || out_broker == nullptr) {
    return NAT_ERR_NULL_ARGUMENT;
  }
  try {
    auto broker = nat::kafka::createBrokerManager(host, port);
    if (!broker) {
      return NAT_ERR_INTERNAL;
    }
    *out_broker = new nat_kafka_broker{std::move(broker)};
    return NAT_OK;
  } catch (...) {
    return NAT_ERR_INTERNAL;
  }
}

void nat_kafka_v1_broker_destroy(nat_kafka_broker_t *broker) {
  delete broker; /* deleting nullptr is a no-op */
}

int nat_kafka_v1_broker_list_topics(nat_kafka_broker_t *broker,
                                    int include_hidden,
                                    char *out_topics,
                                    size_t *inout_topics_size) {
  if (broker == nullptr || inout_topics_size == nullptr) {
    return NAT_ERR_NULL_ARGUMENT;
  }
  try {
    const auto topics = broker->broker->getAllTopicStrings(include_hidden != 0);
    std::string joined;
    for (size_t i = 0; i < topics.size(); ++i) {
      if (i != 0) {
        joined.push_back('\n');
      }
      joined += topics[i];
    }

    const size_t required = joined.size() + 1; /* trailing NUL */
    if (out_topics == nullptr) {
      *inout_topics_size = required;
      return NAT_OK;
    }
    if (*inout_topics_size < required) {
      *inout_topics_size = required;
      return NAT_ERR_BUFFER_TOO_SMALL;
    }
    if (!joined.empty()) {
      std::memcpy(out_topics, joined.data(), joined.size());
    }
    out_topics[joined.size()] = '\0';
    *inout_topics_size = required;
    return NAT_OK;
  } catch (...) {
    return NAT_ERR_INTERNAL;
  }
}

int nat_kafka_v1_messenger_create(nat_kafka_broker_t *broker,
                                  const char *topic_string,
                                  int64_t start_offset,
                                  nat_kafka_messenger_t **out_messenger) {
  if (broker == nullptr || topic_string == nullptr || out_messenger == nullptr) {
    return NAT_ERR_NULL_ARGUMENT;
  }
  try {
    auto topicInfoMaybe =
        nat::core::BasicTopicInformation::create(topic_string);
    if (!topicInfoMaybe.has_value()) {
      return NAT_ERR_INVALID_ARGUMENT;
    }
    std::shared_ptr<nat::core::BasicTopicInformation> topicInfo(
        std::move(topicInfoMaybe.value()));
    auto messenger = broker->broker->createMessenger(topicInfo, start_offset);
    if (!messenger) {
      return NAT_ERR_INTERNAL;
    }
    *out_messenger = new nat_kafka_messenger{std::move(messenger), nullptr};
    return NAT_OK;
  } catch (...) {
    return NAT_ERR_INTERNAL;
  }
}

void nat_kafka_v1_messenger_destroy(nat_kafka_messenger_t *messenger) {
  delete messenger; /* joins the background poll thread via ~BrokerMessagingQueue */
}

int nat_kafka_v1_messenger_send(nat_kafka_messenger_t *messenger,
                                const uint8_t *payload,
                                size_t payload_size) {
  if (messenger == nullptr || (payload == nullptr && payload_size != 0)) {
    return NAT_ERR_NULL_ARGUMENT;
  }
  try {
    nat::core::message_t message(payload, payload + payload_size);
    messenger->messenger->sendRawMessage(message);
    return NAT_OK;
  } catch (...) {
    return NAT_ERR_INTERNAL;
  }
}

int nat_kafka_v1_messenger_flush(nat_kafka_messenger_t *messenger) {
  if (messenger == nullptr) {
    return NAT_ERR_NULL_ARGUMENT;
  }
  try {
    messenger->messenger->flush();
    return NAT_OK;
  } catch (...) {
    return NAT_ERR_INTERNAL;
  }
}

int nat_kafka_v1_messenger_try_recv(nat_kafka_messenger_t *messenger,
                                    uint8_t *out_payload,
                                    size_t *inout_payload_size,
                                    int *out_has_message) {
  if (messenger == nullptr || inout_payload_size == nullptr ||
      out_has_message == nullptr) {
    return NAT_ERR_NULL_ARGUMENT;
  }
  try {
    if (!messenger->pending) {
      auto next = messenger->messenger->tryGetNextRawMessage();
      if (next.has_value()) {
        messenger->pending = next.value();
      }
    }

    if (!messenger->pending) {
      *out_has_message = 0;
      return NAT_OK;
    }

    *out_has_message = 1;
    const size_t required = messenger->pending->size();
    if (out_payload == nullptr) {
      *inout_payload_size = required;
      return NAT_OK;
    }
    if (*inout_payload_size < required) {
      *inout_payload_size = required;
      return NAT_ERR_BUFFER_TOO_SMALL;
    }
    if (required > 0) {
      std::memcpy(out_payload, messenger->pending->data(), required);
    }
    *inout_payload_size = required;
    messenger->pending.reset();
    return NAT_OK;
  } catch (...) {
    return NAT_ERR_INTERNAL;
  }
}

} /* extern "C" */
