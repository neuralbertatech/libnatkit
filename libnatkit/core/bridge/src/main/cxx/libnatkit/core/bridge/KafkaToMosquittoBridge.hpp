#pragma once

#include <memory>

#include <libnatkit/core/kafka/broker/BrokerManager.hpp>
#include <libnatkit/core/mqtt/MosquittoBroker.hpp>

namespace nat::bridge {

class KafkaToMosquittoBridge {
  std::shared_ptr<kafka::BrokerManager> kafkaBroker;
  std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker;
  std::unique_ptr<kafka::TopicMessenger> kafkaMessenger;
  std::unique_ptr<mosquitto::MosquittoPublisher> mosquittoPublisher;

  KafkaToMosquittoBridge(std::shared_ptr<kafka::BrokerManager> kafkaBroker, std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker)
    : kafkaBroker(kafkaBroker), mosquittoBroker(mosquittoBroker) {}
  public:
  static std::optional<KafkaToMosquittoBridge> create(std::shared_ptr<kafka::BrokerManager> kafkaBroker, std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker) {

    return {};
  }
};

}
