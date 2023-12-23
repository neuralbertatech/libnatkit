#include <libnatkit/core/bridge/KafkaToMosquittoBridge.hpp>

namespace nat::bridge {

std::optional<KafkaToMosquittoBridge> KafkaToMosquittoBridge::create(std::shared_ptr<kafka::BrokerManager> kafkaBroker, std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker) {

    return {};
  }

}