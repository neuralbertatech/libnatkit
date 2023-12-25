#include <libnatkit/core/bridge/KafkaMosquittoBridge.hpp>
#include <libnatkit/util/Casting.hpp>

namespace nat::bridge {

std::optional<std::unique_ptr<KafkaMosquittoBridge>> KafkaMosquittoBridge::create(
    std::shared_ptr<kafka::BrokerManager> kafkaBroker,
    std::shared_ptr<mosquitto::MosquittoBroker> mosquittoBroker) {
  auto bridge = std::unique_ptr<KafkaMosquittoBridge>(new KafkaMosquittoBridge(kafkaBroker, mosquittoBroker));
  const auto mosquittoTopicString = "natKit/#";
  bool wasMosquittoClientCreated = bridge->initMosquittoClient();
  if (wasMosquittoClientCreated) {
    bridge->start();
    return std::move(bridge);
  } else {
    return {};
  }
}

std::optional<std::shared_ptr<kafka::message_t>> KafkaMosquittoBridge::getNextKafkaMessage() {
  //return kafkaMessenger->tryGetNextMessage(); 
  return {};
}

} // namespace nat::bridge
