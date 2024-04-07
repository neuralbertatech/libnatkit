#include <chrono>
#include <iostream>

#include <libnatkit-bridge.hpp>
#include <libnatkit/util/Casting.hpp>
#include <libnatkit/util/Strings.hpp>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <kafka-broker> <mosquitto-broker>\n";
    exit(1);
  }

  const std::string kafkaBrokerString = argv[1];
  const std::string mosquittoBrokerString = argv[2];
  const auto splitKafkaBroker = nat::util::Strings::split(kafkaBrokerString, ':');
  if (splitKafkaBroker.size() != 2) {
    std::cerr << "<kafka-broker> takes the form of <hostname>:<port>\n";
    exit(1);
  }
  const auto splitMosquittoBroker =
      nat::util::Strings::split(mosquittoBrokerString, ':');
  if (splitMosquittoBroker.size() != 2) {
    std::cerr << "<moquitto-broker> takes the form of <hostname>:<port>\n";
    exit(1);
  }
  const auto kafkaBrokerHost = splitKafkaBroker[0];
  const auto kafkaBrokerPort = splitKafkaBroker[1];
  const auto mosquittoBrokerHost = splitMosquittoBroker[0];
  const auto mosquittoBrokerPort = std::stoi(splitMosquittoBroker[1]);


  auto kafkaBroker =
      nat::kafka::createBrokerManager(kafkaBrokerHost, kafkaBrokerPort);
  auto mosquittoBroker =
      nat::mosquitto::createMosquittoBroker(mosquittoBrokerHost, mosquittoBrokerPort);
  auto bridgeMaybe = nat::bridge::KafkaMosquittoBridge::create(
      nat::util::asShared(std::move(kafkaBroker)),
      nat::util::asShared(std::move(mosquittoBroker)));

  if (!bridgeMaybe.has_value()) {
    std::cout << "Error: could not create a bridge!\n";
    return 1;
  }

  auto bridge = std::move(bridgeMaybe.value());
  int max = 6000;
  int current = 0;
  while (current < max) {
    auto messageMaybe = bridge->getNextKafkaMessage();
    if (messageMaybe.has_value()) {
      std::string message{messageMaybe.value()->begin(),
                          messageMaybe.value()->end()};
      std::cout << "Recieved message from kafka broker: " << message << '\n';
      current = 0;
    } else {
      ++current;
      std::this_thread::sleep_for(5ms);
    }
  }

  return 0;
}
