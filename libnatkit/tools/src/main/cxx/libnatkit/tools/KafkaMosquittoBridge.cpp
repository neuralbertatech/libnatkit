#include <chrono>
#include <iostream>
#include <optional>

#include <libnatkit-bridge.hpp>
#include <libnatkit/util/Casting.hpp>
#include <libnatkit/util/Strings.hpp>

using namespace std::chrono_literals;

#pragma warning(disable : 4996)
static std::optional<std::string> get_env(const char* env) {
    auto t = std::getenv(env);
    if (t) return t;
    else return {};
}

int main(int argc, char **argv) {
    std::optional<std::string> kafkaBrokerAddress{};
    std::optional<std::string> kafkaBrokerPort{};
    std::optional<std::string> mqttBrokerAddress{};
    std::optional<std::string> mqttBrokerPort{};
  if (argc != 3) {
      kafkaBrokerAddress = get_env("LIBNATKIT_KAFKA_BROKER_ADDRESS");
      kafkaBrokerPort = get_env("LIBNATKIT_KAFKA_BROKER_PORT");
      mqttBrokerAddress = get_env("LIBNATKIT_MQTT_BROKER_ADDRESS");
      mqttBrokerPort = get_env("LIBNATKIT_MQTT_BROKER_PORT");
      if (kafkaBrokerAddress.has_value() && kafkaBrokerPort.has_value() && mqttBrokerAddress.has_value() && mqttBrokerPort.has_value()) {
          std::cout << "No arguments passed to the CLI, but the environment variables have been set. Using the following:\nLIBNATKIT_KAFKA_BROKER_ADDRESS=\"" << kafkaBrokerAddress.value()
              << "\"\nLIBNATKIT_KAFKA_BROKER_PORT=\"" << kafkaBrokerPort.value()
              << "\"\nLIBNATKIT_MQTT_BROKER_ADDRESS=\"" << mqttBrokerAddress.value()
              << "\"\nLIBNATKIT_MQTT_BROKER_PORT=\"" << mqttBrokerPort.value() << "\"\n";
      }
      else {
          std::cerr << "No arguments passed to the CLI, and the environment variables have not been set. The following environment variables are:\nLIBNATKIT_KAFKA_BROKER_ADDRESS=\"" << kafkaBrokerAddress.value()
              << "\"\nLIBNATKIT_KAFKA_BROKER_PORT=\"" << kafkaBrokerPort.value()
              << "\"\nLIBNATKIT_MQTT_BROKER_ADDRESS=\"" << mqttBrokerAddress.value()
              << "\"\nLIBNATKIT_MQTT_BROKER_PORT=\"" << mqttBrokerPort.value() << "\"\n";
          exit(1);
      }
  }
  else {
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
      kafkaBrokerAddress = splitKafkaBroker[0];
      kafkaBrokerPort = splitKafkaBroker[1];
      mqttBrokerAddress = splitMosquittoBroker[0];
      mqttBrokerPort = splitMosquittoBroker[1];
  }

  auto kafkaBroker =
      nat::kafka::createBrokerManager(kafkaBrokerAddress.value(), kafkaBrokerPort.value());
  auto mosquittoBroker =
      nat::mosquitto::createMosquittoBroker(mqttBrokerAddress.value(), std::stoi(mqttBrokerPort.value()));
  auto bridgeMaybe = nat::bridge::KafkaMosquittoBridge::create(
      nat::util::asShared(std::move(kafkaBroker)),
      nat::util::asShared(std::move(mosquittoBroker)));

  if (!bridgeMaybe.has_value()) {
    std::cout << "Error: could not create a bridge!\n";
    return 1;
  }

  auto bridge = std::move(bridgeMaybe.value());
  //int max = 6000;
  //int current = 0;
  while (true) {
    auto messageMaybe = bridge->getNextKafkaMessage();
    if (messageMaybe.has_value()) {
      std::string message{messageMaybe.value()->begin(),
                          messageMaybe.value()->end()};
      std::cout << "Recieved message from kafka broker: " << message << '\n';
      //current = 0;
    } else {
      //++current;
      std::this_thread::sleep_for(5ms);
    }
  }

  return 0;
}
