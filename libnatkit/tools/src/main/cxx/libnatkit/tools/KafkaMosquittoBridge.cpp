#include <chrono>
#include <iostream>
#include <optional>

#include <libnatkit-bridge.hpp>
#include <libnatkit/util/Casting.hpp>
#include <libnatkit/util/Strings.hpp>

using namespace std::chrono_literals;

#pragma warning(disable : 4996)
static std::optional<std::string> get_env(const char* env) {
    const char* t = std::getenv(env);
    if (t) return std::string(t);
    else return {};
}

int main(int argc, char **argv) {
    std::cout << std::flush;
    std::cerr << std::flush;
    std::cout.sync_with_stdio(true);
    std::cerr.sync_with_stdio(true);
    
    std::cout << "========================================\n" << std::flush;
    std::cout << "Kafka-Mosquitto Bridge Starting...\n" << std::flush;
    std::cout << "========================================\n" << std::flush;
    std::cout << "Argument count: " << argc << "\n" << std::flush;
    for (int i = 0; i < argc; ++i) {
        std::cout << "  argv[" << i << "]: " << argv[i] << "\n" << std::flush;
    }
    std::cout << "----------------------------------------\n" << std::flush;
    
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
          std::cerr << "No arguments passed to the CLI, and the environment variables have not been set. The following environment variables are:\nLIBNATKIT_KAFKA_BROKER_ADDRESS=\"" << (kafkaBrokerAddress.has_value() ? kafkaBrokerAddress.value() : "")
              << "\"\nLIBNATKIT_KAFKA_BROKER_PORT=\"" << (kafkaBrokerPort.has_value() ? kafkaBrokerPort.value() : "")
              << "\"\nLIBNATKIT_MQTT_BROKER_ADDRESS=\"" << (mqttBrokerAddress.has_value() ? mqttBrokerAddress.value() : "")
              << "\"\nLIBNATKIT_MQTT_BROKER_PORT=\"" << (mqttBrokerPort.has_value() ? mqttBrokerPort.value() : "") << "\"\n";
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

  std::cout << "\n========================================\n";
  std::cout << "Broker Configuration:\n";
  std::cout << "========================================\n";
  std::cout << "Kafka Broker: " << kafkaBrokerAddress.value() << ":" << kafkaBrokerPort.value() << "\n";
  std::cout << "MQTT Broker:  " << mqttBrokerAddress.value() << ":" << mqttBrokerPort.value() << "\n";
  std::cout << "----------------------------------------\n\n";

  std::cout << "[1/4] Creating Kafka Broker Manager...\n";
  auto kafkaBroker =
      nat::kafka::createBrokerManager(kafkaBrokerAddress.value(), kafkaBrokerPort.value());
  if (!kafkaBroker) {
    std::cerr << "FATAL: Failed to create Kafka Broker Manager!\n";
    return 1;
  }
  std::cout << "✓ Kafka Broker Manager created\n";
  std::cout << "  Connection status: " << (kafkaBroker->isConnected() ? "CONNECTED" : "NOT CONNECTED") << "\n\n";

  std::cout << "[2/4] Creating Mosquitto Broker...\n";
  auto mosquittoBroker =
      nat::mosquitto::createMosquittoBroker(mqttBrokerAddress.value(), std::stoi(mqttBrokerPort.value()));
  if (!mosquittoBroker) {
    std::cerr << "FATAL: Failed to create Mosquitto Broker!\n";
    return 1;
  }
  std::cout << "✓ Mosquitto Broker created\n\n";

  std::cout << "[3/4] Creating Kafka-Mosquitto Bridge...\n";
  std::cout << "  This will attempt to connect to both brokers...\n";
  auto bridgeMaybe = nat::bridge::KafkaMosquittoBridge::create(
      nat::util::asShared(std::move(kafkaBroker)),
      nat::util::asShared(std::move(mosquittoBroker)));

  if (!bridgeMaybe.has_value()) {
    std::cerr << "\n========================================\n";
    std::cerr << "FATAL ERROR: Bridge Creation Failed\n";
    std::cerr << "========================================\n";
    std::cerr << "The bridge could not be created. Common causes:\n";
    std::cerr << "  1. Mosquitto broker is not reachable\n";
    std::cerr << "  2. Mosquitto broker refused the connection\n";
    std::cerr << "  3. Network connectivity issues\n";
    std::cerr << "  4. Broker services not yet ready\n";
    std::cerr << "\nPlease check the error messages above for details.\n";
    std::cerr << "========================================\n";
    return 1;
  }
  std::cout << "✓ Bridge created successfully!\n\n";

  auto bridge = std::move(bridgeMaybe.value());
  
  std::cout << "[4/4] Starting message loop...\n";
  std::cout << "========================================\n";
  std::cout << "Bridge is now running!\n";
  std::cout << "Listening for messages on both brokers...\n";
  std::cout << "========================================\n\n";
  
  //int max = 6000;
  //int current = 0;
  int messageCount = 0;
  while (true) {
    auto messageMaybe = bridge->getNextKafkaMessage();
    if (messageMaybe.has_value()) {
      messageCount++;
      std::string message{messageMaybe.value()->begin(),
                          messageMaybe.value()->end()};
      std::cout << "[Message #" << messageCount << "] Received from Kafka broker: " << message << '\n';
      //current = 0;
    } else {
      //++current;
      std::this_thread::sleep_for(5ms);
    }
  }

  std::cout << "\nBridge shutting down...\n";
  return 0;
}
