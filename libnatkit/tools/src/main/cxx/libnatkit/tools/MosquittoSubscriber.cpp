#include <chrono>
#include <iostream>
#include <mosquitto.h>
#include <stdio.h>
#include <stdlib.h>
#include <thread>

#include <libnatkit/core/mqtt/MosquittoBroker.hpp>
#include <libnatkit/util/Strings.hpp>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <broker> <topic>\n";
    exit(1);
  }

  const std::string broker = argv[1];
  const std::string topic = argv[2];
  const auto splitBroker = nat::util::Strings::split(broker, ':');
  if (splitBroker.size() != 2) {
    std::cerr << "<broker> takes the form of <hostname>:<port>\n";
    exit(1);
  }
  const auto brokerHost = splitBroker[0];
  const auto brokerPort = std::stoi(splitBroker[1]);

  {
    const auto manager =
        nat::mosquitto::createMosquittoBroker(brokerHost, brokerPort);
    const auto client = manager->createClient(topic);
    if (client.has_value()) {
      std::cout << "Successfully created a client\n";
      std::this_thread::sleep_for(30s);
    } else {
      std::cout << "Failed to create a client\n";
    }
  }

  std::this_thread::sleep_for(2s);

  return 0;
}
