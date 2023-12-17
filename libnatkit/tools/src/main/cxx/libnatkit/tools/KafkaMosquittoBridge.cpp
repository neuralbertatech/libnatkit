#include <iostream>

#include <libnatkit/util/Strings.hpp>
#include <libnatkit/core/bridge/KafkaToMosquittoBridge.hpp>

int main(int argc, char** argv) {
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

	return 0;
}
