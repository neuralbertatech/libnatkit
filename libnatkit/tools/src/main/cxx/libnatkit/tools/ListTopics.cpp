#include <iostream>
#include <libnatkit/core/kafka/broker/BrokerManager.hpp>

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <broker>\n";
    exit(1);
  }

  const std::string broker = argv[1];
  const auto splitBroker = nat::util::Strings::split(broker, ':');
  if (splitBroker.size() != 2) {
    std::cerr << "<broker> takes the form of <hostname>:<port>\n";
    exit(1);
  }
  const auto brokerHost = splitBroker[0];
  const auto brokerPort = splitBroker[1];

  const auto manager = nat::kafka::createBrokerManager(brokerHost, brokerPort);
	const auto topics = manager->getAllTopicStrings();
	for (size_t i = 0; i < std::ssize(topics); ++i) {
		std::cout << topics[i] << std::endl;
	}

	return 0;
}
