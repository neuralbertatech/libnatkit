#include <iostream>
#include <libnatkit/kafkit/core/broker/BrokerManager.hpp>

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

  const auto manager = nat::kafkit::createBrokerManager(brokerHost, brokerPort);
	const auto streams = manager->getAllStreams();
	for (size_t i = 0; i < std::ssize(streams); ++i) {
		std::cout << streams[i]->toPrettyString() << std::endl;
	}

	return 0;
}
