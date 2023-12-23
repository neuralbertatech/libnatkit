#include <iostream>

#include <libnatkit/core/mqtt/MosquittoBroker.hpp>
#include <libnatkit/util/Strings.hpp>


int main(int argc, char *argv[])
{
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
    auto publisherMaybe = manager->createPublisher(topic);
    if (publisherMaybe.has_value()) {
      std::cout << "Successfully created a publisher\n";
      std::string line;
      const auto publisher = std::move(publisherMaybe.value());
      while (true) {
        std::cout << "Message to send ('q' to quit): ";
        std::getline(std::cin, line);
        if (line == "q") {
          break;
        }
        publisher->sendMessage(line);
      }
    } else {
      std::cout << "Failed to create a publisher\n";
    }
  }

	return 0;
}
