#include <chrono>
#include <iostream>
#include <thread>

#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>
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
  const auto brokerPort = splitBroker[1];
  const auto topicInfoMaybe = nat::core::BasicTopicInformation::create(topic);
  if (!topicInfoMaybe.has_value()) {
    std::cerr
        << "<topic> takes the form of <stream-type>-<id>-<encoder>-<schema>\n";
    exit(1);
  }
  const auto topicInfo = std::make_shared<nat::core::BasicTopicInformation>(
      *topicInfoMaybe.value());

  const auto manager = nat::kafka::createBrokerManager(brokerHost, brokerPort);
  const auto registry = manager->getRegistry();
  const auto messenger = manager->createMessenger(topicInfo);
  auto timeoutTimeStart = std::chrono::system_clock::now();
  const auto timeoutTime = 5s;
  while (true) {
    const auto messageMaybe = messenger->tryGetNexMessage();
    if (messageMaybe.has_value()) {
      std::cout << "Received Message: " << messageMaybe.value()->toString()
                << '\n';
      timeoutTimeStart = std::chrono::system_clock::now();
    } else if (std::chrono::system_clock::now() - timeoutTimeStart >
               timeoutTime) {
      std::cout << "No more messages\n";
      break;
    }
  }

  return 0;
}
