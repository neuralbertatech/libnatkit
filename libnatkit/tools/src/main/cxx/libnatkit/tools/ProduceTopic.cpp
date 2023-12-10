#include <iostream>
#include <libnatkit/kafkit/core/broker/BrokerManager.hpp>
#include <libnatkit/kafkit/core/schemas/BasicMetaInfoSchema.hpp>
#include <libnatkit/kafkit/core/stream/BasicTopicInformation.hpp>

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
  const auto topicInfoMaybe = nat::kafkit::BasicTopicInformation::create(topic);
  if (!topicInfoMaybe.has_value()) {
    std::cerr
        << "<topic> takes the form of <stream-type>-<id>-<encoder>-<schema>\n\""
        << topic << "\" does not follow that form\n";
    exit(1);
  }
  const auto topicInfo = std::make_shared<nat::kafkit::BasicTopicInformation>(
      *topicInfoMaybe.value());

  const auto manager = nat::kafkit::createBrokerManager(brokerHost, brokerPort);
  const auto decoder =
      std::make_shared<nat::kafkit::BasicMetaInfoSchema>("TODO");
  const auto messenger = manager->createMessenger(topicInfo);

  std::string line;
  while (true) {
    std::cout << "Stream name ('q' to quit): ";
    std::getline(std::cin, line);
    if (line == "q") {
      break;
    }
    const auto schema = nat::kafkit::BasicMetaInfoSchema(line);
    messenger->sendMessage(schema);
  }

  return 0;
}
