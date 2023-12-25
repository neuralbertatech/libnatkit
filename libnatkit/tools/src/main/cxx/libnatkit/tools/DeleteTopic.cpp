#include <iostream>
#include <libnatkit/core/kafka/broker/BrokerManager.hpp>
#include <libnatkit/core/streams/schemas/BasicMetaInfoSchema.hpp>
#include <libnatkit/core/streams/stream/BasicTopicInformation.hpp>

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
  const auto manager = nat::kafka::createBrokerManager(brokerHost, brokerPort);
  manager->deleteTopic(topic);  

  return 0;
}
