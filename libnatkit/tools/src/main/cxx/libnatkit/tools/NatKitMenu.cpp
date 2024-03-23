#include <chrono>
#include <iostream>
#include <cmath>
#include <thread>

#include <libnatkit/core/kafka/broker/BrokerManager.hpp>
#include <libnatkit-core.hpp>
#include <libnatkit/util/Strings.hpp>
#include <libnatkit/util/Vectors.hpp>

using namespace std::chrono_literals;

enum class MenuOptions {
  DeleteTopic,
  DeleteAllTopics,
  ListStreams,
  ListTopics,
  Quit,
  ReadTopic,
  SimulateNatImu
};

static std::vector<std::string> MenuOptionStrings = {
  "d:  Delete Topic",
  "da: Delete All Topics",
  "ls: List Streams",
  "lt:  List Topics",
  "q:  Quit",
  "r:  Read Topic",
  "s:  Simulate natIMU"
};

MenuOptions getMenuOption() {
  while (true) {
    std::cout << '\n';
    for (const auto& optionString : MenuOptionStrings)
      std::cout << optionString << '\n';
    std::cout << "Enter an option: ";
    std::string line;
    std::getline(std::cin, line);
    line = nat::util::Strings::toLowercase(line);
    if (line == "d") {
      return MenuOptions::DeleteTopic;
    } else if (line == "da") {
      return MenuOptions::DeleteAllTopics;
    } else if (line == "ls") {
      return MenuOptions::ListStreams;
    } else if (line == "lt") {
      return MenuOptions::ListTopics;
    } else if (line == "q") {
      return MenuOptions::Quit;
    } else if (line == "s") {
      return MenuOptions::SimulateNatImu;
    } else if (line == "r") {
      return MenuOptions::ReadTopic;
    } else
      std::cout << "Invalid Option\n";
  }
}

void printTopics(const std::vector<std::unique_ptr<nat::core::BasicTopicInformation>>& topics) {
  for (unsigned int i = 0; i < topics.size(); ++i)
    std::cout << "(" << i << ") - " << topics[i]->toTopicString() << '\n';
}

std::unique_ptr<nat::core::BasicTopicInformation> promptUserToChooseTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  auto topics = manager->getAllTopics();
  printTopics(topics);
  int maxIndex = std::max(static_cast<int>(topics.size()-1), 0);
  std::cout << "Select a topic [0-" << maxIndex << "]: ";
  std::string selectedTopicIndex;
  std::getline(std::cin, selectedTopicIndex);
  int index = std::stoi(selectedTopicIndex);
  if (index < 0 || index > topics.size()) {
    std::cerr << selectedTopicIndex << " is not in the range [0-" << maxIndex << "]\n";
    exit(1);
  }
  return std::move(topics[index]);
}

std::pair<std::string, std::string> getBroker() {
  const auto hostCStr = std::getenv("NATKIT_SERVER_HOST");
  const auto portCStr = std::getenv("NATKIT_SERVER_PORT");
  std::string host = "localhost";
  std::string port = "9092";
  if (hostCStr != nullptr && portCStr != nullptr) {
    host = hostCStr;
    port = portCStr;
  }
  std::cout << "Enter broker in the format <hostname>:<port> [" << host << ":" << port << "]: ";
  std::string broker;
  std::getline(std::cin, broker);
  if (broker == "") {
    return {host, port};
  } else {
    const auto splitBroker = nat::util::Strings::split(broker, ':');
    if (splitBroker.size() != 2) {
      std::cerr << "broker takes the form of <hostname>:<port>\n";
      exit(1);
    }
    const auto host = splitBroker[0];
    const auto port = splitBroker[1];

    return {host, port};
  }
}

void deleteTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  const auto topic = promptUserToChooseTopic(manager);
  manager->deleteTopic(topic->toTopicString());
}

void deleteAllTopics(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  const auto topics = manager->getAllTopicStrings();
  for (const auto& topic : topics)
    manager->deleteTopic(topic);
}

void listCurrentStreams(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  auto topics = manager->getAllTopics();
  std::sort(topics.begin(), topics.end(), [](const auto& a, const auto& b) {
    return a->id > b->id;
  });
  std::vector<std::vector<std::unique_ptr<nat::core::BasicTopicInformation>>> streamTopics{};
  uint64_t lastId = -1;
  for (auto it = topics.begin(); it != topics.end(); ++it) {
    if ((*it)->id != lastId)
      streamTopics.push_back({});
    lastId = (*it)->id;
    streamTopics[streamTopics.size() - 1].push_back(std::move(*it));
  }

  std::vector<std::unique_ptr<nat::core::RawStream>> streams{};
  for (auto it = streamTopics.begin(); it != streamTopics.end(); ++it) {
    auto streamMaybe = nat::core::RawStream::create(std::move(*it));
    if (streamMaybe.has_value())
      streams.push_back(std::move(streamMaybe.value()));
  }
  std::cout << streamTopics.size() << '\n';

  for (const auto& stream : streams) {
    std::cout << stream->toString() << '\n';
  }
}

void listCurrentTopics(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  const auto topics = manager->getAllTopicStrings();
  std::cout << "There are currently " << topics.size() << " topics:\n";
  for (const auto& topic : topics)
    std::cout << "    " << topic << '\n';
}

void readTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  const std::shared_ptr<nat::core::BasicTopicInformation> topic = promptUserToChooseTopic(manager);
  const auto registry = manager->getRegistry();
  const auto messenger = manager->createMessenger(topic);
  const auto initailWaitUntil = std::chrono::system_clock::now() + 1s;
  bool firstMessageRecieved = false;
  while (true) {
    const auto messageMaybe = messenger->tryGetNexMessage();
    if (!messageMaybe.has_value()) {
      if (firstMessageRecieved || std::chrono::system_clock::now() > initailWaitUntil)
        break;
      else
        continue;
    }
    firstMessageRecieved = true;
    std::cout << messageMaybe.value()->toString() << '\n';
  }
}

void simulateNatImu(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
  std::cout << "Enter a name for the simulated IMU device: ";
  std::string name;
  std::getline(std::cin, name);
  std::vector<std::string> encodingTypes = { "Json" };
  std::cout << "Enter an encoding type, one of " << nat::util::Vectors::toString(encodingTypes) << ": ";  
  std::string serializationTypeString;
  std::getline(std::cin, serializationTypeString);
  std::cout << "Enter an id: ";
  std::string idString;
  std::getline(std::cin, idString);
  uint64_t id = std::stoll(idString);
  std::string metaSchemaTypeName = "BasicMetaInfoSchema";
  std::string dataSchemaTypeName = "NatImuDataSchema";
  const auto serializationType = nat::core::getSerializationType(serializationTypeString);
  const nat::core::Stream metaStream(name, nat::core::StreamType::META, id, serializationTypeString, metaSchemaTypeName);
  const nat::core::Stream dataStream(name, nat::core::StreamType::DATA, id, serializationTypeString, dataSchemaTypeName);
  const std::string metaTopicString = metaStream.toTopicString();
  const std::string dataTopicString = dataStream.toTopicString();
  auto basicMetaTopicInfoMaybe = nat::core::BasicTopicInformation::create(metaTopicString);
  if (!basicMetaTopicInfoMaybe.has_value()) {
    std::cerr << metaTopicString << " is not a valid topic name!\n";
    return;
  }
  auto dataTopicInfoMaybe = nat::core::BasicTopicInformation::create(dataTopicString);
  if (!dataTopicInfoMaybe.has_value()) {
    std::cerr << dataTopicString << " is not a valid topic name!\n";
    return;
  }
  const auto metaTopicInfo = std::make_shared<nat::core::BasicTopicInformation>(
    *std::move(basicMetaTopicInfoMaybe.value()));
  const auto dataTopicInfo = std::make_shared<nat::core::BasicTopicInformation>(
    *std::move(dataTopicInfoMaybe.value()));
  const auto metaMessenger = manager->createMessenger(metaTopicInfo);
  const auto dataMessenger = manager->createMessenger(dataTopicInfo);
  metaMessenger->sendMessage(nat::core::BasicMetaInfoSchema(name));
  float imuData[9];
  for (int i = 0; i < 100; ++i) {
    for (int j = 0; j < 9; ++j)
      imuData[j] = (rand() % 100000) / 1000.0;
    dataMessenger->sendMessage(nat::core::NatImuDataSchema(rand(), imuData, 9));
  }
}

int main(int argc, char **argv) {
  //if (argc != 1) {
  //  std::cerr << "Usage: " << argv[0] << " takes no arguments\n";
  //  exit(1);
  //}

  const auto [brokerHost, brokerPort] = getBroker();

  //const auto topicInfoMaybe = nat::core::BasicTopicInformation::create(topic);
  //if (!topicInfoMaybe.has_value()) {
  //  std::cerr
  //      << "<topic> takes the form of <stream-type>-<id>-<encoder>-<schema>\n";
  //  exit(1);
  //}
  //const auto topicInfo = std::make_shared<nat::core::BasicTopicInformation>(
  //    *topicInfoMaybe.value());

  const auto manager = nat::kafka::createBrokerManager(brokerHost, brokerPort);
  bool continueRunning = true;
  while (continueRunning) {
    switch (getMenuOption()) {
      case MenuOptions::DeleteTopic:
        deleteTopic(manager);
        break;

      case MenuOptions::DeleteAllTopics:
        deleteAllTopics(manager);
        break;

      case MenuOptions::ListStreams:
        listCurrentStreams(manager);
        break;

      case MenuOptions::ListTopics:
        listCurrentTopics(manager);
        break;

      case MenuOptions::Quit:
        continueRunning = false;
        break;

      case MenuOptions::ReadTopic:
        readTopic(manager);
        break;

      case MenuOptions::SimulateNatImu:
        simulateNatImu(manager);
        break;
    }
  }
  //const auto registry = manager->getRegistry();
  //const auto messenger = manager->createMessenger(topicInfo);
  //auto timeoutTimeStart = std::chrono::system_clock::now();
  //const auto timeoutTime = 5s;
  //while (true) {
  //  const auto messageMaybe = messenger->tryGetNexMessage();
  //  if (messageMaybe.has_value()) {
  //    std::cout << "Received Message: " << messageMaybe.value()->toString()
  //              << '\n';
  //    timeoutTimeStart = std::chrono::system_clock::now();
  //  } else if (std::chrono::system_clock::now() - timeoutTimeStart >
  //             timeoutTime) {
  //    std::cout << "No more messages\n";
  //    break;
  //  }
  //}

  return 0;
}
