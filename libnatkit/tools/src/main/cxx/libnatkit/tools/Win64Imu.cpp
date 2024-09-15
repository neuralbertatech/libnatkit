#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdlib.h>
#include <thread>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>
#include <libnatkit/util/Strings.hpp>
#include <libnatkit/util/Vectors.hpp>

#include <windows.h>

using namespace std::chrono_literals;

class KeyListener {
    std::queue<char> keysPressed{};
    bool newKeysPressedAvailable = false;
    std::thread keyPollingThread;
    std::mutex queueLock;

    void pollKeys() {
        std::array<bool, 256> lastKeyStates{};
        std::array<bool, 256> currentKeyStates{};
        std::array<char, 256> keyDown{};
        std::array<char, 256> keyUp{};

        while (true) {
            // Iterate through all possible key codes (0 to 255)
            bool stateChanged = false;
            int keyUpIndex = 0;
            int keyDownIndex = 0;
            for (int keyCode = 0; keyCode < 256; ++keyCode) {
                // Check if the key with keyCode is currently
                // pressed
                currentKeyStates[keyCode] = GetAsyncKeyState(keyCode) & 0x8000;
                if (currentKeyStates[keyCode] != lastKeyStates[keyCode]) {
                    stateChanged = true;
                    lastKeyStates[keyCode] = currentKeyStates[keyCode];
                    if (currentKeyStates[keyCode])
                        keyDown[keyDownIndex++] = static_cast<char>(keyCode);
                    else
                        keyUp[keyUpIndex++] = static_cast<char>(keyCode);
                }
            }

            if (stateChanged) {
                std::lock_guard<std::mutex> guard(queueLock);
                for (int index = 0; index < keyUpIndex; ++index) {
                    newKeysPressedAvailable = true;
                    keysPressed.emplace(keyUp[index]);
                }
            }

            // Add a small delay to avoid high CPU usage
            std::this_thread::sleep_for(10ms);
        }
    }

public:
    KeyListener() : keyPollingThread(&KeyListener::pollKeys, this) { }

    std::vector<char> getKeysPressed() {
        std::vector<char> keys;
        if (newKeysPressedAvailable) {
            std::lock_guard<std::mutex> guard(queueLock);
            do {
                keys.emplace_back(keysPressed.front());
                keysPressed.pop();
            } while (!keysPressed.empty());
            newKeysPressedAvailable = false;
        }

        return keys;
    }

    bool haveAnyKeysBeenPressed() { return newKeysPressedAvailable; }

    void clearBuffer() {
        if (newKeysPressedAvailable) {
            std::queue<char> emptyQueue{};
            {
                std::lock_guard<std::mutex> guard(queueLock);
                std::swap(emptyQueue, keysPressed);
                newKeysPressedAvailable = false;
            }
            std::this_thread::sleep_for(10ms);
        }
    }
};

KeyListener globalKeyListener{};

class TerminalScreen {
    static const std::string clearBufferCommand;


public:
    TerminalScreen() {}

    static void clearWindow() {
        //std::cout << clearBufferCommand;
        std::cout << "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n";
    }

    static void drawToScreen(const std::string& msg) {
        clearWindow();
        std::cout << msg;
    }
};

const std::string TerminalScreen::clearBufferCommand = (char)27 + "[3J";

enum class MenuOptions {
    DeleteTopic,
    DeleteAllTopics,
    StreamToFile,
    ListStreams,
    ListTopics,
    Quit,
    ReadTopic,
    SimulateNatImu
};

static std::vector<std::string> MenuOptionStrings = {
  "d:  Delete Topic",
  "da: Delete All Topics",
  "f:  Stream to File\n"
  "ls: List Streams",
  "lt:  List Topics",
  "q:  Quit",
  "r:  Read Topic",
  "s:  Simulate natIMU"
};

MenuOptions getMenuOption() {
    std::string command = "";
    while (true) {
        std::string menuOptions = "\n";
        for (const auto& optionString : MenuOptionStrings) {
            bool showCommand = true;
            for (int i = 0; i < command.size(); ++i)
                if (command[i] != optionString[i]) {
                    showCommand = false;
                    break;
                }
            if (showCommand)
                menuOptions.append(optionString + '\n');
        }
        globalKeyListener.clearBuffer();
        menuOptions.append("Enter an option: ");
        TerminalScreen::drawToScreen(menuOptions);
        while (!globalKeyListener.haveAnyKeysBeenPressed()) std::this_thread::sleep_for(10ms);
        const auto keysPressed = globalKeyListener.getKeysPressed();
        std::vector<int> keyCodesPressed = {};
        for (auto character : keysPressed) keyCodesPressed.push_back((int)character);
        menuOptions.append("\n Keys Pressed: " + nat::util::Vectors::toString(keyCodesPressed));
        TerminalScreen::drawToScreen(menuOptions);
                
        for (int i = 0; i < keysPressed.size(); ++i) {
            char characterPressed = nat::util::Strings::toLowercase(keysPressed[i]);
            if (!(characterPressed <= 'z' && characterPressed >= 'a') && !(characterPressed >= '0' && characterPressed <= '9') && characterPressed != (char)13)
                continue;
            if (characterPressed == 13)
                characterPressed = '\n';
            command += characterPressed;
            if (command == "d") {
                continue;
            }
            else if (command == "d\n") {
                return MenuOptions::DeleteTopic;
            }
            else if (command == "da") {
                return MenuOptions::DeleteAllTopics;
            }
            else if (command == "f") {
                return MenuOptions::StreamToFile;
            }
            else if (command == "l") {
                continue;
            }
            else if (command == "ls") {
                return MenuOptions::ListStreams;
            }
            else if (command == "lt") {
                return MenuOptions::ListTopics;
            }
            else if (command == "q") {
                return MenuOptions::Quit;
            }
            else if (command == "s") {
                return MenuOptions::SimulateNatImu;
            }
            else if (command == "r") {
                return MenuOptions::ReadTopic;
            }
            else {
                std::vector<int> keyCodesInCommand = {};
                for (auto character : command) keyCodesInCommand.push_back((int)character);
                std::cout << " Invalid Option: " << "\"" << command << "\" (" << nat::util::Vectors::toString(keyCodesInCommand) << ")\n";
                command = "";
            }
        }
    }
}

void printTopics(const std::vector<std::unique_ptr<nat::core::BasicTopicInformation>>& topics) {
    for (unsigned int i = 0; i < topics.size(); ++i)
        std::cout << "(" << i << ") - " << topics[i]->toTopicString() << '\n';
}

std::unique_ptr<nat::core::BasicTopicInformation> promptUserToChooseTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
    auto topics = manager->getAllTopics();
    printTopics(topics);
    int maxIndex = (std::max)(static_cast<int>(topics.size() - 1), 0);
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
    char envHostBuffer[256];
    char envPortBuffer[256];
    size_t bufferSize;
    auto hostEnvError = getenv_s(&bufferSize, envHostBuffer, sizeof(envHostBuffer), "NATKIT_SERVER_HOST");
    auto portEnvError = getenv_s(&bufferSize, envPortBuffer, sizeof(envPortBuffer), "NATKIT_SERVER_PORT");
    std::string host = "localhost";
    std::string port = "9092";
    if (hostEnvError != 0 && portEnvError != 0) {
        host = std::string(envHostBuffer);
        port = std::string(envPortBuffer);
    }
    std::cout << "Enter broker in the format <hostname>:<port> [" << host << ":" << port << "]: ";
    std::string broker;
    std::getline(std::cin, broker);
    if (broker == "") {
        return { host, port };
    }
    else {
        const auto splitBroker = nat::util::Strings::split(broker, ':');
        if (splitBroker.size() != 2) {
            std::cerr << "broker takes the form of <hostname>:<port>\n";
            exit(1);
        }
        const auto host = splitBroker[0];
        const auto port = splitBroker[1];

        return { host, port };
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
        if (streamMaybe.has_value()) {            
            streams.push_back(std::move(streamMaybe.value()));
        }
    }
    std::cout << streamTopics.size() << '\n';

    for (const auto& stream : streams) {
        auto metaTopics = stream->getTopicsByType(nat::core::StreamType::META);
        if (metaTopics.size() > 0) {
            const auto messenger = manager->createMessenger(std::move(metaTopics[0]));
            std::string name{};
            const auto initailWaitUntil = std::chrono::system_clock::now() + 50ms;
            bool firstMessageRecieved = false;
            while (true) {
                const auto messageMaybe = messenger->tryGetNexMessage();
                if (!messageMaybe.has_value()) {
                    if (firstMessageRecieved || std::chrono::system_clock::now() > initailWaitUntil) {
                        break;
                    }
                    else {
                        std::this_thread::sleep_for(1ms);
                        continue;
                    }
                }
                firstMessageRecieved = true;
                name = messageMaybe.value()->getName();
            }
            std::cout << stream->getId() << " (" << name << ")\n";
        }
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
            if (firstMessageRecieved || std::chrono::system_clock::now() > initailWaitUntil) {
                break;
            }
            else {
                std::this_thread::sleep_for(1ms);
                continue;
            }
        }
        firstMessageRecieved = true;
        std::string messageString = messageMaybe.value()->toString();
        std::cout << messageString << '\n';
    }
}

void streamToFile(const std::unique_ptr<nat::kafka::BrokerManager>& manager) {
    std::cout << "Enter a name for the file: ";
    std::string filename;
    std::getline(std::cin, filename);
    const std::shared_ptr<nat::core::BasicTopicInformation> topic = promptUserToChooseTopic(manager);
    std::ofstream file;
    file.open(filename);
    const auto registry = manager->getRegistry();
    const auto messenger = manager->createMessenger(topic);
    const auto initailWaitUntil = std::chrono::system_clock::now() + 1s;
    bool firstMessageRecieved = false;
    while (true) {
        const auto messageMaybe = messenger->tryGetNexMessage();
        if (!messageMaybe.has_value()) {
            if (std::chrono::system_clock::now() > initailWaitUntil) {
                break;
            }
            else {
                std::this_thread::sleep_for(1ms);
                continue;
            }
        }
        firstMessageRecieved = true;
        file << nat::core::toString(*(messageMaybe.value()->encodeToBytes(nat::core::SerializationType::Csv))) << '\n';
    }
    file.close();
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
        dataMessenger->sendMessage(nat::core::NatImuDataSchema(rand(), nat::core::NatImuDataSchema::convertIntToSensorAccuracy(rand() % 4), imuData, 13));
    }
}

int main(int argc, char** argv) {
    //if (argc != 1) {
    //  std::cerr << "Usage: " << argv[0] << " takes no arguments\n";
    //  exit(1);
    //}
    std::mutex mtx;
    mtx.lock();
    mtx.unlock();

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

        case MenuOptions::StreamToFile:
            streamToFile(manager);
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

    return 0;
}
