#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <stdlib.h>
#include <thread>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>
#include <libnatkit/util/Strings.hpp>
#include <libnatkit/util/Vectors.hpp>

#include <nlohmann/json.hpp>

#include <windows.h>

using namespace std::chrono_literals;

void get_window_size(int& lines, int& columns, int defaultLines = 40, int defaultColumns = 80) {
    CONSOLE_SCREEN_BUFFER_INFO csbi;
    if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi)) {
        lines = csbi.srWindow.Bottom - csbi.srWindow.Top + 1;
        columns = csbi.srWindow.Right - csbi.srWindow.Left + 1;
    }
    else {
        lines = defaultLines;
        columns = defaultColumns;
    }
}

void set_window_size(int lines, int columns)
{
    CONSOLE_SCREEN_BUFFER_INFO csbi;
    if (GetConsoleScreenBufferInfo(
        GetStdHandle(STD_OUTPUT_HANDLE),
        &csbi
    ))
    {
        // Make sure the new size isn't too big
        if (lines > csbi.dwSize.Y) lines = csbi.dwSize.Y;
        if (columns > csbi.dwSize.X) columns = csbi.dwSize.X;

        // Adjust window origin if necessary
        if ((csbi.srWindow.Top + lines) > csbi.dwSize.Y) csbi.srWindow.Top = csbi.dwSize.Y - lines - 1;
        if ((csbi.srWindow.Left + columns) > csbi.dwSize.Y) csbi.srWindow.Left = csbi.dwSize.X - columns - 1;

        // Calculate new size
        csbi.srWindow.Bottom = csbi.srWindow.Top + lines - 1;
        csbi.srWindow.Right = csbi.srWindow.Left + columns - 1;

        SetConsoleWindowInfo(
            GetStdHandle(STD_OUTPUT_HANDLE),
            true,
            &csbi.srWindow
        );
    }
}

class KeyListener {
    std::queue<char> keysPressed{};
    bool newKeysPressedAvailable = false;
    std::thread keyPollingThread;
    std::mutex queueLock;
    std::function<bool(char)> filter;
    std::function<char(char)> map;
    HWND windowHandle;
    std::mutex lock{};

    static char getShiftedKey(char keycode, bool isShiftDown) {
        if (!isShiftDown) {
            return nat::util::Strings::toLowercase(keycode);
        } else {
            if ((keycode >= 'a' && keycode <= 'z') || (keycode >= 'A' && keycode <= 'Z')) {
                return nat::util::Strings::toUppercase(keycode);
            }
            else {
                switch (keycode) {
                case '1': return '!';
                case '2': return '@';
                case '3': return '#';
                case '4': return '$';
                case '5': return '%';
                case '6': return '^';
                case '7': return '&';
                case '8': return '*';
                case '9': return '(';
                case '0': return ')';
                default: return keycode;
                }
            }
        }
    }

    static int asciiToVirtualKey(char c) {
        switch (c) {
        case 8:
            return VK_BACK;

        case 27:
            return VK_ESCAPE;

        case 32:
            return VK_SPACE;

        case 46:
            return VK_DECIMAL;

        default:
            return (int)c;
        }
    }

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
            {
                std::lock_guard<std::mutex> guard(lock);
                if (windowHandle == NULL || windowHandle == GetForegroundWindow()) {
                    for (int keyCode = 0; keyCode < 128; ++keyCode) {
                        // Check if the key with keyCode is currently
                        // pressed
                        currentKeyStates[keyCode] = GetAsyncKeyState(asciiToVirtualKey(keyCode));// & 0x8000;
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
                        bool shiftDown = GetAsyncKeyState(VK_SHIFT);
                        std::lock_guard<std::mutex> guard(queueLock);
                        for (int index = 0; index < keyUpIndex; ++index) {
                            if (filter(keyUp[index])) {
                                newKeysPressedAvailable = true;
                                keysPressed.emplace(map(getShiftedKey(keyUp[index], shiftDown)));
                            }
                            else {
                                char c = keyUp[index];
                                int i = keyUp[index];
                                ;
                            }
                        }
                    }
                }
            }

            // Add a small delay to avoid high CPU usage
            std::this_thread::sleep_for(10ms);
        }
    }


public:
    static bool defaultFilter(char c) { return true; }

    static char defaultMap(char c) { return c; }

    static bool isAlphaNumeric(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    }

    static bool isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    static bool isSpecialCharacter(char c) {
        return (c >= '!' && c <= '/') || (c >= ':' && c <= '@') || (c >= ']' && c <= '`') || (c >= '{' && c <= '~');
    }

    static bool isBackspace(char c) {
        return c == (char)8;
    }

    KeyListener() : keyPollingThread(&KeyListener::pollKeys, this), filter(defaultFilter), map(defaultMap) {
        wchar_t buffer[1024];
        wchar_t newTitleBuffer[1024];
        int size = GetConsoleTitle(buffer, 1024);
        if (size > 0) {
            wchar_t format[6] = { '%', 'd', '/', '%', 'd', 0 };
            wsprintf(newTitleBuffer, format, GetTickCount(), GetCurrentProcessId());
            SetConsoleTitle(newTitleBuffer);
            std::this_thread::sleep_for(40ms);
            windowHandle = FindWindow(NULL, newTitleBuffer);
            SetConsoleTitle(buffer);
        }
        else {
            windowHandle = NULL;
        }
    }

    void setFilter(std::function<bool(char)> func) {
        std::lock_guard<std::mutex> guard(lock);
        filter = func;
    }

    void resetFilter() {
        std::lock_guard<std::mutex> guard(lock);
        filter = defaultFilter;
    }

    std::function<bool(char)> getFilter() const { return filter; }

    void setMap(std::function<char(char)> func) {
        std::lock_guard<std::mutex> guard(lock);
        map = func;
    }

    void resetMap() {
        map = defaultMap;
    }

    std::function<char(char)> getMap() const { return map; }

    std::vector<char> getKeysPressed() {
        std::vector<char> keys;
        std::lock_guard<std::mutex> guard(lock);
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

    bool haveAnyKeysBeenPressed() const { return newKeysPressedAvailable; }

    void clearBuffer() {
        std::lock_guard<std::mutex> guard(lock);
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

std::shared_ptr<KeyListener> globalKeyListener = std::make_shared<KeyListener>();

class TerminalScreen {
    static const std::string clearBufferCommand;
    int terminalWidth{};
    int terminalHeight{};
    int defaultWidth{};
    int defaultHeight{};
    std::mutex lock{};

public:
    TerminalScreen(int defaultWidth = 80, int defaultHeight = 60) : defaultWidth(defaultWidth), defaultHeight(defaultHeight) {
        get_window_size(terminalHeight, terminalWidth, defaultHeight, defaultWidth);
    }

    static void clearWindow() {
        std::cout << clearBufferCommand;
    }

    static void drawToScreen(const std::string& msg) {
        clearWindow();
        std::cout << msg;
    }

    void update() {
        std::lock_guard<std::mutex> gaurd(lock);
        get_window_size(terminalHeight, terminalWidth, defaultHeight, defaultWidth);
    }

    int getWidth() const { return terminalWidth; }

    int getHeight() const { return terminalHeight; }
};

const std::string TerminalScreen::clearBufferCommand = (char)27 + "[2J";

struct Command {
    const std::string command;
    const std::string discription;

    Command(const std::string& command, const std::string& discription)
        : command(command), discription(discription) {}

    Command(std::pair<std::string, std::string> pair)
        : command(pair.first), discription(pair.second) {}

    Command(const Command& other)
        : command(other.command), discription(other.discription) {}

    Command operator=(const Command& other) {
        return Command(other);
    }
};

class ImuTuiWindow {
    TerminalScreen screen;
    std::vector<std::string> sidePannelContent;
    std::vector<Command> commands;
    std::deque<std::string> outputPallet;
    std::mutex lock{};

    int getSidePannelWidth() {
        int width = screen.getWidth();
        int sidePannelWidth = 80;
        if (width < 160) {
            sidePannelWidth = 30;
        }
        return sidePannelWidth;
    }

public:
    ImuTuiWindow() = default;

    void setSidePannelContent(const std::vector<std::string>& sidePannel) {
        std::lock_guard<std::mutex> guard(lock);
        this->sidePannelContent = std::vector<std::string>{};
        this->sidePannelContent.reserve(sidePannel.size());
        int sidePannelWidth = getSidePannelWidth();
        for (const auto& str : sidePannel) {
            std::string tmpStr{ str };
            int strLen = str.length();
            int index = 0;
            while (strLen >= sidePannelWidth) {
                this->sidePannelContent.push_back(str.substr(index, sidePannelWidth - 1));
                strLen -= (sidePannelWidth - 1);
                index += (sidePannelWidth - 1);
            }
            this->sidePannelContent.push_back(str.substr(index));
        }
    }

    void setCommands(const std::vector<Command>& c) {
        std::lock_guard<std::mutex> guard(lock);
        this->commands = std::vector<Command>{ c };
    }

    void addOutput(const std::string& output) {
        std::lock_guard<std::mutex> guard(lock);
        for (const auto& line : nat::util::Strings::split(output, '\n'))
            outputPallet.emplace_front(line);
        while (outputPallet.size() > 2 * screen.getHeight())
            outputPallet.pop_back();
    }

    void addNewLineToOutput() {
        std::lock_guard<std::mutex> guard(lock);
        outputPallet.emplace_front("");
    }

    void draw() {
        std::lock_guard<std::mutex> guard(lock);
        screen.update();
        int width = screen.getWidth();
        int height = screen.getHeight();
        int sidePannelWidth = getSidePannelWidth();
        int currentHeight = 0;
        std::string emptyLine = "";
        for (int i = 0; i < width; ++i) {
            if (i == width - sidePannelWidth) {
                emptyLine.append("|");
            }
            else {
                emptyLine.append(" ");
            }
        }
        emptyLine.append("\n");

        std::string commandsPallet = "";
        for (int i = 0; i < width; ++i) {
            if (i < width - sidePannelWidth) {
                commandsPallet.append("-");
            }
            else if (i == width - sidePannelWidth) {
                commandsPallet.append("|");
            }
            else {
                commandsPallet.append(" ");
            }
        }
        commandsPallet.append("\n");
        ++currentHeight;
        for (const auto& command : commands) {
            std::string padding = " ";
            std::string commandString = command.command + ":" + padding + command.discription;
            for (int i = commandString.length(); i < width; ++i) {
                if (i == width - sidePannelWidth) {
                    commandString.append("|");
                }
                else if (currentHeight <= sidePannelContent.size() && i > width - sidePannelWidth) {
                    commandString.append(sidePannelContent[currentHeight - 1]);
                    break;
                }
                else {
                    commandString.append(" ");
                }
            }
            commandsPallet.append(commandString + "\n");
            ++currentHeight;
        }

        std::string outputPalletString = "";
        int spaceAvailable = height - currentHeight;
        int numberOfOutputLines = spaceAvailable < outputPallet.size() ? spaceAvailable : outputPallet.size();
        for (int i = numberOfOutputLines; i > 0; --i, ++currentHeight) {
            std::string endOfLine = "";
            for (int j = outputPallet[i - 1].length(); j < width; ++j) {
                if (j == width - sidePannelWidth) {
                    endOfLine.append("|");
                }
                else if (currentHeight <= sidePannelContent.size() && i > width - sidePannelWidth) {
                    endOfLine.append(sidePannelContent[currentHeight - 1]);
                    break;
                }
                else {
                    endOfLine.append(" ");
                }
            }
            outputPalletString += outputPallet[i - 1] + endOfLine + "\n";
        }

        std::string topPadding = "";
        for (int i = currentHeight; i < height; ++i)
            topPadding.append(emptyLine);

        screen.drawToScreen(topPadding + outputPalletString + commandsPallet);
    }

    std::string interactiveInput(const std::string& msg, KeyListener& keyListener, char terminationCharacter = '\n') {
        const auto existingFilter = keyListener.getFilter();
        const auto existingMap = keyListener.getMap();
        keyListener.setMap([](char c) {
            if (c == '\r')
                return '\n';
            return c;
            });
        keyListener.setFilter([](char c) {
            return KeyListener::isAlphaNumeric(c) || KeyListener::isWhitespace(c) || KeyListener::isSpecialCharacter(c) || KeyListener::isBackspace(c);
            });
        keyListener.clearBuffer();
        const auto existingCommands = commands;
        std::string currentInput = "";
        bool continueReadingInput = true;
        bool refreashWindow = true;
        while (continueReadingInput) {
            setCommands({ Command{msg, currentInput} });
            const auto keysPressed = keyListener.getKeysPressed();
            if (refreashWindow) {
                draw();
                refreashWindow = false;
            }
            for (char key : keysPressed) {
                if (key == terminationCharacter) {
                    continueReadingInput = false;
                    break;
                }
                else if (KeyListener::isBackspace(key) && currentInput.size() > 0) {
                    currentInput.substr(0, currentInput.size() - 1);
                }
                else {
                    refreashWindow = true;
                    currentInput += key;
                }
            }
        }
        setCommands(existingCommands);
        keyListener.setMap(existingMap);
        keyListener.setFilter(existingFilter);
        return currentInput;
    }
};

struct ImuConfig {
    std::string id;
    std::string name;
};

class Config {
    std::vector<std::string> ids{};
    std::map<std::string, std::string> idNames{};

    void populateDataFromJsonFile() {
        std::ifstream jsonFile("config.json");
        nlohmann::json json = nlohmann::json::parse(jsonFile);
        for (auto& [index, listEntry] : json["imu"].items()) {
            for (auto& [imuId, imuFields] : listEntry.items()) {
                ids.push_back(imuId);
                for (auto& [imuFieldName, imuFieldValue] : imuFields.items()) {
                    if (imuFieldName == "name") {
                        idNames.insert({ imuId, imuFieldValue.get<std::string>() });
                    }
                }
            }
        }
    }

public:
    Config() {
        populateDataFromJsonFile();
    }

    std::optional<std::string> tryGetName(std::string id) {
        if (auto it = idNames.find(id); it != idNames.end()) {
            return it->second;
        }
        else {
            return {};
        }
    }
};

std::vector<std::unique_ptr<nat::core::RawStream>> promptUserToChooseStreams(const std::unique_ptr<nat::kafka::BrokerManager>& manager, std::shared_ptr<ImuTuiWindow> window, std::shared_ptr<KeyListener> keyListener) {
    while (true) {
        auto streams = manager->getAllStreams();
        for (int i = 0; i < streams.size(); ++i) {
            window->addOutput("(" + std::to_string(i) + ") " + streams[i]->toPrettyString());
        }
        int maxIndex = (std::max)(static_cast<int>(streams.size() - 1), 0);
        std::string selectedStreamIndex = window->interactiveInput("Select one or multiple streams seperated by ',' [0-" + std::to_string(maxIndex) + "] or 'r' to refreash: ", *keyListener);
        if (selectedStreamIndex == "r")
            continue;

        std::vector<int> indexes = {};
        for (const auto str : nat::util::Strings::split(selectedStreamIndex, ',')) {
            int index = std::stoi(str);
            indexes.emplace_back(index);
            if (index < 0 || index > streams.size()) {
                window->addOutput(std::to_string(index) + " is not in the range [0-" + std::to_string(maxIndex) + "]");
            }
        }
        if (indexes.size() == 0) {
            window->addOutput("No valid indexes selected, try again");
            continue;
        }

        std::vector<std::unique_ptr<nat::core::RawStream>> selectedStreams{};
        for (auto index : indexes) {
            selectedStreams.emplace_back(std::move(streams[index]));
        }
        return selectedStreams;
    }
}

class ImuApplication {
    enum class ImuApplicationStage {
        SelectImus = 1,
        Calibration,
    };

    Config config;
    std::unique_ptr<nat::kafka::BrokerManager> manager;
    std::shared_ptr<ImuTuiWindow> window;
    std::shared_ptr<KeyListener> keyListener;
    ImuApplicationStage currentStage;
    std::optional<std::vector<std::unique_ptr<nat::core::TopicMessenger>>> imuStreams;

    static std::vector<std::unique_ptr<nat::core::TopicMessenger>> getImuStreams(const std::unique_ptr<nat::kafka::BrokerManager>& manager, std::shared_ptr<ImuTuiWindow> window, std::shared_ptr<KeyListener> keyListener) {
        const std::vector<std::unique_ptr<nat::core::RawStream>> streams = promptUserToChooseStreams(manager, window, keyListener);
        std::vector<std::unique_ptr<nat::core::TopicMessenger>> messengers{};
        for (const auto& stream : streams) {
            auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
            if (dataTopics.size() > 0) {
                std::shared_ptr<nat::core::BasicTopicInformation> dataTopic = std::move(stream->getTopicsByType(nat::core::StreamType::DATA)[0]);
                messengers.emplace_back(manager->createMessenger(dataTopic));
            }
        }

        return messengers;
    }

    std::string getNameForImu(std::string id) {
        std::optional<std::string> nameMaybe = config.tryGetName(id);
        if (nameMaybe.has_value())
            return nameMaybe.value();
        else
            return "?";
    }

public:
    ImuApplication(std::unique_ptr<nat::kafka::BrokerManager> manager, std::shared_ptr<ImuTuiWindow> window, std::shared_ptr<KeyListener> keyListener, Config config)
        : manager(std::move(manager)), window(window), keyListener(keyListener), currentStage(ImuApplicationStage::SelectImus), config(config) {}

    void start() {
        while (true) {
            window->draw();
            switch (currentStage) {
            case ImuApplicationStage::SelectImus:
                if (!imuStreams.has_value()) {
                    imuStreams = getImuStreams(manager, window, keyListener);
                }
                else {
                    currentStage = ImuApplicationStage::Calibration;
                }
                break;

            case ImuApplicationStage::Calibration:
                window->setCommands({ {"n", "Next"} });
                std::vector<std::string> imuAccuracies{};
                auto pollStreamFunc = [this](std::shared_ptr<nat::core::TopicMessenger> s) -> std::vector<std::string> {
                    std::vector<std::string> accuracies{};
                    s->clearAllMessages();
                    std::shared_ptr<nat::core::Schema> message{ nullptr };
                    const auto initailWaitUntil = std::chrono::system_clock::now() + 1s;
                    while (std::chrono::system_clock::now() < initailWaitUntil) {
                        auto messageMaybe = s->tryGetNexMessage();
                        if (messageMaybe.has_value()) {
                            message = std::move(messageMaybe.value());
                            break;
                        }
                    }
                    if (message == nullptr) {
                        window->addOutput("No messages recieved from " + std::to_string(s->getId()) + ". Is it connected?");
                    }
                    else {
                        std::optional<std::shared_ptr<nat::core::NatImuDataSchema>> convertedMessageMaybe = nat::core::NatImuDataSchema::tryCreateFromSchema(message);
                        if (!convertedMessageMaybe.has_value()) {
                            window->addOutput(std::to_string(s->getId()) + ": Was expecting a NatImuDataSchema object, but found a " + message->getName() + " object instead. Error");
                        }
                        else {
                            std::string id = std::to_string(s->getId());
                            std::string name = getNameForImu(id);
                            accuracies.push_back(name + ":");
                            accuracies.push_back("    Id = " + id);
                            accuracies.push_back("    Accuracy = " + std::to_string(nat::core::NatImuDataSchema::convertSensorAccuracyToInt(convertedMessageMaybe.value()->getAccuracy())));
                            accuracies.push_back("");
                        }
                    }
                    return accuracies;
                    };
                std::vector<std::thread> threads{};
                for (auto& stream : imuStreams.value()) {
                    std::shared_ptr<nat::core::TopicMessenger> sharedStream = std::move(stream);
                    threads.emplace_back(pollStreamFunc, sharedStream);                    
                }
                

                window->setSidePannelContent(imuAccuracies);
                break;
            }
        }
    }
};

enum class MenuOptions {
    DeleteTopic,
    DeleteAllTopics,
    StreamToFile,
    ImuExperiment,
    ListStreams,
    ListTopics,
    Quit,
    ReadTopic,
    SimulateNatImu
};

static std::vector<Command> MenuOptionCommands = {
    {"d",  "Delete Topic"},
    {"da", "Delete All Topics"},
    {"f",  "Stream to File"},
    {"i",  "Run IMU Experiment"},
    {"ls", "List Streams"},
    {"lt", "List Topics"},
    {"q",  "Quit"},
    {"r",  "Read Topic"},
    {"s",  "Simulate natIMU"}
};

MenuOptions getMenuOption(ImuTuiWindow& window) {
    std::string command = "";
    globalKeyListener->setFilter([](char c) {
        char lowerC = nat::util::Strings::toLowercase(c);
        return (lowerC <= 'z' && lowerC >= 'a') || (c >= '0' && c <= '9') || c == (char)13 || c == '\n'; }
    );
    globalKeyListener->setMap([](char c) {
        if (c == (char)13)
            return '\n';
        else
            return c;
        });
    globalKeyListener->clearBuffer();
    while (true) {
        std::vector<Command> menuOptions{};
        for (const auto& option : MenuOptionCommands) {
            bool showCommand = true;
            for (int i = 0; i < command.size() && i < option.command.size(); ++i)
                if (command[i] != option.command[i]) {
                    showCommand = false;
                    break;
                }
            if (showCommand)
                menuOptions.emplace_back(option);
        }
        /*globalKeyListener.clearBuffer();
        menuOptions.append("Enter an option: ");
        TerminalScreen::drawToScreen(menuOptions);*/
        window.setCommands(menuOptions);
        window.draw();
        while (!globalKeyListener->haveAnyKeysBeenPressed()) std::this_thread::sleep_for(10ms);
        const auto keysPressed = globalKeyListener->getKeysPressed();
        std::vector<int> keyCodesPressed = {};
        for (auto character : keysPressed) keyCodesPressed.push_back((int)character);
        //menuOptions.append("\n Keys Pressed: " + nat::util::Vectors::toString(keyCodesPressed));
        //TerminalScreen::drawToScreen(menuOptions);
                
        for (int i = 0; i < keysPressed.size(); ++i) {
            char characterPressed = nat::util::Strings::toLowercase(keysPressed[i]);
            //if (characterPressed == 13)
            //    characterPressed = '\n';
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
            else if (command == "i") {
                return MenuOptions::ImuExperiment;
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

std::unique_ptr<nat::core::BasicTopicInformation> promptUserToChooseTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager, bool chooseMultiple = false) {
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

void deleteTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
    const auto topic = promptUserToChooseTopic(manager);
    manager->deleteTopic(topic->toTopicString());
}

void deleteAllTopics(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
    const auto topics = manager->getAllTopicStrings();
    for (const auto& topic : topics)
        manager->deleteTopic(topic);
}

void listCurrentStreams(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
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
    window.addOutput(std::to_string(streamTopics.size()));

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
            window.addOutput(std::to_string(stream->getId()) + " (" + name + ")");
        }
    }
}

void listCurrentTopics(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
    const auto topics = manager->getAllTopicStrings();
    if (topics.size() == 0) {
        window.addOutput("There are currently no topics available");
    }
    else {
        window.addOutput("There are currently " + std::to_string(topics.size()) + " topics:");
        for (const auto& topic : topics)
            window.addOutput("    " + topic);
    }
}

void readTopic(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
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
        window.addOutput(messageString);
    }
}

void streamToFile(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
    /*window.addOutput("Enter a name for the file: ");
    std::string filename;
    std::getline(std::cin, filename);*/
    std::string filename = window.interactiveInput("Enter a name for the file", *globalKeyListener);
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

void simulateNatImu(const std::unique_ptr<nat::kafka::BrokerManager>& manager, ImuTuiWindow& window) {
    window.addOutput("Enter a name for the simulated IMU device: ");
    std::string name;
    std::getline(std::cin, name);
    std::vector<std::string> encodingTypes = { "Json" };
    window.addOutput("Enter an encoding type, one of " + nat::util::Vectors::toString(encodingTypes) + ": ");
    std::string serializationTypeString;
    std::getline(std::cin, serializationTypeString);
    window.addOutput("Enter an id: ");
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
    std::shared_ptr<ImuTuiWindow> window = std::make_shared<ImuTuiWindow>();
    window->setCommands(MenuOptionCommands);

    //const auto topicInfoMaybe = nat::core::BasicTopicInformation::create(topic);
    //if (!topicInfoMaybe.has_value()) {
    //  std::cerr
    //      << "<topic> takes the form of <stream-type>-<id>-<encoder>-<schema>\n";
    //  exit(1);
    //}
    //const auto topicInfo = std::make_shared<nat::core::BasicTopicInformation>(
    //    *topicInfoMaybe.value());

    auto manager = nat::kafka::createBrokerManager(brokerHost, brokerPort);
    bool continueRunning = true;
    while (continueRunning) {
        switch (getMenuOption(*window)) {
        case MenuOptions::DeleteTopic:
            deleteTopic(manager, *window);
            break;

        case MenuOptions::DeleteAllTopics:
            deleteAllTopics(manager, *window);
            break;

        case MenuOptions::StreamToFile:
            streamToFile(manager, *window);
            break;

        case MenuOptions::ImuExperiment:
        {
            ImuApplication application(std::move(manager), window, globalKeyListener, Config{});
            application.start();
        }
            break;

        case MenuOptions::ListStreams:
            listCurrentStreams(manager, *window);
            break;

        case MenuOptions::ListTopics:
            listCurrentTopics(manager, *window);
            break;

        case MenuOptions::Quit:
            continueRunning = false;
            break;

        case MenuOptions::ReadTopic:
            readTopic(manager, *window);
            break;

        case MenuOptions::SimulateNatImu:
            simulateNatImu(manager, *window);
            break;
        }
    }

    return 0;
}
