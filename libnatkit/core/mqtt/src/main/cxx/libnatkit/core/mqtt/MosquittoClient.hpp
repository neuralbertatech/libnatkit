//#pragma once
//
//#include <functional>
//#include <iostream>
//#include <memory>
//#include <mutex>
//#include <optional>
//#include <queue>
//#include <thread>
//
//#include <mosquitto.h>
//
//namespace nat::mosquitto {
//
//class MosquittoClient {
//public:
//  using onMessageCallback_t =
//      std::function<void(MosquittoClient *, const std::string &topic,
//                         const std::string &message, int qos)>;
//
//private:
//  struct mosquitto *client;
//  const std::string topic;
//  std::queue<std::unique_ptr<std::pair<std::string, std::string>>> messages;
//  std::jthread thread;
//  bool running{false};
//  onMessageCallback_t onMessageCallback{MosquittoClient::defaultOnMessageCallback};
//
//  MosquittoClient(const std::string &topic) : topic(topic) {}
//
//  void setOnMessageCallback(onMessageCallback_t callback) { onMessageCallback = callback; }
//
//  void init(struct mosquitto *client) { this->client = client; }
//
//public:
//  ~MosquittoClient() {
//    running = false;
//    mosquitto_destroy(client);
//  }
//
//  static std::optional<std::unique_ptr<MosquittoClient>>
//  create(const std::string &brokerHostname, int brokerPort,
//         const std::string &topic,
//         const std::optional<onMessageCallback_t>& onMessageCallbackMaybe = {}) {
//    std::unique_ptr<MosquittoClient> mosquittoClient{
//        new MosquittoClient(topic)};
//    if (onMessageCallbackMaybe.has_value()) {
//      mosquittoClient->setOnMessageCallback(onMessageCallbackMaybe.value());
//    }
//    char *id{nullptr};
//    bool cleanSession{true};
//    void *callbackObj{convertCallbackObject(mosquittoClient.get())};
//
//    struct mosquitto *client = mosquitto_new(id, cleanSession, callbackObj);
//    if (client == nullptr) {
//      // Out of memory
//      std::cout << "% Mosquitto client error: out of memory" << '\n';
//      return {};
//    }
//
//    mosquitto_connect_callback_set(client, defaultOnConnect);
//    mosquitto_subscribe_callback_set(client, defaultOnSubscribe);
//    mosquitto_message_callback_set(client, onMessageHandler);
//
//    int keepAlive = 60;
//    int returnCode = mosquitto_connect(client, brokerHostname.c_str(),
//                                       brokerPort, keepAlive);
//    if (returnCode != MOSQ_ERR_SUCCESS) {
//      mosquitto_destroy(client);
//      std::cout << "% Mosquitto client error: "
//                << mosquitto_strerror(returnCode) << '\n';
//      return {};
//    }
//
//    mosquittoClient->init(client);
//    mosquittoClient->start();
//    return mosquittoClient;
//  }
//
//  void start() {
//    running = true;
//    thread = std::jthread(&MosquittoClient::handleMessages, this);
//  }
//
//  void stop() { running = false; }
//
//private:
//  void handleMessages() {
//    while (running && client)
//      mosquitto_loop(client, -1,
//                     1); // Loop until mosquitto_disconnect is called
//  }
//
//  void addMessageToQueue(const std::string &topic, const std::string &msg) {
//    messages.push(
//        std::make_unique<std::pair<std::string, std::string>>(topic, msg));
//  }
//
//  static MosquittoClient *convertCallbackObject(void *callbackObj) {
//    return static_cast<MosquittoClient *>(callbackObj);
//  }
//
//  static void *convertCallbackObject(MosquittoClient *client) {
//    return static_cast<void *>(client);
//  }
//
//  static void defaultOnConnect(struct mosquitto *clientPtr, void *callbackObj,
//                               int reasonCode) {
//    auto mosquittoClient = convertCallbackObject(callbackObj);
//    std::cout << "% Mosquitto client connected: "
//              << mosquitto_connack_string(reasonCode);
//    if (reasonCode != 0) {
//      std::cout << "% Client connection failed\n";
//      mosquittoClient->stop();
//      mosquitto_disconnect(clientPtr); // Stop trying to reconnect
//      return;
//    }
//
//    int *messageId{nullptr};
//    int qualityOfService{1};
//    int returnCode = mosquitto_subscribe(
//        clientPtr, messageId, mosquittoClient->topic.c_str(), qualityOfService);
//    if (returnCode != MOSQ_ERR_SUCCESS) {
//      std::cout << "% Error: While subscribing to " << mosquittoClient->topic
//                << "the following error occured: "
//                << mosquitto_strerror(returnCode) << '\n';
//      return;
//    }
//  }
//
//  static void defaultOnSubscribe(struct mosquitto *clientPtr, void *callbackObj,
//                                 int messageId, int qualitOfServiceCount,
//                                 const int *grantedQualityOfService) {
//    auto mosquittoClient = convertCallbackObject(callbackObj);
//    bool haveSubscription{false};
//    for (int i = 0; i < qualitOfServiceCount; ++i) {
//      std::cout << "% Mosquitto client subscriber " << i
//                << " has a granted pos of " << grantedQualityOfService[i]
//                << '\n';
//      if (grantedQualityOfService[i] <= 2) {
//        haveSubscription = true;
//      }
//    }
//
//    if (!haveSubscription) {
//      std::cout
//          << "% Mosquitto client error: All subscriptions were rejected\n";
//      mosquittoClient->stop();
//      mosquitto_disconnect(clientPtr);
//    }
//  }
//
//  static void onMessageHandler(struct mosquitto *clientPtr, void *callbackObj,
//                               const struct mosquitto_message *message) {
//    const auto mosquittoClient = convertCallbackObject(callbackObj);
//    const std::string topic{message->topic};
//    const std::string msg{(char *)message->payload};
//    std::cout << "% Mosquitto Client " << topic << " receivied a message: " << msg << '\n';
//    mosquittoClient->onMessageCallback(mosquittoClient, topic, msg,
//                                       message->qos);
//  }
//
//  static void defaultOnMessageCallback(MosquittoClient *client,
//                                       const std::string &topic,
//                                       const std::string &message, int qos) {
//    std::cout << "% Mosquitto client recieved the following message: " << topic
//              << " " << qos << " " << message << '\n';
//    client->addMessageToQueue(topic, message);
//  }
//};
//
//} // namespace nat::mosquitto
