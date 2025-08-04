#include <libnatkit-mqtt.hpp>
#include <libnatkit-core.hpp>

namespace nat::mosquitto {

MosquittoClient::MosquittoClient(const std::string &topic) : topic(topic) {}

void MosquittoClient::setOnMessageCallback(onMessageCallback_t callback) { onMessageCallback = callback; }

void MosquittoClient::init(struct mosquitto *client) { this->client = client; }

MosquittoClient::~MosquittoClient() {
  running = false;
  mosquitto_destroy(client);
}

std::optional<std::unique_ptr<MosquittoClient>>
MosquittoClient::create(const std::string &brokerHostname, int brokerPort,
       const std::string &topic,
       const std::optional<onMessageCallback_t>& onMessageCallbackMaybe) {
  std::unique_ptr<MosquittoClient> mosquittoClient{ new MosquittoClient(topic) };
  if (onMessageCallbackMaybe.has_value()) {
    mosquittoClient->setOnMessageCallback(onMessageCallbackMaybe.value());
  }
  char *id{nullptr};
  bool cleanSession{true};
  void *callbackObj{convertCallbackObject(mosquittoClient.get())};

  struct mosquitto *client = mosquitto_new(id, cleanSession, callbackObj);
  if (client == nullptr) {
    // Out of memory
    std::cout << "% Mosquitto client error: out of memory" << '\n';
    return {};
  }

  mosquitto_connect_callback_set(client, defaultOnConnect);
  mosquitto_subscribe_callback_set(client, defaultOnSubscribe);
  mosquitto_message_callback_set(client, onMessageHandler);

  int keepAlive = 60;
  int returnCode = mosquitto_connect(client, brokerHostname.c_str(),
                                     brokerPort, keepAlive);
  if (returnCode != MOSQ_ERR_SUCCESS) {
    mosquitto_destroy(client);
    std::cout << "% Mosquitto client error: "
              << mosquitto_strerror(returnCode) << '\n';
    return {};
  }

  mosquittoClient->init(client);
  mosquittoClient->start();
  return mosquittoClient;
}

void MosquittoClient::start() {
  running = true;
  thread = std::jthread(&MosquittoClient::handleMessages, this);
}

void MosquittoClient::stop() { running = false; }

void MosquittoClient::handleMessages() {
  while (running && client)
    mosquitto_loop(client, -1,
                   1); // Loop until mosquitto_disconnect is called
}

void MosquittoClient::addMessageToQueue(const std::string &topic, const core::message_t &msg) {
  messages.push(
      std::make_unique<std::pair<std::string, core::message_t>>(topic, msg));
}

MosquittoClient *MosquittoClient::convertCallbackObject(void *callbackObj) {
  return static_cast<MosquittoClient *>(callbackObj);
}

void *MosquittoClient::convertCallbackObject(MosquittoClient *client) {
  return static_cast<void *>(client);
}

void MosquittoClient::defaultOnConnect(struct mosquitto *clientPtr, void *callbackObj,
                             int reasonCode) {
  auto mosquittoClient = convertCallbackObject(callbackObj);
  std::cout << "% Mosquitto client connected: "
            << mosquitto_connack_string(reasonCode);
  if (reasonCode != 0) {
    std::cout << "% Client connection failed\n";
    mosquittoClient->stop();
    mosquitto_disconnect(clientPtr); // Stop trying to reconnect
    return;
  }

  int *messageId{nullptr};
  int qualityOfService{1};
  int returnCode = mosquitto_subscribe(
      clientPtr, messageId, mosquittoClient->topic.c_str(), qualityOfService);
  if (returnCode != MOSQ_ERR_SUCCESS) {
    std::cout << "% Error: While subscribing to " << mosquittoClient->topic
              << "the following error occured: "
              << mosquitto_strerror(returnCode) << '\n';
    return;
  }
}

void MosquittoClient::defaultOnSubscribe(struct mosquitto *clientPtr, void *callbackObj,
                               int messageId, int qualitOfServiceCount,
                               const int *grantedQualityOfService) {
  auto mosquittoClient = convertCallbackObject(callbackObj);
  bool haveSubscription{false};
  for (int i = 0; i < qualitOfServiceCount; ++i) {
    std::cout << "% Mosquitto client subscriber " << i
              << " has a granted pos of " << grantedQualityOfService[i]
              << '\n';
    if (grantedQualityOfService[i] <= 2) {
      haveSubscription = true;
    }
  }

  if (!haveSubscription) {
    std::cout
        << "% Mosquitto client error: All subscriptions were rejected\n";
    mosquittoClient->stop();
    mosquitto_disconnect(clientPtr);
  }
}

void MosquittoClient::onMessageHandler(struct mosquitto *clientPtr, void *callbackObj,
                             const struct mosquitto_message *message) {
  const auto mosquittoClient = convertCallbackObject(callbackObj);
  const std::string topic{message->topic};
  const size_t messageLength = message->payloadlen;
  const uint8_t* payload = (uint8_t*)message->payload;
  core::message_t msg{};
  msg.reserve(messageLength);
  for (size_t i = 0; i < messageLength; ++i)
      msg.push_back(payload[i]);
  std::cout << "% Mosquitto Client " << topic << " receivied a message: [ ";
  for (const auto byte : msg)
      printf("%.2X, ", byte);
  std::cout << "]\n";
  mosquittoClient->onMessageCallback(mosquittoClient, topic, msg,
                                     message->qos);
}

void MosquittoClient::defaultOnMessageCallback(MosquittoClient *client,
                                     const std::string &topic,
                                     const core::message_t &message, int qos) {
  /*std::cout << "% Mosquitto client recieved the following message: " << topic
            << " " << qos << " " << message << '\n';*/
  client->addMessageToQueue(topic, message);
}

}
