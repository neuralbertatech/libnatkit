#define _CRT_SECURE_NO_WARNINGS
#include <libnatkit-mqtt.hpp>
#include <cerrno>
#include <cstring>


namespace nat::mosquitto {

MosquittoClient::MosquittoClient(const std::string &topic) : topic(topic) { }

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
  std::cout << "    [MQTT Client] Creating new client instance...\n";
  std::cout << "    [MQTT Client] Topic: " << topic << "\n";
  
  std::unique_ptr<MosquittoClient> mosquittoClient{
      new MosquittoClient(topic)};
  if (onMessageCallbackMaybe.has_value()) {
    std::cout << "    [MQTT Client] Setting custom message callback\n";
    mosquittoClient->setOnMessageCallback(onMessageCallbackMaybe.value());
  } else {
    std::cout << "    [MQTT Client] Using default message callback\n";
  }
  
  char *id{nullptr};
  bool cleanSession{true};
  void *callbackObj{convertCallbackObject(mosquittoClient.get())};

  std::cout << "    [MQTT Client] Creating mosquitto instance...\n";
  struct mosquitto *client = mosquitto_new(id, cleanSession, callbackObj);
  if (client == nullptr) {
    // Out of memory
    std::cout << "    [MQTT Client] ✗ FATAL: Out of memory!\n";
    return {};
  }
  std::cout << "    [MQTT Client] ✓ Mosquitto instance created\n";

  std::cout << "    [MQTT Client] Setting callbacks...\n";
  mosquitto_connect_callback_set(client, defaultOnConnect);
  mosquitto_subscribe_callback_set(client, defaultOnSubscribe);
  mosquitto_message_callback_set(client, onMessageHandler);
  std::cout << "    [MQTT Client] ✓ Callbacks set\n";

  int keepAlive = 60;
  std::cout << "    [MQTT Client] Attempting to connect to " << brokerHostname << ":" << brokerPort << "...\n";
  std::cout << "    [MQTT Client] Keep-alive: " << keepAlive << " seconds\n";
  std::cout << "    [MQTT Client] Mosquitto version: " << LIBMOSQUITTO_MAJOR << "." << LIBMOSQUITTO_MINOR << "." << LIBMOSQUITTO_REVISION << "\n";
  
  int returnCode = mosquitto_connect(client, brokerHostname.c_str(),
                                     brokerPort, keepAlive);
  
  if (returnCode != MOSQ_ERR_SUCCESS) {
    mosquitto_destroy(client);
    std::cout << "    [MQTT Client] ✗ CONNECTION FAILED!\n";
    std::cout << "    [MQTT Client] Error code: " << returnCode << " (expected " << MOSQ_ERR_SUCCESS << ")\n";
    std::cout << "    [MQTT Client] Error message: " << mosquitto_strerror(returnCode) << "\n";
    std::cout << "    [MQTT Client] Target: " << brokerHostname << ":" << brokerPort << "\n";
    std::cout << "    [MQTT Client] System errno: " << errno << " (" << strerror(errno) << ")\n";
    std::cout << "    [MQTT Client] \n";
    std::cout << "    [MQTT Client] Possible causes:\n";
    std::cout << "    [MQTT Client]   - Broker is not running\n";
    std::cout << "    [MQTT Client]   - Hostname cannot be resolved\n";
    std::cout << "    [MQTT Client]   - Port is blocked or incorrect\n";
    std::cout << "    [MQTT Client]   - Network connectivity issues\n";
    return {};
  }

  std::cout << "    [MQTT Client] ✓ Successfully connected to broker!\n";
  mosquittoClient->init(client);
  std::cout << "    [MQTT Client] Starting message handler thread...\n";
  mosquittoClient->start();
  std::cout << "    [MQTT Client] ✓ Client ready\n";
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

void MosquittoClient::addMessageToQueue(const std::string &topic, const std::string &msg) {
  messages.push(
      std::make_unique<std::pair<std::string, std::string>>(topic, msg));
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
  // Use payloadlen to properly handle binary data with null bytes
  const std::string msg{static_cast<char*>(message->payload), static_cast<size_t>(message->payloadlen)};
  //std::cout << "% Mosquitto Client " << topic << " receivied a message: " << msg << '\n';
  mosquittoClient->onMessageCallback(mosquittoClient, topic, msg,
                                     message->qos);
}

void MosquittoClient::defaultOnMessageCallback(MosquittoClient *client,
                                     const std::string &topic,
                                     const std::string &message, int qos) {
  /*std::cout << "% Mosquitto client recieved the following message: " << topic
            << " " << qos << " " << message << '\n';*/
  client->addMessageToQueue(topic, message);
}

}
