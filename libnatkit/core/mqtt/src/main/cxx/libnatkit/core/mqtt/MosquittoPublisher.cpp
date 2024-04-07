#include <libnatkit-mqtt.hpp>


using namespace std::chrono_literals;

namespace nat::mosquitto {

void MosquittoPublisher::init(struct mosquitto *producer, const std::string &topic) {
  this->producer = producer;
  this->topic = topic;
  start();
}

void MosquittoPublisher::start() {
  running = true;
  thread = std::jthread(&MosquittoPublisher::handleMessages, this);
}

MosquittoPublisher::~MosquittoPublisher() {
  running = false;
  mosquitto_destroy(producer);
}

std::optional<std::unique_ptr<MosquittoPublisher>>
MosquittoPublisher::create(const std::string &brokerHostname, int brokerPort,
       const std::string &topic) {
  std::unique_ptr<MosquittoPublisher> mosquittoPublisher{
      new MosquittoPublisher()};
  char *id{nullptr};
  bool cleanSession{true};
  void *callbackObj{convertCallbackObject(mosquittoPublisher.get())};

  struct mosquitto *publisher = mosquitto_new(id, cleanSession, callbackObj);
  if (publisher == nullptr) {
    // Out of memory
    std::cout << "% Mosquitto publisher error: out of memory" << '\n';
    return {};
  }

  mosquitto_connect_callback_set(publisher, defaultOnConnect);
  mosquitto_publish_callback_set(publisher, defaultOnPublish);

  int keepAlive = 60;
  int returnCode = mosquitto_connect(publisher, brokerHostname.c_str(),
                                     brokerPort, keepAlive);
  if (returnCode != MOSQ_ERR_SUCCESS) {
    mosquitto_destroy(publisher);
    std::cout << "% Mosquitto publisher error: "
              << mosquitto_strerror(returnCode) << '\n';
    return {};
  }

  std::cout << "% Mosquitto publisher with topic of \"" << topic << "\" was succesfully created\n";
  mosquittoPublisher->init(publisher, topic);
  return mosquittoPublisher;
}

void MosquittoPublisher::stop() { running = false; }

void MosquittoPublisher::sendMessage(const std::string &msg) { messages.emplace(std::make_unique<std::string>(msg)); }

void MosquittoPublisher::handleMessages() {
  while (running && producer) {
    if (!messages.empty())
      publishMessages();
    mosquitto_loop(producer, -1, 1);
    std::this_thread::sleep_for(10ms);
  }
  std::cout << "% Mosquitto publisher stopping\n";
}

void MosquittoPublisher::publishMessages() {
  int *messageId{nullptr};
  const int qualityOfService{2};
  const bool retain{false};
  while (!messages.empty()) {
    const auto message = std::move(messages.front());
    messages.pop();
    int returnCode =
        mosquitto_publish(producer, messageId, topic.c_str(), message->size(),
                          message->c_str(), qualityOfService, retain);

    if (returnCode != MOSQ_ERR_SUCCESS) {
      std::cout << "% Mosquitto publisher error: Could not publish \""
                << *message << "\" because " << mosquitto_strerror(returnCode)
                << '\n';
    } else {
      std::cout << "% Mosquitto publisher successfully sent \"" << *message << "\"\n";
    }
  }
}

MosquittoPublisher *MosquittoPublisher::convertCallbackObject(void *callbackObj) {
  return static_cast<MosquittoPublisher *>(callbackObj);
}

void *MosquittoPublisher::convertCallbackObject(MosquittoPublisher *client) {
  return static_cast<void *>(client);
}

void MosquittoPublisher::defaultOnConnect(struct mosquitto *clientPtr, void *callbackObj,
                             int reasonCode) {
  auto mosquittoPublisher = convertCallbackObject(callbackObj);
  std::cout << "% Mosquitto publisher connected: "
            << mosquitto_connack_string(reasonCode);
  if (reasonCode != 0) {
    std::cout << "% Client connection failed\n";
    mosquittoPublisher->stop();
    mosquitto_disconnect(clientPtr); // Stop trying to reconnect
  }
}

void MosquittoPublisher::defaultOnPublish(struct mosquitto *clientPtr, void *callbackObj,
                             int messageId) {
  std::cout << "Message with mid " << messageId << " has been published.\n";
}

}
