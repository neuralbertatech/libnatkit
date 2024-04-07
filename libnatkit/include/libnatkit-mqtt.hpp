#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <queue>
#include <thread>

#include <mosquitto.h>


namespace nat::mosquitto {

class MosquittoPublisher {
  struct mosquitto *producer;
  std::string topic;
  std::queue<std::unique_ptr<std::string>> messages;
  std::jthread thread;
  bool running{false};

  MosquittoPublisher() = default;

  void init(struct mosquitto *producer, const std::string &topic);
  void start();

public:
  ~MosquittoPublisher();

  static std::optional<std::unique_ptr<MosquittoPublisher>>
  create(const std::string &brokerHostname, int brokerPort,
         const std::string &topic);
  void stop();
  void sendMessage(const std::string &msg);

private:
  void handleMessages();
  void publishMessages();
  static MosquittoPublisher *convertCallbackObject(void *callbackObj);
  static void *convertCallbackObject(MosquittoPublisher *client);
  static void defaultOnConnect(struct mosquitto *clientPtr, void *callbackObj,
                               int reasonCode);
  static void defaultOnPublish(struct mosquitto *clientPtr, void *callbackObj,
                               int messageId);
};

class MosquittoClient {
public:
  using onMessageCallback_t =
      std::function<void(MosquittoClient *, const std::string &topic,
                         const std::string &message, int qos)>;

private:
  struct mosquitto *client;
  const std::string topic;
  std::queue<std::unique_ptr<std::pair<std::string, std::string>>> messages;
  std::jthread thread;
  bool running{false};
  onMessageCallback_t onMessageCallback{MosquittoClient::defaultOnMessageCallback};

  MosquittoClient(const std::string &topic);

  void setOnMessageCallback(onMessageCallback_t callback);

  void init(struct mosquitto *client);

public:
  ~MosquittoClient();

  static std::optional<std::unique_ptr<MosquittoClient>>
  create(const std::string &brokerHostname, int brokerPort,
         const std::string &topic,
         const std::optional<onMessageCallback_t>& onMessageCallbackMaybe = {});
  void start();
  void stop();

private:
  void handleMessages();
  void addMessageToQueue(const std::string &topic, const std::string &msg);
  static MosquittoClient *convertCallbackObject(void *callbackObj);
  static void *convertCallbackObject(MosquittoClient *client);
  static void defaultOnConnect(struct mosquitto *clientPtr, void *callbackObj,
                               int reasonCode);
  static void defaultOnSubscribe(struct mosquitto *clientPtr, void *callbackObj,
                                 int messageId, int qualitOfServiceCount,
                                 const int *grantedQualityOfService);
  static void onMessageHandler(struct mosquitto *clientPtr, void *callbackObj,
                               const struct mosquitto_message *message);
  static void defaultOnMessageCallback(MosquittoClient *client,
                                       const std::string &topic,
                                       const std::string &message, int qos);
};



class MosquittoBroker {
public:
  virtual ~MosquittoBroker() {}

  virtual std::optional<std::unique_ptr<MosquittoClient>>
  createClient(const std::string &topic) = 0;
  virtual std::optional<std::unique_ptr<MosquittoClient>>
  createClient(const std::string &topic,
               std::function<void(MosquittoClient *, const std::string &,
                                  const std::string &, int)>
                   onMessageCallback) = 0;
  virtual std::optional<std::unique_ptr<MosquittoPublisher>>
  createPublisher(const std::string &topic) = 0;
};

std::unique_ptr<MosquittoBroker>
createMosquittoBroker(const std::string &hostname, int port);

} // namespace nat::mosquitto
