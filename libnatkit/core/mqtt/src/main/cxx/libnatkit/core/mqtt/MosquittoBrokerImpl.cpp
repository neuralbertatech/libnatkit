#include <libnatkit-mqtt.hpp>

namespace nat::mosquitto {

class MosquittoBrokerImpl : public MosquittoBroker {
  std::string hostname;
  int port;

public:
  MosquittoBrokerImpl(const std::string &hostname, int port)
      : hostname(hostname), port(port) {
    mosquitto_lib_init();
  }

  ~MosquittoBrokerImpl() { mosquitto_lib_cleanup(); }

  virtual std::optional<std::unique_ptr<MosquittoClient>>
  createClient(const std::string &topic) override {
    return MosquittoClient::create(hostname, port, topic);
  }

  virtual std::optional<std::unique_ptr<MosquittoClient>>
  createClient(
      const std::string &topic,
      std::function<void(MosquittoClient *, const std::string &,
                         const std::vector<uint8_t> &, int)>
          onMessageCallback) override {
    return MosquittoClient::create(hostname, port, topic, onMessageCallback);
  }

  virtual std::optional<std::unique_ptr<MosquittoPublisher>>
  createPublisher(const std::string &topic) override {
    return MosquittoPublisher::create(hostname, port, topic);
  }
};

std::unique_ptr<MosquittoBroker>
createMosquittoBroker(const std::string &hostname, int port) {
  return std::unique_ptr<MosquittoBroker>(
      new MosquittoBrokerImpl(hostname, port));
}

} // namespace nat::mosquitto
