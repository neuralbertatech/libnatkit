#include <libnatkit-mqtt.hpp>

namespace nat::mosquitto {

class MosquittoBrokerImpl : public MosquittoBroker {
  std::string hostname;
  int port;

public:
  MosquittoBrokerImpl(const std::string &hostname, int port)
      : hostname(hostname), port(port) {
    std::cout << "  [MQTT] Initializing Mosquitto library...\n";
    int init_result = mosquitto_lib_init();
    if (init_result == MOSQ_ERR_SUCCESS) {
      std::cout << "  [MQTT] ✓ Mosquitto library initialized\n";
    } else {
      std::cout << "  [MQTT] ✗ Mosquitto library init failed: " << init_result << "\n";
    }
    std::cout << "  [MQTT] Target: " << this->hostname << ":" << this->port << "\n";
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
                         const std::string &, int)>
          onMessageCallback) override {
      std::cout << "  [MQTT] Creating client for topic: " << topic << "\n";
      std::cout << "  [MQTT] Connecting to: " << hostname << ":" << port << "\n";
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
