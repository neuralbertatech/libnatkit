#include <memory>
#include <optional>
#include <string>

#include <libnatkit/core/mqtt/MosquittoClient.hpp>
#include <libnatkit/core/mqtt/MosquittoPublisher.hpp>

namespace nat::mosquitto {

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
