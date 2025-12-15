#include <libnatkit-kafka.hpp>

using namespace std::chrono_literals;

namespace nat::kafka {

core::message_t BrokerMessagingQueue::ConsumerCallback::stringToMessageType(const std::string &string) {
  return core::message_t{string.begin(), string.end()};
}

BrokerMessagingQueue::ConsumerCallback::ConsumerCallback(BrokerMessagingQueue &messagingQueue)
  : messagingQueue(messagingQueue) {}

void BrokerMessagingQueue::ConsumerCallback::consume_cb(RdKafka::Message &msg, void *opaque) {
  const RdKafka::Headers *headers;
  switch (msg.err()) {
  case RdKafka::ERR__TIMED_OUT:
    break;

  case RdKafka::ERR_NO_ERROR:
    /* Real message */
    //std::cout << "Read msg at offset " << msg.offset() << std::endl;
    /*if (msg.key()) {
      std::cout << "Key: " << *msg.key() << std::endl;
    }*/
    headers = msg.headers();
    if (headers) {
      std::vector<RdKafka::Headers::Header> hdrs = headers->get_all();
      for (size_t i = 0; i < hdrs.size(); i++) {
        const RdKafka::Headers::Header hdr = hdrs[i];

        /*if (hdr.value() != NULL)
          printf(" Header: %s = \"%.*s\"\n", hdr.key().c_str(),
                 (int)hdr.value_size(), (const char *)hdr.value());
        else
          printf(" Header:  %s = NULL\n", hdr.key().c_str());*/
      }
    }
    {
        const uint8_t* payload_start = static_cast<const uint8_t*>(msg.payload());
        size_t payload_len = msg.len();
        
        // Log sample of first IMU reading (first 50 bytes): time(8) + data[10](40) + accuracies(1) + has_data(1)
        if (payload_len >= 50) {
            uint64_t time;
            std::memcpy(&time, payload_start, sizeof(time));
            uint8_t accuracies = payload_start[48];
            uint8_t has_data = payload_start[49];
            int accel = (accuracies >> 4) & 3;
            int gyro = (accuracies >> 2) & 3;
            int rot = accuracies & 3;
            std::cout << "[Kafka] Sample - time: " << time 
                      << ", accuracies: " << (int)accuracies 
                      << " (accel=" << accel << ", gyro=" << gyro << ", rot=" << rot << ")"
                      << ", has_data: " << (int)has_data << "\n";
        }
        
        messagingQueue.onMessageRecieved(
            std::make_unique<core::message_t>(payload_start, payload_start + payload_len));
    }
    /*printf("%.*s\n", static_cast<int>(msg.len()),
           static_cast<const char *>(msg.payload()));*/

    break;

  case RdKafka::ERR__PARTITION_EOF:
    /* Last message */
    messagingQueue.doesBrokerHaveMoreMessagesForReading = true;
    break;

  case RdKafka::ERR__UNKNOWN_TOPIC:
  case RdKafka::ERR__UNKNOWN_PARTITION:
    std::cerr << "Consume failed: " << msg.errstr() << std::endl;
    messagingQueue.doesBrokerHaveMoreMessagesForReading = false;
    break;

  default:
    /* Errors */
    std::cerr << "Consume failed: " << msg.errstr() << std::endl;
    messagingQueue.doesBrokerHaveMoreMessagesForReading = false;
  }
}

BrokerMessagingQueue::BrokerMessagingQueue(
    const std::string &topicName,
    const std::shared_ptr<RdKafka::Producer> &producer,
    const std::shared_ptr<RdKafka::Consumer> &consumer,
    const std::shared_ptr<RdKafka::Topic> &topicHandle,
    std::optional<std::function<void(std::unique_ptr<core::message_t> &&)>>
        onMessageRecievedHandlerMaybe)
    : topicName(topicName), producer(producer), consumer(consumer),
      topicHandle(topicHandle) {
  if (onMessageRecievedHandlerMaybe.has_value()) {
    onMessageRecieved = onMessageRecievedHandlerMaybe.value();
  } else {
    onMessageRecieved = [this](auto &&msg) {
      this->defaultOnMessageRecieved(std::move(msg));
    };
  }
  consumerCallback = std::make_unique<ConsumerCallback>(*this);
  startConsumer();
  thread = std::jthread{&BrokerMessagingQueue::handleMessages, this};
}

BrokerMessagingQueue::~BrokerMessagingQueue() { running = false; }

void BrokerMessagingQueue::enqueueMessageToSend(std::unique_ptr<core::message_t> &&message) {
  const std::lock_guard<std::mutex> lock(sendingQueueLock);
  sendingQueue.push(std::move(message));
}

void BrokerMessagingQueue::enqueueMessageToReceive(const std::shared_ptr<core::message_t> message) {
  const std::lock_guard<std::mutex> lock(receivingQueueLock);
  receivingQueue.push(message);
}

nat::core::Optional<std::shared_ptr<core::message_t>> BrokerMessagingQueue::tryGetNextMessage() {
  const std::lock_guard<std::mutex> lock(receivingQueueLock);
  //std::cout << "reading got a lock\n";
  if (receivingQueue.empty()) {
    return {};
  } else {
    //std::cout << "Got Message!\n";
    auto message = std::move(receivingQueue.front());
    receivingQueue.pop();
    return {std::move(message)};
  }
}

void BrokerMessagingQueue::clearAllMessages() {
    std::queue<std::shared_ptr<core::message_t>> empty{};
    const std::lock_guard<std::mutex> lock(receivingQueueLock);
    std::swap(receivingQueue, empty);
}

std::string BrokerMessagingQueue::byteArrayToString(const std::vector<uint8_t> &byteArray) {
  return std::string{byteArray.begin(), byteArray.end()};
}

void BrokerMessagingQueue::handleMessages() {
  while (running) {
    pollResources();
    readMessages();
    sendMessages();

    std::this_thread::sleep_for(10ms);
  }
}

void BrokerMessagingQueue::pollResources() {
  producer->poll(0);
  consumer->poll(0);
}

void BrokerMessagingQueue::sendMessages() {
    std::queue<std::unique_ptr<core::message_t>> messagesToSend{};
    {
        std::lock_guard<std::mutex> guard(sendingQueueLock);
        while (!sendingQueue.empty()) {
            messagesToSend.push(std::move(sendingQueue.front()));
            sendingQueue.pop();
        }
    }
    while (!messagesToSend.empty()) {
        sendMessage(std::move(messagesToSend.front()));
        messagesToSend.pop();
    }
}

void BrokerMessagingQueue::sendMessage(std::unique_ptr<core::message_t> message) {
  const auto stringMessage = byteArrayToString(*message);
  /*std::cout << "Attempting to send the following message to broker: "
            << stringMessage << '\n';*/
  const auto err = producer->produce(
      topicName, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
      const_cast<char *>(stringMessage.c_str()), stringMessage.size(), NULL,
      0, 0, NULL, NULL);
  if (err != RdKafka::ERR_NO_ERROR) {
    std::cout << "Error: " << RdKafka::err2str(err) << '\n';
  } else {
    /*std::cerr << "% Enqueued message (" << stringMessage.size() << " bytes) "
              << "for topic " << stringMessage << std::endl;*/
  }
  pollResources();
  //std::cerr << "% Flushing final messages..." << std::endl;
  producer->flush(10 * 1000 /* wait for max 10 seconds */);
  if (producer->outq_len() > 0)
    std::cerr << "% " << producer->outq_len()
              << " message(s) were not delivered" << std::endl;
}

void BrokerMessagingQueue::readMessages() {
  doesBrokerHaveMoreMessagesForReading = true;
  do {
    if (consumer->consume_callback(topicHandle.get(), partition, 500,
                                   consumerCallback.get(), nullptr) < 1) {
      doesBrokerHaveMoreMessagesForReading = false;
    }
  } while (doesBrokerHaveMoreMessagesForReading);
  pollResources();
}

void BrokerMessagingQueue::startConsumer(int64_t startOffset) {
  RdKafka::ErrorCode resp =
      consumer->start(topicHandle.get(), partition, startOffset);
  if (resp != RdKafka::ERR_NO_ERROR) {
    std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp)
              << std::endl;
    exit(1);
  }
}

void BrokerMessagingQueue::stopConsumer() { consumer->stop(topicHandle.get(), partition); }

void BrokerMessagingQueue::defaultOnMessageRecieved(std::unique_ptr<core::message_t> &&msg) {
  {
   //     std::cout << "defaultOnMessageRecieved() got message " << core::toString(*msg) << " \n";
    const std::lock_guard<std::mutex> lock(receivingQueueLock);
    //std::cout << "Got lock \n";
    receivingQueue.push(std::move(msg));
  }
}

} // namespace nat::kafka
