#include <libnatkit-kafka.hpp>

using namespace std::chrono_literals;

namespace nat::kafka {

namespace {
constexpr int kConsumerPollTimeoutMs = 10;
}

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
        onMessageRecievedHandlerMaybe,
    int64_t startOffset)
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
  startConsumer(startOffset);
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

void BrokerMessagingQueue::flush() {
  // Wait for the background thread to drain the send queue. Each dequeued
  // message is produced with a synchronous producer->flush() in sendMessage(),
  // so once the queue is empty every message has been delivered. Bounded so a
  // dead broker cannot hang the caller forever.
  constexpr int kMaxWaitMs = 10000;
  int waitedMs = 0;
  while (waitedMs < kMaxWaitMs) {
    {
      const std::lock_guard<std::mutex> lock(sendingQueueLock);
      if (sendingQueue.empty()) {
        break;
      }
    }
    std::this_thread::sleep_for(5ms);
    waitedMs += 5;
  }
  // One more poll cycle so an in-progress send finishes flushing.
  producer->poll(0);
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
  // KEYED BY TOPIC NAME, deliberately (TEC-NATKIT-108). With a null key
  // librdkafka's default partitioner picks a partition AT RANDOM PER MESSAGE, so
  // a multi-partition topic loses all ordering -- and this stack's consumers
  // assume a topic's messages arrive in the order they were produced. Keying by
  // the topic name puts every message of one topic in one partition, so that
  // order is preserved whatever the partition count. Each device+schema already
  // has its own topic, so parallelism comes from having many topics rather than
  // from splitting one.
  const auto err = producer->produce(
      topicName, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
      const_cast<char *>(stringMessage.c_str()), stringMessage.size(),
      topicName.c_str(), topicName.size(), 0, NULL, NULL);
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
  // Drain every partition, not just the first. A partition left unread is data
  // that never arrives, with nothing anywhere reporting it.
  for (const auto partition : partitions) {
    doesBrokerHaveMoreMessagesForReading = true;
    do {
      if (consumer->consume_callback(topicHandle.get(), partition, kConsumerPollTimeoutMs,
                                     consumerCallback.get(), nullptr) < 1) {
        doesBrokerHaveMoreMessagesForReading = false;
      }
    } while (doesBrokerHaveMoreMessagesForReading);
  }
  pollResources();
}

void BrokerMessagingQueue::discoverPartitions() {
  partitions.clear();
  RdKafka::Metadata *raw_metadata = nullptr;
  const auto err = consumer->metadata(false, topicHandle.get(), &raw_metadata, 5000);
  const std::unique_ptr<RdKafka::Metadata> metadata{raw_metadata};
  if (err == RdKafka::ERR_NO_ERROR && metadata != nullptr) {
    for (auto topic = metadata->topics()->begin();
         topic != metadata->topics()->end(); ++topic) {
      if ((*topic)->topic() != topicName) {
        continue;
      }
      for (auto part = (*topic)->partitions()->begin();
           part != (*topic)->partitions()->end(); ++part) {
        partitions.push_back((*part)->id());
      }
    }
  }
  if (partitions.empty()) {
    // Metadata was unavailable, or the topic does not exist yet (it is
    // auto-created on first produce). Partition 0 is the old behaviour and the
    // only partition a default broker creates, so this keeps working -- but it
    // is a guess, and a guess about which data we can see should be visible.
    std::cerr << "% Could not read partition metadata for topic " << topicName
              << " (" << RdKafka::err2str(err)
              << "); assuming a single partition. If this topic in fact has "
                 "more, messages on the others will not be read."
              << std::endl;
    partitions.push_back(0);
  } else if (partitions.size() > 1) {
    // Handled correctly now, but worth saying out loud: cross-partition
    // ORDERING is not guaranteed by Kafka, and this stack relies on per-topic
    // order. Keyed production keeps one topic in one partition, so a count
    // above one means somebody pre-created the topic with more.
    std::cerr << "% Topic " << topicName << " has " << partitions.size()
              << " partitions; consuming all of them. Note that production is "
                 "keyed by topic, so only one will normally carry data."
              << std::endl;
  }
}

void BrokerMessagingQueue::startConsumer(int64_t startOffset) {
  discoverPartitions();
  for (const auto partition : partitions) {
    RdKafka::ErrorCode resp =
        consumer->start(topicHandle.get(), partition, startOffset);
    if (resp != RdKafka::ERR_NO_ERROR) {
      std::cerr << "Failed to start consumer for " << topicName << " partition "
                << partition << ": " << RdKafka::err2str(resp) << std::endl;
      exit(1);
    }
  }
}

void BrokerMessagingQueue::stopConsumer() {
  for (const auto partition : partitions) {
    consumer->stop(topicHandle.get(), partition);
  }
}

void BrokerMessagingQueue::defaultOnMessageRecieved(std::unique_ptr<core::message_t> &&msg) {
  {
   //     std::cout << "defaultOnMessageRecieved() got message " << core::toString(*msg) << " \n";
    const std::lock_guard<std::mutex> lock(receivingQueueLock);
    //std::cout << "Got lock \n";
    receivingQueue.push(std::move(msg));
  }
}

} // namespace nat::kafka
