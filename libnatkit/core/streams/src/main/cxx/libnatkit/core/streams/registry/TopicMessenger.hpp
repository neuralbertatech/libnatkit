#pragma once

#include <libnatkit/core/streams/registry/MessagingQueue.hpp>
#include <libnatkit/core/streams/registry/TopicTranslator.hpp>


namespace nat::kafka {

class TopicMessenger {
  std::unique_ptr<MessagingQueue> messagingQueue;
  std::shared_ptr<TopicTranslator> translator;

  public:
  TopicMessenger(std::unique_ptr<MessagingQueue>&& messagingQueue, const std::shared_ptr<TopicTranslator> translator)
    : messagingQueue(std::move(messagingQueue)), translator(translator) {}
  
  void sendMessage(const Schema &schema) {
    auto encodedMessageMaybe = translator->tryEncodeMessage(schema);
    if (encodedMessageMaybe.has_value()) {
      messagingQueue->enqueueMessageToSend(std::move(encodedMessageMaybe.value()));
    }
  }

  std::optional<std::unique_ptr<Schema>> tryGetNexMessage() {
    const auto message = messagingQueue->tryGetNextMessage();
    if (message.has_value()) {
      return translator->tryDecodeMessage(*message.value());
    } else {
      return {};
    }
  }
};

}
