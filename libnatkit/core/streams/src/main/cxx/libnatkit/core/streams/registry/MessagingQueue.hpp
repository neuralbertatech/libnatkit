#pragma once

#include <memory>
#include <optional>
#include <libnatkit/core/streams/registry/Message.hpp>

namespace nat::kafka {

class MessagingQueue {
public:
  virtual ~MessagingQueue() = default;

  virtual void enqueueMessageToSend(std::unique_ptr<message_t> &&message) = 0;

  virtual void enqueueMessageToReceive(const std::shared_ptr<message_t> message) = 0;

  virtual std::optional<std::shared_ptr<message_t>> tryGetNextMessage() = 0;
};

}
