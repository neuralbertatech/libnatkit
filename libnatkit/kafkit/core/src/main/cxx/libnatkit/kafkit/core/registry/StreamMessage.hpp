#pragma once

#include <libnatkit/kafkit/core/registry/PlainTextMessage.hpp>
#include <libnatkit/kafkit/core/stream/Stream.hpp>
#include <libnatkit/kafkit/core/stream/StreamType.hpp>

namespace nat::kafkit {

class StreamMessage {
  const std::string message;
  const Stream stream;

  public:
    StreamMessage(const PlainTextMessage& message, const Stream& stream) : message(message.getPlainTextMessage()) stream(stream) {}

    std::string getMessage() const { return message; }

    SerializationType getSerializationType() const { return stream.getSerializationType(); }     
};

}
