#pragma once

#include <cstdint>

#include <libnatkit/kafkit/core/registry/PlainTextMessage.hpp>
#include <libnatkit/kafkit/core/stream/Stream.hpp>
#include <libnatkit/kafkit/core/stream/StreamType.hpp>


namespace nat::kafkit {

class StreamMessage {
  const std::vector<uint8_t> message;
  const Stream stream;

public:
  StreamMessage(const std::vector<uint8_t>& message, const Stream& stream) : message(message), stream(stream) {}

  std::vector<uint8_t> getMessage() const { return message; }

  SerializationType getSerializationType() const { return stream.getSerializationType(); }     

  std::string getSchemaName() const { return stream.getSchemaName(); }
};

}
