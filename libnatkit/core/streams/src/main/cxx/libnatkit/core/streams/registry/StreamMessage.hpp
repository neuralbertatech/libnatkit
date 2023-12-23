#pragma once

#include <cstdint>

#include <libnatkit/core/streams/registry/PlainTextMessage.hpp>
#include <libnatkit/core/streams/stream/Stream.hpp>
#include <libnatkit/core/streams/stream/StreamType.hpp>


namespace nat::kafka {

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
