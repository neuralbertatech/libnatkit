#pragma once

#include <libnatkit/core/streams/registry/SerializationType.hpp>
#include <libnatkit/core/streams/registry/StreamMessage.hpp>


namespace nat::kafka {

class Encoder {
  public:
    virtual ~Encoder();

    virtual bool isSerializationTypeSupported(const SerializationType) = 0;

    virtual std::vector<uint8_t> encode(const StreamMessage& message) = 0;
};

}
