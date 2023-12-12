#pragma once

#include <libnatkit/kafkit/core/registry/SerializationType.hpp>
#include <libnatkit/kafkit/core/registry/StreamMessage.hpp>


namespace nat::kafkit {

class Encoder {
  public:
    virtual ~Encoder();

    virtual bool isSerializationTypeSupported(const SerializationType) = 0;

    virtual std::vector<uint8_t> encode(const StreamMessage& message) = 0;
};

}
