#pragma once

#include <libnatkit/kafkit/core/registry/SerializationType.hpp>


namespace nat::kafkit {

class Decoder {
  public:
    virtual ~Decoder();

    virtual bool isSerializationTypeSupported(const SerializationType) = 0;
};

}
