#pragma once

#include <libnatkit/kafkit/core/registry/Encoder.hpp>

namespace nat::kafkit {

class JsonEncoder : public Encoder {
  public:
    JsonEncoder() = default;

    bool isSerializationTypeSupported(const SerializationType type) override {
      switch(type) {
        case SerializationType::Json:
          return true;
        default:
          return false;
      }
    }

    std::vector<uint8_t> encode(const StreamMessage& message) override {

    }
 
};

}
