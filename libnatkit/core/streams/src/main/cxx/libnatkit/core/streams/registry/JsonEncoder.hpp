#pragma once

#include <libnatkit/core/streams/registry/Encoder.hpp>

namespace nat::kafka {

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
