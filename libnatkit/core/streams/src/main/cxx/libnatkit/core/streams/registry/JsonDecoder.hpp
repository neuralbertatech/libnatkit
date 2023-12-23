#pragma once

#include <libnatkit/core/streams/registry/Decoder.hpp>

namespace nat::kafka {

class JsonDecoder : public Decoder {
  public:
    bool isSerializationTypeSupported(const SerializationType type) override {
      switch(type) {
        case SerializationType::Json:
          return true;
        default:
          return false;
      }
    }

    // TODO: Change this return type
  std::vector<uint8_t> decode(const StreamMessage& message) override {
    switch(message.getSerializationType()) {
      case SerializationType::Json:
        // TODO: Need a json library
        break;
      default:
        return {};
    }
  }
};

}
