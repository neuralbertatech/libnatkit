#pragma once

#include <cassert>

#include <libnatkit/kafkit/core/schemas/Schema.hpp>

//#include <nlohmann/json.hpp>

namespace nat::kafkit {

class BasicMetaInfoSchema : public Schema<BasicMetaInfoSchema> {
    std::string name;

  public:
    BasicMetaInfoSchema(const std::string& name) {}

    virtual std::vector<uint8_t> encode(const SerializationType& type) override {
      switch(type) {
        case SerializationType::Json:
          return {};
        default:
          return {};
      }
    }

    BasicMetaInfoSchema decode(const std::vector<uint8_t>& message, const SerializationType& type) override {
      switch(type) {
        case SerializationType::Json:
          return BasicMetaInfoSchema("Hi");
        default:
          assert(0);
      }
    }
};

}
