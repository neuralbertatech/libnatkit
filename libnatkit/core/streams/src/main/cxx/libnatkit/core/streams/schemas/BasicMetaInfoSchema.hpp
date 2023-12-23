#pragma once

#include <cassert>

#include <libnatkit/core/streams/registry/Registry.hpp>
#include <libnatkit/core/streams/schemas/Schema.hpp>

#include <nlohmann/json.hpp>

namespace nat::kafka {

class BasicMetaInfoSchema : public Schema, public Decoder {
  std::string streamName;

public:
  inline static const std::string name = "BasicMetaInfoSchema";

  BasicMetaInfoSchema(const std::string &streamName) : streamName(streamName) {}

  virtual std::unique_ptr<std::vector<uint8_t>>
  encodeToBytes(const SerializationType &type) const override {
    switch (type) {
    case SerializationType::Json:
      nlohmann::json j;
      j["name"] = streamName;
      const auto jsonStr = j.dump();
      return std::make_unique<std::vector<uint8_t>>(std::begin(jsonStr), std::end(jsonStr));
    }
  }

  virtual bool isSerializationTypeSupported(const SerializationType type) const override {
    switch (type) {
    case SerializationType::Json:
      return true;
    }
  }

  virtual std::string toString() const override {
    return getName() + ": {\"name\": " + streamName + "}";
  }

  static std::optional<std::unique_ptr<BasicMetaInfoSchema>> decodeJson(const std::vector<uint8_t> &message) {
      try {
        std::string jsonStr(std::begin(message), std::end(message));
        const auto json = nlohmann::json::parse(jsonStr);
        return std::make_unique<BasicMetaInfoSchema>(json["name"]);
      } catch(...) {
        return {};
      }
  }

  static std::optional<std::unique_ptr<BasicMetaInfoSchema>> decodeAll(const std::vector<uint8_t> &message,
                                    const SerializationType &type) {
    switch (type) {
    case SerializationType::Json:
      return decodeJson(message);
    }
  }

  static void
  decodeAndDispatch(const std::vector<uint8_t> &message,
                    const SerializationType &type,
                    const std::function<void(const std::shared_ptr<Schema> &)>
                        &dispatchMethod) {
    const std::optional<std::shared_ptr<Schema>> decodedMessageMaybe = decodeAll(message, type);
    if (decodedMessageMaybe.has_value()) {
      dispatchMethod(decodedMessageMaybe.value());
    }
  }

  virtual std::optional<std::shared_ptr<Schema>> tryDecode(const std::vector<uint8_t> &message,
                                    const SerializationType &type) const override {
    return decodeAll(message, type);
  }

  static void registerWithRegistry(Registry &registry) {
    registry.registerDecoder(name, SerializationType::Json, decodeAll);
  }

  std::string getName() const override { return name; }

  std::string getStreamName() const { return streamName; }
};

} // namespace nat::kafka
