#pragma once

#include <iostream>
#include <memory>

#include <libnatkit/core/streams/registry/SerializationType.hpp>
#include <libnatkit/core/streams/stream/StreamType.hpp>

namespace nat::kafka {

struct BasicTopicInformation {
  const StreamType type;
  const SerializationType serializationType;
  const uint64_t id;
  const std::string schemaName;

  BasicTopicInformation() = delete;
  BasicTopicInformation(const BasicTopicInformation &other) = default;
  auto operator<=>(const BasicTopicInformation &) const = default;

  static std::optional<std::unique_ptr<BasicTopicInformation>>
  create(const std::string &kafkaTopicString) {
    const auto splitName = nat::util::Strings::split(kafkaTopicString, '-');
    if (std::ssize(splitName) != 4) {
      std::cerr << "Topic String does not contain the four parts\n";
      return {};
    }
    const auto streamTypeName = splitName[0];
    const auto streamIdString = splitName[1];
    const auto streamEncoderName = splitName[2];
    const auto streamSchemaName = splitName[3];

    const auto streamTypeMaybe = streamTypeFromString(streamTypeName);
    const auto serializationTypeMaybe =
        serializationTypeFromString(streamEncoderName);
    const auto streamId = std::stoll(streamIdString);

    if (!streamTypeMaybe.has_value()) {
      std::cerr << "\"" << streamTypeName << "\" is not a valid stream type\n";
      return {};
    }
    if (!serializationTypeMaybe.has_value()) {
      std::cerr << "\"" << streamEncoderName << "\" is not a valid serialization type\n";
      return {};
    }

    return std::unique_ptr<BasicTopicInformation>(new BasicTopicInformation(
        streamTypeMaybe.value(), serializationTypeMaybe.value(), streamId,
        streamSchemaName));
  }

  std::string toString() const {
    return "BasicTopicInformation: {type=\"" + nat::kafka::toString(type) +
           "\", id=" + std::to_string(id) + ", serializationType=\"" +
           ::nat::kafka::toString(serializationType) + "\", schemaName=\"" +
           schemaName + "\"}";
  }

  std::string toTopicString() const {
    return ::nat::kafka::toString(type) + "-" + std::to_string(id) + "-" +
           ::nat::kafka::toString(serializationType) + "-" + schemaName;
  }

private:
  BasicTopicInformation(const StreamType &type,
                        const SerializationType &serializationType,
                        const uint64_t id, const std::string &schemaName)
      : type(type), serializationType(serializationType), id(id),
        schemaName(schemaName) {}
};

} // namespace nat::kafka
