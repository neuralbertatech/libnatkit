#pragma once

#include <iostream>
#include <memory>

#include <libnatkit/kafkit/core/registry/SerializationType.hpp>
#include <libnatkit/kafkit/core/stream/StreamType.hpp>

namespace nat::kafkit {

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
    return "BasicTopicInformation: {type=\"" + nat::kafkit::toString(type) +
           "\", id=" + std::to_string(id) + ", serializationType=\"" +
           ::nat::kafkit::toString(serializationType) + "\", schemaName=\"" +
           schemaName + "\"}";
  }

  std::string toTopicString() const {
    return ::nat::kafkit::toString(type) + "-" + std::to_string(id) + "-" +
           ::nat::kafkit::toString(serializationType) + "-" + schemaName;
  }

private:
  BasicTopicInformation(const StreamType &type,
                        const SerializationType &serializationType,
                        const uint64_t id, const std::string &schemaName)
      : type(type), serializationType(serializationType), id(id),
        schemaName(schemaName) {}
};

} // namespace nat::kafkit
