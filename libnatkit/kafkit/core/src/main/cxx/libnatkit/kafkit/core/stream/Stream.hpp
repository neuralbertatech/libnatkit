#pragma once

#include <cstdint>
#include <iostream>
#include <optional>
#include <string>

#include <libnatkit/kafkit/core/registry/SerializationType.hpp>
#include <libnatkit/kafkit/core/stream/StreamType.hpp>

namespace nat::kafkit {

class Stream {
  const std::string name;
  const StreamType type;
  const uint64_t id;
  const std::string encoderName;
  const SerializationType serializationType;
  const std::string schemaName;

public:
  Stream(const std::string &name, const StreamType &type, uint64_t id,
         const std::string &encoderName, const std::string &schemaName)
      : name(name), type(type), id(id), encoderName(encoderName),
        serializationType(nat::kafkit::getSerializationType(encoderName)),
        schemaName(schemaName) {}

  static std::optional<Stream>
  createFromKafkaBrokerName(const std::string &brokerName) {
    const auto splitName = nat::util::Strings::split(brokerName, '-');
    if (std::ssize(splitName) != 4) {
      return {};
    }
    const auto streamTypeName = splitName[0];
    const auto streamIdString = splitName[1];
    const auto streamEncoderName = splitName[2];
    const auto streamSchemaName = splitName[3];

    const auto streamTypeMaybe = streamTypeFromString(streamTypeName);
    const auto streamId = std::stoll(streamIdString);

    if (streamTypeMaybe.has_value()) {
      return Stream(brokerName, streamTypeMaybe.value(), streamId,
                    streamEncoderName, streamSchemaName);
    } else {
      return {};
    }
  }

  std::string getName() const { return name; }

  StreamType getType() const { return type; }

  uint64_t getId() const { return id; }

  std::string getEncoderName() const { return encoderName; }

  SerializationType getSerializationType() const { return serializationType; }

  std::string getSchemaName() const { return schemaName; }

  std::string toTopicString() const {
    return toString(type) + "-" + std::to_string(id) + "-" + encoderName + "-" + schemaName;
  }
};

} // namespace nat::kafkit
