#pragma once

#include <memory>

#include <libnatkit/kafkit/core/stream/StreamType.hpp>

namespace nat::kafkit {

struct BasicTopicInformation {
  const StreamType type;
  const uint64_t id;
  const std::string encoderName;
  const std::string schemaName;

  BasicTopicInformation() = delete;
  BasicTopicInformation(const BasicTopicInformation& other) = default;

  static std::optional<std::unique_ptr<BasicTopicInformation>> create(const std::string& kafkaTopicString) {
    const auto splitName = nat::util::Strings::split(kafkaTopicString, '-');
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
      return std::unique_ptr<BasicTopicInformation>(new BasicTopicInformation(streamTypeMaybe.value(), streamId, streamEncoderName, streamSchemaName));
    } else {
      return {};
    }
  }

  std::string toString() const {
    return "BasicTopicInformation: {type=\"" + nat::kafkit::toString(type) + "\", id=" + std::to_string(id) + ", encoderName=\"" + encoderName + "\", schemaName=\"" + schemaName + "\"}";
  }

	private:
  BasicTopicInformation(const StreamType& type, const uint64_t id, const std::string& encoderName, const std::string& schemaName)
	  : type(type), id(id), encoderName(encoderName), schemaName(schemaName) {}
};

}
