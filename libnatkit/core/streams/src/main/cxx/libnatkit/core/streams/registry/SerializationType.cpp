#include <libnatkit/core/streams/registry/SerializationType.hpp>
#include <libnatkit/core/streams/stream/Stream.hpp>
#include <libnatkit/util/Strings.hpp>

namespace nat::kafka {

// TODO: DELETEME
SerializationType getSerializationType(const std::string& encoderName) {
  const auto lowercaseEncoderName = nat::util::Strings::toLowercase(encoderName);
  if (lowercaseEncoderName == "json") {
    return SerializationType::Json;
  }

  std::cout << "Fatal Error: Invalid serialization type '" << encoderName << "'" << std::endl;
  exit(1);
}

std::string toString(const SerializationType& serializationType) {
  switch (serializationType) {
    case SerializationType::Json:
      return serializationTypeToStringMapping.at(SerializationType::Json);
  }
}

std::optional<SerializationType>
serializationTypeFromString(const std::string &serializationTypeString) {
  const auto lowercaseType = util::Strings::toLowercase(serializationTypeString);
  if (lowercaseStringToSerializationTypeMapping.contains(lowercaseType)) {
    return lowercaseStringToSerializationTypeMapping.at(lowercaseType);
  } else {
    return {};
  }
}

}
