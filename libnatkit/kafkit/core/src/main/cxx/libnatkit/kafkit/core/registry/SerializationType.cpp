#include <libnatkit/kafkit/core/registry/SerializationType.hpp>
#include <libnatkit/kafkit/core/stream/Stream.hpp>
#include <libnatkit/util/Strings.hpp>

namespace nat::kafkit {

SerializationType getSerializationType(const std::string& encoderName) {
  const auto lowercaseEncoderName = nat::util::Strings::toLowercase(encoderName);
  if (lowercaseEncoderName == "json") {
    return SerializationType::Json;
  }

  std::cout << "Fatal Error: Invalid serialization type '" << encoderName << "'" << std::endl;
  assert(0);
}

}
