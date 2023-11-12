#pragma once

#include <cassert>
#include <string>

namespace nat::kafkit {

enum class SerializationType {
  Json
};

SerializationType getSerializationType(const std::string& encoderName);

}
