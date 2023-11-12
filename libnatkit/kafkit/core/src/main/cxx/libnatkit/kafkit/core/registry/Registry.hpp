#pragma once

#include <string>
#include <unordered_map>

#include <libnatkit/kafkit/core/registry/Decoder.hpp>
#include <libnatkit/kafkit/core/registry/Encoder.hpp>

namespace nat::kafkit {

class Registry {
  std::unordered_map<std::string, Encoder> encoders;
  std::unordered_map<std::string, Decoder> decoders;
	public:
};

}
