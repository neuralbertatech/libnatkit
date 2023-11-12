#pragma once

#include <memory>
#include <libnatkit/kafkit/core/registry/encoder.hpp>
#include <libnatkit/kafkit/core/registry/decoder.hpp>

namespace nat::kafkit {

class Translator {
  std::shared_ptr<Encoder> encoder;
  std::shared_ptr<Decoder> decoder;

  public:
};

}
