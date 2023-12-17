#pragma once

#include <memory>
#include <libnatkit/core/streams/registry/encoder.hpp>
#include <libnatkit/core/streams/registry/decoder.hpp>

namespace nat::kafka {

class Translator {
  std::shared_ptr<Encoder> encoder;
  std::shared_ptr<Decoder> decoder;

  public:
};

}
