#pragma once

#include <memory>
#include <libnatkit/core/streams/registry/Encoder.hpp>
#include <libnatkit/core/streams/registry/Decoder.hpp>

namespace nat::kafka {

class Translator {
  std::shared_ptr<Encoder> encoder;
  std::shared_ptr<Decoder> decoder;

  public:
};

}
