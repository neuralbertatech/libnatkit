#pragma once

#include <vector>
#include <cstdint>

#include <libnatkit/kafkit/core/registry/SerializationType.hpp>

namespace nat::kafkit {

template <typename T>
class Schema {
  public:
    virtual std::vector<uint8_t> encode(const SerializationType& type) = 0;

    virtual T decode(const std::vector<uint8_t>& message, const SerializationType& type) = 0;
};

}
