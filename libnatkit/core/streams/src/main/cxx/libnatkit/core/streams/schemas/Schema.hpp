#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include <libnatkit/core/streams/registry/SerializationType.hpp>
#include <libnatkit/core/streams/registry/Message.hpp>

namespace nat::kafka {

class Schema {
  public:
    virtual bool isSerializationTypeSupported(const SerializationType) const = 0;

    virtual std::unique_ptr<message_t> encodeToBytes(const SerializationType& type) const = 0;

    virtual std::string getName() const = 0;

    virtual std::string toString() const = 0;
};

}
