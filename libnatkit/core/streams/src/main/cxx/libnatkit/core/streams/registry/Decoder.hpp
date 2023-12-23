#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include <libnatkit/core/streams/registry/SerializationType.hpp>
#include <libnatkit/core/streams/schemas/Schema.hpp>


namespace nat::kafka {

using decoder_t = std::function<std::optional<std::unique_ptr<Schema>>(const std::vector<uint8_t>& message, const SerializationType& type)>;

class Decoder {
  public:
    virtual ~Decoder() {}

    virtual bool isSerializationTypeSupported(const SerializationType) const = 0;

    virtual std::optional<std::shared_ptr<Schema>> tryDecode(const std::vector<uint8_t> &message,
                                    const SerializationType &type) const = 0;

};

}
