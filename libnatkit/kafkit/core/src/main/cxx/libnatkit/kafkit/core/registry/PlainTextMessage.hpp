#pragma once

#include <string>

#include <libnatkit/kafkit/core/stream/StreamType.hpp>

namespace nat::kafkit {

class PlainTextMessage {
    const std::string plainTextMessage;

  public:
    PlainTextMessage(const std::string& message) : plainTextMessage(message) {}

    std::string getPlainTextMessage() const { return plainTextMessage; }
};

}
