#pragma once

#include <string>

#include <libnatkit/core/streams/stream/StreamType.hpp>

namespace nat::kafka {

class PlainTextMessage {
    const std::string plainTextMessage;

  public:
    PlainTextMessage(const std::string& message) : plainTextMessage(message) {}

    std::string getPlainTextMessage() const { return plainTextMessage; }
};

}
