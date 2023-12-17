#pragma once

#include <cstdint>
#include <string>
#include <vector>

//#include <libnatkit/core/streams/stream/StreamInfo.hpp>


namespace nat::kafka {

using message_t = std::vector<uint8_t>;

//class Message {
//  const StreamInfo streamInfo;
//  const std::vector<uint8_t> message;
//
//public:
//  Message(const StreamInfo& streamInfo, const std::vector<uint8_t>& message) : streamInfo(streamInfo), message(message) {}
//
//  StreamInfo getStreamInfo() const { return streamInfo; }
//
//  std::vector<uint8_t> getMessage() const { return message; }
//};

}
