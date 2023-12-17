#pragma once

#include <cstdint>
#include <string>
#include <vector>


namespace nat::kafka {

class StreamInfo {
  std::string streamName;

  public:
  StreamInfo(const std::string& streamName) : streamName(streamName) {}
};

}
