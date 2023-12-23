#pragma once

#include <algorithm>
#include <iterator>
#include <optional>
#include <string>
#include <unordered_map>

#include <libnatkit/util/Strings.hpp>

namespace nat::kafka {

enum class StreamType {
  // Core types
  DATA,
  META,

  // Execution Extension
  EXECUTION_COMMAND,

  // Hardware Extension
  HARDWARE_STATUS,
  HARDWARE_CONFIGURATION,

  // Logging Extension
  LOGGING_LOG,
  LOGGING_HEARTBEAT,
};

static const std::unordered_map<StreamType, std::string>
    streamTypeToStringMapping = {
        {StreamType::DATA, "Data"},
        {StreamType::META, "Meta"},
        {StreamType::EXECUTION_COMMAND, "Command"},
        {StreamType::HARDWARE_STATUS, "Status"},
        {StreamType::HARDWARE_CONFIGURATION, "Configuration"},
        {StreamType::LOGGING_LOG, "Log"},
        {StreamType::LOGGING_HEARTBEAT, "Heartbeat"},
};

static const std::unordered_map<std::string, StreamType>
    stringToStreamTypeMapping = []() {
      std::unordered_map<std::string, StreamType> newMap{};
      std::transform(
          streamTypeToStringMapping.begin(), streamTypeToStringMapping.end(),
          std::inserter(newMap, newMap.end()),
          [](const auto &pair) -> std::pair<std::string, StreamType> {
            return {::nat::util::Strings::toLowercase(pair.second), pair.first};
          });
      return newMap;
    }();

static const std::unordered_map<std::string, StreamType>
    lowercaseStringToStreamTypeMapping = []() {
      std::unordered_map<std::string, StreamType> lowercaseMap{};
      for (const auto& [key, val] : stringToStreamTypeMapping) {
        lowercaseMap[util::Strings::toLowercase(key)] = val;
      }
      return lowercaseMap;
    }();

std::string toString(const StreamType &streamType);

std::optional<StreamType>
streamTypeFromString(const std::string &streamTypeString);

} // namespace nat::kafka
