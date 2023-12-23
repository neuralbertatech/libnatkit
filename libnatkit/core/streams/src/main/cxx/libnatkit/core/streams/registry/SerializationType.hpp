#pragma once

#include <cassert>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <unordered_map>

#include <libnatkit/util/Strings.hpp>

namespace nat::kafka {

enum class SerializationType {
  Json
};

static const std::unordered_map<SerializationType, std::string>
    serializationTypeToStringMapping = {
        {SerializationType::Json, "Json"},
};

static const std::unordered_map<std::string, SerializationType>
    stringToSerializationTypeMapping = []() {
      std::unordered_map<std::string, SerializationType> newMap{};
      std::transform(
          serializationTypeToStringMapping.begin(), serializationTypeToStringMapping.end(),
          std::inserter(newMap, newMap.end()),
          [](const auto &pair) -> std::pair<std::string, SerializationType> {
            return {::nat::util::Strings::toLowercase(pair.second), pair.first};
          });
      return newMap;
    }();

static const std::unordered_map<std::string, SerializationType>
    lowercaseStringToSerializationTypeMapping = []() {
      std::unordered_map<std::string, SerializationType> lowercaseMap{};
      for (const auto& [key, val] : stringToSerializationTypeMapping) {
        lowercaseMap[util::Strings::toLowercase(key)] = val;
      }
      return lowercaseMap;
    }();

SerializationType getSerializationType(const std::string& encoderName);

std::string toString(const SerializationType& streamType);

std::optional<SerializationType>
serializationTypeFromString(const std::string &streamTypeString);

}
