#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <unordered_map>

#include <libnatkit/kafkit/core/registry/Decoder.hpp>
#include <libnatkit/kafkit/core/registry/Encoder.hpp>
#include <libnatkit/kafkit/core/registry/SerializationType.hpp>
#include <libnatkit/kafkit/core/schemas/Schema.hpp>
#include <libnatkit/kafkit/core/stream/BasicTopicInformation.hpp>

namespace nat::kafkit {

class Registry {
    std::unordered_map<std::string, std::shared_ptr<Encoder>> encoders;
    std::unordered_map<std::string, decoder_t> decoders;
    std::unordered_map<std::string, std::vector<std::function<void(const std::shared_ptr<Schema>&)>>> schemaHandlers;

    static std::string createKey(const std::string& schemaName, const SerializationType& type) {
      return schemaName + '-' + toString(type);
    }

    static std::string createKey(const StreamMessage& message) {
      return createKey(message.getSchemaName(), message.getSerializationType());
    }

    static std::string createKey(const BasicTopicInformation& topicInfo) {
      return createKey(topicInfo.schemaName, topicInfo.serializationType);
    }

	public:
    Registry() = default;

    static std::unique_ptr<Registry> createDefaultInitalizeRegistry();

    void registerEncoder(const std::string& schemaName, const SerializationType& type, const std::shared_ptr<Encoder>& encoder) {
      const auto [_, wasEntryAdded] = encoders.try_emplace(createKey(schemaName, type), encoder);
    }

    void registerDecoder(const std::string& schemaName, const SerializationType& type, const decoder_t& decoder) {
      const auto [_, wasEntryAdded] = decoders.try_emplace(createKey(schemaName, type), decoder);
    }

    void registerSchemaHandler(const std::string& schemaName, const SerializationType& type, const std::function<void(const std::shared_ptr<Schema>&)>& dispatchFunction) {
      const auto key = createKey(schemaName, type);
      if (const auto results = schemaHandlers.find(key); results != schemaHandlers.end()) {
        results->second.push_back(dispatchFunction);
      } else {
        schemaHandlers.emplace(std::make_pair(key, std::vector<std::function<void(const std::shared_ptr<Schema>&)>>{dispatchFunction}));
      }
    }
    
    std::optional<std::unique_ptr<Schema>> tryDecode(const std::vector<uint8_t>& message, const BasicTopicInformation& topicInfo) const {
      const auto key = createKey(topicInfo);
     if (const auto results = decoders.find(key); results != decoders.end()) {
       return results->second(message, topicInfo.serializationType);
     } else {
        return {};
     }
    }

    void dispatchOnDecode(const std::vector<uint8_t>& message, const BasicTopicInformation& topicInfo) {
      const std::optional<std::shared_ptr<Schema>> schemaMaybe = tryDecode(message, topicInfo);
      if (!schemaMaybe.has_value()) {
        return;
      }

      const auto key = createKey(topicInfo);
      if (const auto results = schemaHandlers.find(key); results != schemaHandlers.end()) {
          for (const auto& handler : results->second) {
            handler(schemaMaybe.value());
          }
      }
    }
};

}
