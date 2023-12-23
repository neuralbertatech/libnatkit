#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include <libnatkit/core/streams/registry/Registry.hpp>
#include <libnatkit/core/streams/registry/SerializationType.hpp>
#include <libnatkit/core/streams/schemas/BasicMetaInfoSchema.hpp>
#include <libnatkit/core/streams/schemas/Schema.hpp>

namespace nat::kafka {

class RegistryTest : public ::testing::Test {
public:
};

TEST_F(RegistryTest, dispatchedDecode_single) {
  Registry registry{};
  const std::string schemaStreamName{"mySchema"};
  const SerializationType serializationType = SerializationType::Json;
  const BasicMetaInfoSchema schema{schemaStreamName};
  std::shared_ptr<BasicMetaInfoSchema> decodedSchemaMaybe{nullptr};
  registry.registerDecoder(schema.getName(), serializationType,
                           BasicMetaInfoSchema::decodeAll);
  registry.registerSchemaHandler(
      schema.getName(), serializationType,
      [&decodedSchemaMaybe](const std::shared_ptr<Schema> &schema) {
        decodedSchemaMaybe = std::make_unique<BasicMetaInfoSchema>(
            std::dynamic_pointer_cast<BasicMetaInfoSchema>(schema)
                ->getStreamName());
      });
  ASSERT_FALSE(decodedSchemaMaybe);
  const auto topicInfo =
      BasicTopicInformation::create("meta-123-json-BasicMetaInfoSchema")
          .value();
  const auto msg =
      BasicMetaInfoSchema{schemaStreamName}.encodeToBytes(serializationType);
  registry.dispatchOnDecode(*msg, *topicInfo);
  ASSERT_TRUE(decodedSchemaMaybe);
  ASSERT_EQ(schemaStreamName, decodedSchemaMaybe->getStreamName());
}

TEST_F(RegistryTest, dispatchedDecode_multiple) {
  Registry registry{};
  const std::string schemaStreamName{"mySchema"};
  const SerializationType serializationType = SerializationType::Json;
  const BasicMetaInfoSchema schema{schemaStreamName};
  std::shared_ptr<BasicMetaInfoSchema> decodedSchemaMaybe0{nullptr};
  std::shared_ptr<BasicMetaInfoSchema> decodedSchemaMaybe1{nullptr};
  registry.registerDecoder(schema.getName(), serializationType,
                           BasicMetaInfoSchema::decodeAll);
  registry.registerSchemaHandler(
      schema.getName(), serializationType,
      [&decodedSchemaMaybe0](const std::shared_ptr<Schema> &schema) {
        decodedSchemaMaybe0 = std::make_unique<BasicMetaInfoSchema>(
            std::dynamic_pointer_cast<BasicMetaInfoSchema>(schema)
                ->getStreamName());
      });
  registry.registerSchemaHandler(
      schema.getName(), serializationType,
      [&decodedSchemaMaybe1](const std::shared_ptr<Schema> &schema) {
        decodedSchemaMaybe1 = std::make_unique<BasicMetaInfoSchema>(
            std::dynamic_pointer_cast<BasicMetaInfoSchema>(schema)
                ->getStreamName());
      });

  ASSERT_FALSE(decodedSchemaMaybe0);
  ASSERT_FALSE(decodedSchemaMaybe1);
  const auto topicInfo =
      BasicTopicInformation::create("meta-123-json-BasicMetaInfoSchema")
          .value();
  const auto msg =
      BasicMetaInfoSchema{schemaStreamName}.encodeToBytes(serializationType);
  registry.dispatchOnDecode(*msg, *topicInfo);
  ASSERT_TRUE(decodedSchemaMaybe0);
  ASSERT_TRUE(decodedSchemaMaybe1);
  ASSERT_EQ(schemaStreamName, decodedSchemaMaybe0->getStreamName());
  ASSERT_EQ(schemaStreamName, decodedSchemaMaybe1->getStreamName());
}

} // namespace nat::kafka
