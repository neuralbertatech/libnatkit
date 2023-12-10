#include <gtest/gtest.h>
#include <iostream>
#include <string>

#include <libnatkit/kafkit/core/registry/SerializationType.hpp>
#include <libnatkit/kafkit/core/schemas/BasicMetaInfoSchema.hpp>
#include <libnatkit/kafkit/core/schemas/Schema.hpp>
#include <libnatkit/util/Casting.hpp>

namespace nat::kafkit {

class BasicMetaInfoSchemaTest : public ::testing::Test {
public:
};

TEST_F(BasicMetaInfoSchemaTest, encodeDecode_json) {
  const BasicMetaInfoSchema schema{"my-schema"};
  const auto jsonEncodedSchema = schema.encodeToBytes(SerializationType::Json);
  const auto decodedSchemaMaybe = BasicMetaInfoSchema::decodeAll(
      *jsonEncodedSchema, SerializationType::Json);
  ASSERT_TRUE(decodedSchemaMaybe.has_value());
  ASSERT_EQ(schema.getStreamName(), decodedSchemaMaybe.value()->getStreamName());
}

TEST_F(BasicMetaInfoSchemaTest, encodeDecodeAndDispatch_json) {
  const BasicMetaInfoSchema schema{"my-schema"};
  const auto jsonEncodedSchema = schema.encodeToBytes(SerializationType::Json);
  std::unique_ptr<BasicMetaInfoSchema> decodedSchema{nullptr};
  BasicMetaInfoSchema::decodeAndDispatch(
      *jsonEncodedSchema, SerializationType::Json,
      [&decodedSchema](const std::shared_ptr<Schema> &schema) {
        decodedSchema = std::make_unique<BasicMetaInfoSchema>(
            std::dynamic_pointer_cast<BasicMetaInfoSchema>(schema)
                ->getStreamName());
      });
  ASSERT_EQ(schema.getStreamName(), decodedSchema->getStreamName());
}

} // namespace nat::kafkit
