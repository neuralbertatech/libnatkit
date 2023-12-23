#include <gtest/gtest.h>
#include <string>
#include <iostream>

#include <libnatkit/core/streams/stream/Stream.hpp>

namespace nat::kafka {

class StreamTest : public ::testing::Test {
public:

};

TEST_F(StreamTest, parseStreamString_validMetaStream) {
    const auto streamMaybe = Stream::createFromKafkaBrokerName("meta-123-json-mySchema");
    ASSERT_TRUE(streamMaybe.has_value());
    const auto stream = streamMaybe.value();
    ASSERT_EQ(StreamType::META, stream.getType());
    ASSERT_EQ(123, stream.getId());
    ASSERT_EQ("json", stream.getEncoderName());
    ASSERT_EQ("mySchema", stream.getSchemaName());
}

TEST_F(StreamTest, parseStreamString_validDataStream) {
    const auto streamMaybe = Stream::createFromKafkaBrokerName("data-123-json-mySchema");
    ASSERT_TRUE(streamMaybe.has_value());
    const auto stream = streamMaybe.value();
    ASSERT_EQ(StreamType::DATA, stream.getType());
    ASSERT_EQ(123, stream.getId());
    ASSERT_EQ("json", stream.getEncoderName());
    ASSERT_EQ("mySchema", stream.getSchemaName());
}



}
