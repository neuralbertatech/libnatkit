#include <gtest/gtest.h>
#include <string>
#include <iostream>

#include <libnatkit/kafkit/core/stream/Stream.h>
#include <libnatkit/kafkit/core/stream/StreamType.h>
#include <libnatkit/kafkit/core/stream/StreamId.h>

class StreamTest : public ::testing::Test {
public:

};

TEST_F(StreamTest, parseStreamString_validMetaStream) {
    Stream* stream;
    const std::string streamString = std::string("meta-123-myEncoder-mySchema");
    const char* streamCString = streamString.c_str();
    const auto result = parseStreamString(streamCString, &stream);
    //ASSERT_TRUE(result.isSuccess);
    EXPECT_EQ(1, result);
    EXPECT_STREQ(streamCString, stream->name);
    EXPECT_EQ(STREAMTYPE_META, stream->type);
    EXPECT_EQ(123, stream->id);
    EXPECT_STREQ("myEncoder", stream->encoder);
    EXPECT_STREQ("mySchema", stream->schema);
    std::cout << "Hi there\n";

    freeStream(stream);
}
