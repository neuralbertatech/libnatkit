#include <gtest/gtest.h>
#include <string>
#include <iostream>

#include <libnatkit/core/streams/stream/Stream.hpp>
//#include <libnatkit/core/streams/stream/StreamType.h>
//#include <libnatkit/core/streams/stream/StreamId.h>

class StreamTest : public ::testing::Test {
public:

};

TEST_F(StreamTest, parseStreamString_validMetaStream) {
    Stream* stream;
    const std::string streamString = std::string("meta-123-myEncoder-mySchema");
    const char* streamCString = streamString.c_str();
    const auto result = parseStreamString(streamCString, &stream);
    if (isResultFailure(result)) {
        std::cout << result.failureMessage << '\n';
        ASSERT_TRUE(false);
    }
    EXPECT_STREQ(streamCString, stream->name);
    EXPECT_EQ(STREAMTYPE_META, stream->type);
    EXPECT_EQ(123, stream->id);
    EXPECT_STREQ("myEncoder", stream->encoder);
    EXPECT_STREQ("mySchema", stream->schema);
    std::cout << "Hi there\n";

    freeStream(stream);
}
