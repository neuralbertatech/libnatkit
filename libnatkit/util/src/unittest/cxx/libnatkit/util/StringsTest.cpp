#include <gtest/gtest.h>

#include <libnatkit/util/Strings.hpp>

class StringsTest: public ::testing::Test {
public:

};

TEST_F(StringsTest, toLowercase) {
    const auto lowerString = nat::util::Strings::toLowercase("Hello THERE WorlD!");
    ASSERT_EQ("hello there world!", lowerString);
}

TEST_F(StringsTest, split) {
    const auto splitString = nat::util::Strings::split("hi,there,world", ',');
    const std::vector<std::string> expected = {"hi", "there", "world"};

    ASSERT_EQ(expected.size(), splitString.size());
    for (int i = 0; i < std::ssize(splitString); ++i) {
      EXPECT_EQ(expected[i], splitString[i]) << "Vectors differ at index " << i;
    }
}
