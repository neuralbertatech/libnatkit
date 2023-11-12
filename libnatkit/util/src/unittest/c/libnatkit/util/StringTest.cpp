#include <gtest/gtest.h>

#include <libnatkit/util/String.h>

class StringTest: public ::testing::Test {
public:

};

TEST_F(StringTest, referenceCounting_stackBasedString_singleReferenceIsFreed) {
    String string = createStringFromCharPtrWithClone("Hello World");
    freeStringValue(&string);
}

TEST_F(StringTest, stringCopy_shallowCopySuccessful) {
    String string1 = createStringFromCharPtrWithClone("Hello World");
    String string2 = copyString(string1);
    ASSERT_EQ("Hello World", string1.data);
    ASSERT_EQ("Hello World", string2.data);
    ASSERT_EQ(11, string1.size);
    ASSERT_EQ(11, string2.size);
    ASSERT_EQ(2, *string1.numberOfReferences);
    ASSERT_EQ(2, *string2.numberOfReferences);
    ASSERT_EQ(False, string1.heapAllocated);
    ASSERT_EQ(False, string2.heapAllocated);
}
