#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>

#include "BrokerManagerMock.hpp"


using ::testing::Return;

namespace nat::kafka {

class BrokerManagerTest: public ::testing::Test {
public:
  std::unique_ptr<BrokerManagerMock> manager;

  void SetUp() override {
    manager = std::make_unique<BrokerManagerMock>();
  }
};

//TEST_F(BrokerManagerTest, connectToBroker) {
//  ASSERT_TRUE(manager->isConnected());
//}

TEST_F(BrokerManagerTest, createProducer) {
  const auto producer = manager->createProducer();
  ASSERT_TRUE(producer);
  std::cout << "New Producer: " << producer->name() << '\n';
}

TEST_F(BrokerManagerTest, produceMessages) {
  const auto producer = manager->createProducer();
  std::string msg = "Hi there";
  const auto err = producer->produce("myTopic", RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(msg.c_str()), msg.size(), NULL, 0, 0, NULL, NULL);
  producer->poll(1000);
  if (err != RdKafka::ERR_NO_ERROR) {
    std::cout << "Error: " << RdKafka::err2str(err) << '\n';
    ASSERT_TRUE(false);
  } else {
    std::cout << "Success\n";
    const auto topics = manager->getAllTopicStrings(false);
    std::cout << "topics = [";
    for (size_t i = 0; i < std::ssize(topics); ++i) {
      std::cout << topics[i];
      if (i < std::ssize(topics) - 1) {
        std::cout << ", ";
      }
    }
    std::cout << "]\n";
  }
}

}
