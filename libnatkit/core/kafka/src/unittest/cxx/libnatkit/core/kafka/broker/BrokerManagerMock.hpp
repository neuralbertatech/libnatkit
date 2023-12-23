#pragma once

#include <gmock/gmock.h>

#include <libnatkit/core/kafka/broker/BrokerManager.hpp>

namespace nat::kafka {

class BrokerManagerMock : public BrokerManager {
  public:
    MOCK_METHOD(bool, isConnected, (), (const, override));

    MOCK_METHOD(std::shared_ptr<RdKafka::Producer>, createProducer, (), (const, override));

    MOCK_METHOD(std::shared_ptr<RdKafka::Consumer>, createConsumer, (), (const, override));

    MOCK_METHOD(std::vector<std::string>, getAllTopicStrings, (bool includeHiddenTopics), (const, override));

    MOCK_METHOD(std::vector<std::unique_ptr<BasicTopicInformation>>, getAllTopics, (), (const, override));

    MOCK_METHOD(std::vector<std::unique_ptr<RawStream>>, getAllStreams, (), (const, override));
};

}
