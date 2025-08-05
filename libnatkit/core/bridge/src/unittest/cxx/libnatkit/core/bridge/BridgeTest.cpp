#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <optional>

#include <libnatkit-bridge.hpp>
#include <libnatkit/util/Casting.hpp>
#include <libnatkit/util/Strings.hpp>

using namespace std::chrono_literals;

TEST(BridgeTesting, Instanciation) {
    auto mosquittoBroker =
        nat::mosquitto::createMosquittoBroker("127.0.0.1", 1883);
    auto kafkaBroker =
        nat::kafka::createBrokerManager("127.0.0.1", "29092");
    ASSERT_NE(mosquittoBroker, nullptr);
    ASSERT_NE(kafkaBroker, nullptr);
    auto bridgeMaybe = nat::bridge::KafkaMosquittoBridge::create(
        nat::util::asShared(std::move(kafkaBroker)),
        nat::util::asShared(std::move(mosquittoBroker)));
    ASSERT_TRUE(bridgeMaybe.has_value());
}
