#include <iostream>
#include <libnatkit/kafkit/core/broker/BrokerManager.hpp>

int main() {
	const auto manager = nat::kafkit::createBrokerManager("perrin.selk.io", "29093");
	const auto topics = manager->getAllTopicStrings();
	for (size_t i = 0; i < std::ssize(topics); ++i) {
		std::cout << topics[i] << std::endl;
	}

	return 0;
}
