#include <iostream>
#include <libnatkit/kafkit/core/broker/BrokerManager.hpp>

int main() {
	const auto manager = nat::kafkit::createBrokerManager("perrin.selk.io", "29093");
	const auto streams = manager->getAllStreams();
	for (size_t i = 0; i < std::ssize(streams); ++i) {
		std::cout << streams[i]->toPrettyString() << std::endl;
	}

	return 0;
}
