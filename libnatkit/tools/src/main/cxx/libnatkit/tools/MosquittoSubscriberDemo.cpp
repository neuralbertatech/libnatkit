#include <stdlib.h>
#include <stdio.h>
#include <mosquitto.h>
#include <iostream>

#include <libnatkit/util/Strings.hpp>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <broker> <topic>\n";
    exit(1);
  }

  const std::string broker = argv[1];
  const std::string topic = argv[2];
  const auto splitBroker = nat::util::Strings::split(broker, ':');
  if (splitBroker.size() != 2) {
    std::cerr << "<broker> takes the form of <hostname>:<port>\n";
    exit(1);
  }
  const auto brokerHost = splitBroker[0];
  const auto brokerPort = std::stoi(splitBroker[1]);


  int rc;
	struct mosquitto_message *msg;

	mosquitto_lib_init();

	rc = mosquitto_subscribe_simple(
			&msg, 1, true,
			topic.c_str(), 0,
			brokerHost.c_str(), brokerPort,
			NULL, 60, true,
			NULL, NULL,
			NULL, NULL);

	if(rc){
		printf("Error: %s\n", mosquitto_strerror(rc));
		mosquitto_lib_cleanup();
		return rc;
	}

	printf("%s %s\n", msg->topic, (char *)msg->payload);
	mosquitto_message_free(&msg);

	mosquitto_lib_cleanup();

	return 0;
}
