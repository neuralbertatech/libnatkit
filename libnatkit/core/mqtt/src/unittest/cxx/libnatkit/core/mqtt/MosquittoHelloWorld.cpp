/*
 * This example shows how to write a client that subscribes to a topic and does
 * not do anything other than handle the messages that are received.
 */

#include <gtest/gtest.h>
#include <mosquitto.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cstdlib>
#include <string>
#include <libnatkit-mqtt.hpp>


 /* Callback called when the client receives a CONNACK message from the broker. */
void on_connect(struct mosquitto* mosq, void* obj, int reason_code)
{
	int rc;
	/* Print out the connection result. mosquitto_connack_string() produces an
	 * appropriate string for MQTT v3.x clients, the equivalent for MQTT v5.0
	 * clients is mosquitto_reason_string().
	 */
	printf("on_connect: %s\n", mosquitto_connack_string(reason_code));
	if (reason_code != 0) {
		/* If the connection fails for any reason, we don't want to keep on
		 * retrying in this example, so disconnect. Without this, the client
		 * will attempt to reconnect. */
		mosquitto_disconnect(mosq);
	}

	/* Making subscriptions in the on_connect() callback means that if the
	 * connection drops and is automatically resumed by the client, then the
	 * subscriptions will be recreated when the client reconnects. */
	rc = mosquitto_subscribe(mosq, NULL, "example/temperature", 1);
	if (rc != MOSQ_ERR_SUCCESS) {
		fprintf(stderr, "Error subscribing: %s\n", mosquitto_strerror(rc));
		/* We might as well disconnect if we were unable to subscribe */
		mosquitto_disconnect(mosq);
	}
}


/* Callback called when the broker sends a SUBACK in response to a SUBSCRIBE. */
void on_subscribe(struct mosquitto* mosq, void* obj, int mid, int qos_count, const int* granted_qos)
{
	int i;
	bool have_subscription = false;

	/* In this example we only subscribe to a single topic at once, but a
	 * SUBSCRIBE can contain many topics at once, so this is one way to check
	 * them all. */
	for (i = 0; i < qos_count; i++) {
		printf("on_subscribe: %d:granted qos = %d\n", i, granted_qos[i]);
		if (granted_qos[i] <= 2) {
			have_subscription = true;
		}
	}
	if (have_subscription == false) {
		/* The broker rejected all of our subscriptions, we know we only sent
		 * the one SUBSCRIBE, so there is no point remaining connected. */
		fprintf(stderr, "Error: All subscriptions rejected.\n");
		mosquitto_disconnect(mosq);
	}
}


/* Callback called when the client receives a message. */
void on_message(struct mosquitto* mosq, void* obj, const struct mosquitto_message* msg)
{
	/* This blindly prints the payload, but the payload can be anything so take care. */
	printf("%s %d %s\n", msg->topic, msg->qos, (char*)msg->payload);
}

// Whether the caller has opted into tests that reach a THIRD-PARTY server.
//
// ⚠️ Off by default, and that is the whole point of this ticket
// (TEC-NATKIT-79). `ManualConnection.HelloWorld` connects to
// `test.mosquitto.org` — somebody else's public broker, on the internet. On a
// machine that cannot reach it the test spends 133 SECONDS timing out and then
// fails, which made `ctest` red for the whole repository: every subsequent change
// had to decide whether the one red test was theirs, and that is exactly how a
// real failure eventually gets waved through.
//
// It is also not ours to hammer. Opt in with NATKIT_TEST_PUBLIC_BROKER=1 when you
// actually want to check interoperability against a public broker.
bool publicBrokerTestsEnabled() {
    const char* opt_in = std::getenv("NATKIT_TEST_PUBLIC_BROKER");
    return opt_in != nullptr && std::string(opt_in) == "1";
}

void manually_test_connection(const std::string& host, int port) {
	struct mosquitto* mosq;
	int rc;

	mosquitto_lib_init();
	mosq = mosquitto_new(NULL, true, NULL);
	EXPECT_TRUE(mosq != NULL);
	if (mosq == NULL) {
		fprintf(stderr, "Error: Out of memory.\n");
	}
	mosquitto_connect_callback_set(mosq, on_connect);
	mosquitto_subscribe_callback_set(mosq, on_subscribe);
	mosquitto_message_callback_set(mosq, on_message);
	rc = mosquitto_connect(mosq, host.c_str(), port, 60);
	// Says WHICH broker and what to do about it. A bare "expected 0, got 14" left
	// the reader to find the hard-coded host in the source before they could tell
	// an unreachable broker from a broken client.
	EXPECT_EQ(rc, MOSQ_ERR_SUCCESS)
		<< "could not connect to " << host << ":" << port << " — "
		<< mosquitto_strerror(rc)
		<< (host == "localhost"
			    ? ". Is the natKit dev stack running? (mosquitto is one of its services)"
			    : ". This is a third-party broker; set NATKIT_TEST_PUBLIC_BROKER=1 only if you meant to reach it.");
	if (rc != MOSQ_ERR_SUCCESS) {
		mosquitto_destroy(mosq);
		fprintf(stderr, "Error: %s\n", mosquitto_strerror(rc));
	}

	mosquitto_lib_cleanup();
}


TEST(ManualConnection, HelloWorld)
{
	if (!publicBrokerTestsEnabled()) {
		GTEST_SKIP() << "skipped: reaches test.mosquitto.org, a third-party public "
		                "broker. Set NATKIT_TEST_PUBLIC_BROKER=1 to run it.";
	}
	manually_test_connection("test.mosquitto.org", 1883);
}


TEST(ManualConnection, NatKitServer)
{
	manually_test_connection("localhost", 1883);
}

TEST(FrameWorkConnection, NatKitServer)
{
	const auto mosquitto_broker = nat::mosquitto::createMosquittoBroker("localhost", 1883);
	const auto client = mosquitto_broker->createClient("#", [this](auto* client, const auto& topic, const auto& message, auto qos) { ; });
	EXPECT_TRUE(client.has_value());
}