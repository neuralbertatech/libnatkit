/*
 * libnatkit-kafka-abi.h -- stable C ABI for the librdkafka-backed Kafka
 * transport (nat::kafka::BrokerManager / nat::core::TopicMessenger).
 *
 * This is the Phase 4 companion to libnatkit-core-abi.h. It lives in the full
 * libnatkit tree (not libnatkit-core) because the Kafka transport links
 * librdkafka, which libnatkit-core -- the lean, embedded/ESP-IDF-friendly
 * library -- deliberately has no dependency on. The two ABIs ship as two
 * separate shared libraries and are bound separately from each language:
 * liblibnatkit-core.so (LIBNATKIT_CORE_PATH) and liblibnatkit-kafka.so
 * (LIBNATKIT_KAFKA_PATH).
 *
 * Like libnatkit-core-abi.h this header is intentionally PURE C so that Rust
 * `bindgen`, Java `jextract` / Panama FFM, and plain C consumers can parse it.
 * Do not add C++ syntax here.
 *
 * Conventions (full policy: lib/libnatkit-core/docs/ABI_CONVENTIONS.md):
 *   - Symbols are named nat_kafka_v1_<verb>; _v1_ is frozen once shipped.
 *   - Every function returns `int`: 0 (NAT_OK) == success, nonzero == a status
 *     code from libnatkit-core-abi.h. No C++ exception crosses the boundary.
 *   - Stateful resources use OPAQUE HANDLES: an explicit _create returns a
 *     handle, a matching _destroy releases it. A handle is never valid across a
 *     process boundary -- each process owns the handles it creates.
 *   - Variable-length outputs use the TWO-CALL pattern: call once with a NULL
 *     output buffer to learn the required size (through an in/out size pointer,
 *     including the trailing NUL for strings), allocate, then call again.
 *
 * Threading: the status/registration state reached by broker creation is
 * thread-safe (see the ABI_CONVENTIONS.md audit). A single broker handle may be
 * shared across threads. A single MESSENGER handle, however, must not be used
 * concurrently from multiple threads (its send queue is thread-safe, but the
 * receive two-call keeps a peeked message in the handle) -- give each thread
 * its own messenger, exactly as one gives each thread its own confluent-kafka
 * Consumer.
 */
#ifndef LIBNATKIT_KAFKA_ABI_H
#define LIBNATKIT_KAFKA_ABI_H

#include <stddef.h>
#include <stdint.h>

/* Reuse the single NAT_OK / NAT_ERR_* status-code registry. */
#include <libnatkit-core-abi.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Start offsets accepted by nat_kafka_v1_messenger_create. These mirror
 * librdkafka's RdKafka::Topic offset sentinels; any value >= 0 is a concrete
 * offset. NAT_KAFKA_OFFSET_END consumes only messages produced after the
 * messenger is created (live tail); NAT_KAFKA_OFFSET_BEGINNING replays the whole
 * topic from the start (historical drain).
 */
#define NAT_KAFKA_OFFSET_BEGINNING ((int64_t)-2)
#define NAT_KAFKA_OFFSET_END ((int64_t)-1)

/* Opaque handles. The full struct definitions live in the shim .cpp. */
typedef struct nat_kafka_broker nat_kafka_broker_t;
typedef struct nat_kafka_messenger nat_kafka_messenger_t;

/* -- Broker lifecycle ---------------------------------------------------- *
 *
 * Wraps nat::kafka::createBrokerManager(host, port). The connection is lazy --
 * create succeeds as long as the configuration is accepted; the first send/recv
 * is what actually reaches the broker. `host` and `port` are separate strings
 * (e.g. "127.0.0.1" and "29092"), matching the C++ factory; a Python caller
 * that has a combined "host:port" splits on the last ':'.
 *
 * Returns NAT_OK and writes the handle to *out_broker, or NAT_ERR_NULL_ARGUMENT
 * / NAT_ERR_INTERNAL. The handle must be released with
 * nat_kafka_v1_broker_destroy.
 */
int nat_kafka_v1_broker_create(const char *host,
                               const char *port,
                               nat_kafka_broker_t **out_broker);

void nat_kafka_v1_broker_destroy(nat_kafka_broker_t *broker);

/*
 * Enumerate the topic strings on the broker (metadata request). With
 * include_hidden == 0, topics beginning with '_' are omitted (matching the C++
 * BrokerManager default). Output is the topic strings joined by '\n' and
 * NUL-terminated, using the two-call pattern: pass out_topics == NULL to write
 * the required size (including the trailing NUL) to *inout_topics_size. An empty
 * broker yields a single NUL byte (required size 1).
 */
int nat_kafka_v1_broker_list_topics(nat_kafka_broker_t *broker,
                                    int include_hidden,
                                    char *out_topics,
                                    size_t *inout_topics_size);

/* -- Messenger lifecycle & I/O ------------------------------------------- *
 *
 * A messenger is a per-topic send+receive channel over one Kafka topic, backed
 * by a background poll thread. `topic_string` is a full natKit topic string
 * (e.g. "Data-123-Json-ExgPillEmgDataSchemaV1"); it is parsed via
 * BasicTopicInformation::create, so a malformed string returns
 * NAT_ERR_INVALID_ARGUMENT. `start_offset` is NAT_KAFKA_OFFSET_END,
 * NAT_KAFKA_OFFSET_BEGINNING, or a concrete offset >= 0.
 */
int nat_kafka_v1_messenger_create(nat_kafka_broker_t *broker,
                                  const char *topic_string,
                                  int64_t start_offset,
                                  nat_kafka_messenger_t **out_messenger);

void nat_kafka_v1_messenger_destroy(nat_kafka_messenger_t *messenger);

/*
 * Enqueue raw bytes to produce on the messenger's topic. Returns after the bytes
 * are queued on the background thread's send queue (fire-and-forget, matching
 * confluent-kafka producer.produce() semantics); delivery happens on the poll
 * thread. Returns NAT_OK or NAT_ERR_NULL_ARGUMENT.
 */
int nat_kafka_v1_messenger_send(nat_kafka_messenger_t *messenger,
                                const uint8_t *payload,
                                size_t payload_size);

/*
 * Block until every message queued by nat_kafka_v1_messenger_send has been
 * delivered to the broker (bounded internally so a dead broker cannot hang the
 * caller). A produce-then-exit publisher must call this before destroying the
 * messenger, mirroring confluent-kafka's producer.flush(). Returns NAT_OK or
 * NAT_ERR_NULL_ARGUMENT.
 */
int nat_kafka_v1_messenger_flush(nat_kafka_messenger_t *messenger);

/*
 * Non-blocking receive of the next raw message, using the two-call pattern with
 * peek semantics so a too-small buffer never drops a message:
 *
 *   - *out_has_message is set to 1 if a message is available, 0 if the receive
 *     queue is currently empty (in which case nothing else is written).
 *   - With out_payload == NULL (sizing call): if a message is available its byte
 *     count is written to *inout_payload_size and the message is held in the
 *     handle (peeked, not consumed); NAT_OK is returned.
 *   - With out_payload != NULL (fill call): if the held/next message fits it is
 *     copied out, *inout_payload_size is set to the actual size, the message is
 *     consumed, and NAT_OK is returned. If it does not fit,
 *     NAT_ERR_BUFFER_TOO_SMALL is returned with the required size written back
 *     and the message left held for a retry.
 *
 * Poll this in a loop the way confluent-kafka's consumer.poll() is polled.
 */
int nat_kafka_v1_messenger_try_recv(nat_kafka_messenger_t *messenger,
                                    uint8_t *out_payload,
                                    size_t *inout_payload_size,
                                    int *out_has_message);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* LIBNATKIT_KAFKA_ABI_H */
