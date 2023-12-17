set(LIBNATKIT_TOOLS_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(list-topics ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListTopics.cpp)
target_link_libraries(list-topics PRIVATE
	core-kafka-cxx
	)

add_executable(list-basic-streams ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListBasicStreamInfo.cpp)
target_link_libraries(list-basic-streams PRIVATE
	core-kafka-cxx
	)

add_executable(produce-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ProduceTopic.cpp)
target_link_libraries(produce-topic PRIVATE
	core-kafka-cxx
	)

add_executable(consume-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ConsumeTopic.cpp)
target_link_libraries(consume-topic PRIVATE
	core-kafka-cxx
	)

add_executable(producer-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ProducerDemo.cpp)
target_link_libraries(producer-demo PRIVATE
	core-kafka-cxx
	)

add_executable(mosquitto-subscriber ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoSubscriber.cpp)
target_link_libraries(mosquitto-subscriber PRIVATE
  core-mqtt-cxx
	)

add_executable(mosquitto-publisher ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoPublisher.cpp)
target_link_libraries(mosquitto-publisher PRIVATE
  core-mqtt-cxx
	)

add_executable(mosquitto-subscriber-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoSubscriberDemo.cpp)
target_link_libraries(mosquitto-subscriber-demo PRIVATE
  core-mqtt-cxx
	)

add_executable(mosquitto-publisher-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoPublisherDemo.cpp)
target_link_libraries(mosquitto-publisher-demo PRIVATE
  core-mqtt-cxx
	)

add_executable(kafka-mosquitto-bridge ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/KafkaMosquittoBridge.cpp)
target_link_libraries(kafka-mosquitto-bridge PRIVATE
  core-bridge-cxx
	)


