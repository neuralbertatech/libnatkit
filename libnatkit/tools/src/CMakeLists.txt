set(LIBNATKIT_TOOLS_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(libnatkit-list-topics ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListTopics.cpp)
target_link_libraries(libnatkit-list-topics PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-list-basic-streams ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListBasicStreamInfo.cpp)
target_link_libraries(libnatkit-list-basic-streams PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-produce-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ProduceTopic.cpp)
target_link_libraries(libnatkit-produce-topic PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-consume-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ConsumeTopic.cpp)
target_link_libraries(libnatkit-consume-topic PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-producer-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ProducerDemo.cpp)
target_link_libraries(libnatkit-producer-demo PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-delete-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/DeleteTopic.cpp)
target_link_libraries(libnatkit-delete-topic PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-mosquitto-subscriber ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoSubscriber.cpp)
target_link_libraries(libnatkit-mosquitto-subscriber PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-mosquitto-publisher ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoPublisher.cpp)
target_link_libraries(libnatkit-mosquitto-publisher PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-mosquitto-subscriber-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoSubscriberDemo.cpp)
target_link_libraries(libnatkit-mosquitto-subscriber-demo PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-mosquitto-publisher-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoPublisherDemo.cpp)
target_link_libraries(libnatkit-mosquitto-publisher-demo PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-kafka-mosquitto-bridge ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/KafkaMosquittoBridge.cpp)
target_link_libraries(libnatkit-kafka-mosquitto-bridge PRIVATE
  libnatkit-core-bridge-cxx
)

add_executable(libnatkit-natkit-menu ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/NatKitMenu.cpp)
target_link_libraries(libnatkit-natkit-menu PRIVATE
  libnatkit-core-kafka-cxx
)

if(BUILD_NATKIT_BACKEND)
  add_executable(libnatkit-natkit-backend
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/Auth.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/NatKitBackend.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/StreamViewerWebSocket.cpp
  )
  target_link_libraries(libnatkit-natkit-backend PRIVATE
    libnatkit-core-kafka-cxx
    nlohmann
    jsoncpp
    drogon
    z
    brotlidec
    sqlite3
    trantor
    ssl
    crypto
    pq
    mysqlclient
    uuid
    brotlienc
  )
endif()
