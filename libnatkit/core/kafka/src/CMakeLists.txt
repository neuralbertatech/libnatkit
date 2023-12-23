set(CORE_KAFKA_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(CORE_KAFKA_C_FILES
  ${CORE_KAFKA_ROOT}/main/c/libnatkit/core/streams/stream/Stream.h
  ${CORE_KAFKA_ROOT}/main/c/libnatkit/core/streams/stream/Stream.c
  ${CORE_KAFKA_ROOT}/main/c/libnatkit/core/streams/stream/StreamExtension.h
  ${CORE_KAFKA_ROOT}/main/c/libnatkit/core/streams/stream/StreamId.h
  ${CORE_KAFKA_ROOT}/main/c/libnatkit/core/streams/stream/StreamType.h
  )

set(CORE_KAFKA_CXX_FILES
  ${CORE_KAFKA_ROOT}/main/cxx/libnatkit/core/kafka/broker/BrokerManager.hpp
  ${CORE_KAFKA_ROOT}/main/cxx/libnatkit/core/kafka/broker/BrokerManagerImpl.cpp
  )


add_library(core-kafka-cxx SHARED ${CORE_KAFKA_CXX_FILES})
set_target_properties(core-kafka-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(core-kafka-cxx PUBLIC
  core-streams-cxx
  librdkafka-cxx
)
target_include_directories(core-kafka-cxx
  PUBLIC
  $<BUILD_INTERFACE:${CORE_KAFKA_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET core-kafka-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)
