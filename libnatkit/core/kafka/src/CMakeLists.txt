set(LIBNATKIT_CORE_KAFKA_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

set(LIBNATKIT_CORE_KAFKA_CXX_FILES
  #${LIBNATKIT_CORE_KAFKA_ROOT}/main/cxx/libnatkit/core/kafka/broker/BrokerManager.hpp
  ${LIBNATKIT_CORE_KAFKA_ROOT}/main/cxx/libnatkit/core/kafka/broker/BrokerMessagingQueue.cpp
  ${LIBNATKIT_CORE_KAFKA_ROOT}/main/cxx/libnatkit/core/kafka/broker/BrokerManagerImpl.cpp
  )

add_library(libnatkit-core-kafka-cxx SHARED ${LIBNATKIT_CORE_KAFKA_CXX_FILES})
set_target_properties(libnatkit-core-kafka-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libnatkit-core-kafka-cxx PUBLIC
  libnatkit-util-cxx
  libnatkit-core
  librdkafka-c
  librdkafka-cxx
)
target_include_directories(libnatkit-core-kafka-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_CORE_KAFKA_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
target_include_directories(libnatkit-core-kafka-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_PUBLIC_INCLUDE_DIR}>
  $<INSTALL_INTERFACE:include>
)
#target_include_directories(libnatkit-core-kafka-cxx
#  PUBLIC
#  $<BUILD_INTERFACE:${LIBRDKAFKA_INCLUDE_DIR}>
#  $<INSTALL_INTERFACE:include>
#)

set_property(TARGET libnatkit-core-kafka-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)

# --- Kafka C ABI shim -------------------------------------------------------
# liblibnatkit-kafka.so is the stable extern "C" boundary (libnatkit-kafka-abi.h)
# that non-C++ languages bind against, layered on top of the C++ transport in
# libnatkit-core-kafka-cxx. It is the Phase-4 companion to liblibnatkit-core.so:
# a separate .so so the lean, embedded/ESP-IDF libnatkit-core never takes on a
# librdkafka dependency. Python loads it via ctypes (LIBNATKIT_KAFKA_PATH).
add_library(libnatkit-kafka SHARED
  ${LIBNATKIT_CORE_KAFKA_ROOT}/main/cxx/libnatkit/core/kafka/abi/KafkaAbi.cpp
)
set_target_properties(libnatkit-kafka PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libnatkit-kafka PUBLIC
  libnatkit-core-kafka-cxx
)
target_include_directories(libnatkit-kafka
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_PUBLIC_INCLUDE_DIR}>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET libnatkit-kafka PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)
