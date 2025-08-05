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
