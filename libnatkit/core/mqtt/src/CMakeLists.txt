set(LIBNATKIT_CORE_MQTT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

set(LIBNATKIT_CORE_MQTT_CXX_FILES
  #${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoBroker.hpp
  ${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoBrokerImpl.cpp
  #${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoClient.hpp
  ${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoClient.cpp
  #${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoPublisher.hpp
  ${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoPublisher.cpp
  ) 

add_library(libnatkit-core-mqtt-cxx SHARED ${LIBNATKIT_CORE_MQTT_CXX_FILES})
set_target_properties(libnatkit-core-mqtt-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libnatkit-core-mqtt-cxx PUBLIC
  libnatkit-util-cxx
  libmosquitto
)
target_include_directories(libnatkit-core-mqtt-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_CORE_MQTT_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
target_include_directories(libnatkit-core-mqtt-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_PUBLIC_INCLUDE_DIR}>
  $<INSTALL_INTERFACE:include>
)

set_property(TARGET libnatkit-core-mqtt-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


if(BUILD_TESTING)
  set(LIBNATKIT_CORE_MQTT_UNITTEST_CXX_FILES
  ${LIBNATKIT_CORE_MQTT_ROOT}/unittest/cxx/libnatkit/core/mqtt/MosquittoHelloWorld.cpp
  )

  add_executable(libnatkit-core-mqtt-unittest-cxx ${LIBNATKIT_CORE_MQTT_UNITTEST_CXX_FILES})
  add_gtest(libnatkit-core-mqtt-unittest-cxx)
  target_link_libraries(libnatkit-core-mqtt-unittest-cxx PUBLIC
    libnatkit-core-mqtt-cxx
  )

  set_target_properties(libnatkit-core-mqtt-unittest-cxx PROPERTIES LINKER_LANGUAGE CXX)
endif()
