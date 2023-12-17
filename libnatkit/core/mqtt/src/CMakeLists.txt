set(CORE_MQTT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

set(CORE_MQTT_CXX_FILES
  ${CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoBroker.hpp
  ${CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoBrokerImpl.cpp
  ${CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoClient.hpp
  ${CORE_MQTT_ROOT}/main/cxx/libnatkit/core/mqtt/MosquittoPublisher.hpp
  ) 

add_library(core-mqtt-cxx SHARED ${CORE_MQTT_CXX_FILES})
set_target_properties(core-mqtt-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(core-mqtt-cxx PUBLIC
  libnatkit-util-cxx
  libmosquitto
)
target_include_directories(core-mqtt-cxx
  PUBLIC
  $<BUILD_INTERFACE:${CORE_MQTT_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET core-mqtt-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


