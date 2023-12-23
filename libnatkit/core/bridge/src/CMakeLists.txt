set(CORE_BRIDGE_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

set(CORE_BRIDGE_CXX_FILES
  ${CORE_BRIDGE_ROOT}/main/cxx/libnatkit/core/bridge/KafkaToMosquittoBridge.hpp
  ${CORE_BRIDGE_ROOT}/main/cxx/libnatkit/core/bridge/KafkaToMosquittoBridge.cpp
  ) 

add_library(core-bridge-cxx SHARED ${CORE_BRIDGE_CXX_FILES})
set_target_properties(core-bridge-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(core-bridge-cxx PUBLIC
  core-mqtt-cxx
  core-kafka-cxx
)
target_include_directories(core-bridge-cxx
  PUBLIC
  $<BUILD_INTERFACE:${CORE_BRIDGE_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET core-bridge-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


