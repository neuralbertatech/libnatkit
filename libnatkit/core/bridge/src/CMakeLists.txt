set(LIBNATKIT_CORE_BRIDGE_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

set(LIBNATKIT_CORE_BRIDGE_CXX_FILES
  ${LIBNATKIT_CORE_BRIDGE_ROOT}/main/cxx/libnatkit/core/bridge/KafkaMosquittoBridge.hpp
  ${LIBNATKIT_CORE_BRIDGE_ROOT}/main/cxx/libnatkit/core/bridge/KafkaMosquittoBridge.cpp
  ) 

add_library(libnatkit-core-bridge-cxx SHARED ${LIBNATKIT_CORE_BRIDGE_CXX_FILES})
set_target_properties(libnatkit-core-bridge-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libnatkit-core-bridge-cxx PUBLIC
  libnatkit-core-mqtt-cxx
  libnatkit-core-kafka-cxx
)
target_include_directories(libnatkit-core-bridge-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_CORE_BRIDGE_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET libnatkit-core-bridge-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


