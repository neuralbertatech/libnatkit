set(CORE_STREAMS_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(CORE_STREAMS_CXX_FILES
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/schemas/BasicMetaInfoSchema.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/schemas/Schema.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/BasicTopicInformation.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/RawStream.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/Stream.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/StreamInfo.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/StreamType.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/StreamType.cpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/SerializationType.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/SerializationType.cpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/Message.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/TopicTranslator.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/Registry.hpp
  ${CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/Registry.cpp
  )


add_library(core-streams-cxx SHARED ${CORE_STREAMS_CXX_FILES})
set_target_properties(core-streams-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(core-streams-cxx PUBLIC
  libnatkit-util-cxx
  nlohmann
)
target_include_directories(core-streams-cxx
  PUBLIC
  $<BUILD_INTERFACE:${CORE_STREAMS_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET core-streams-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


if (BUILD_TESTING)
  set(CORE_STREAMS_UNITTEST_C_FILES
    ${CORE_STREAMS_ROOT}/unittest/c/libnatkit/core/streams/stream/StreamTest.cpp
    )

set(CORE_STREAMS_UNITTEST_CXX_FILES
    ${CORE_STREAMS_ROOT}/unittest/cxx/libnatkit/core/streams/stream/StreamTest.cpp
    ${CORE_STREAMS_ROOT}/unittest/cxx/libnatkit/core/streams/registry/RegistryTest.cpp
    ${CORE_STREAMS_ROOT}/unittest/cxx/libnatkit/core/streams/schemas/BasicMetaInfoSchemaTest.cpp
    )

  add_executable(kafka-core-unittest-cxx ${CORE_STREAMS_UNITTEST_CXX_FILES})
  add_gtest(kafka-core-unittest-cxx)
  target_link_libraries(kafka-core-unittest-cxx PUBLIC
    core-streams-cxx
  )
  target_include_directories(kafka-core-unittest-cxx
    PUBLIC
    $<BUILD_INTERFACE:${CORE_STREAMS_ROOT}/unittest/cxx/>
    $<INSTALL_INTERFACE:include>
  )

  set_target_properties(kafka-core-unittest-cxx PROPERTIES LINKER_LANGUAGE CXX)
endif()
