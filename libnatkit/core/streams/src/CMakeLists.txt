set(LIBNATKIT_CORE_STREAMS_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(LIBNATKIT_CORE_STREAMS_CXX_FILES
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/schemas/BasicMetaInfoSchema.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/schemas/Schema.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/BasicTopicInformation.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/RawStream.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/Stream.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/StreamInfo.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/StreamType.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/stream/StreamType.cpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/SerializationType.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/SerializationType.cpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/Message.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/TopicTranslator.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/Registry.hpp
  ${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/libnatkit/core/streams/registry/Registry.cpp
  )


add_library(libnatkit-core-streams-cxx SHARED ${LIBNATKIT_CORE_STREAMS_CXX_FILES})
set_target_properties(libnatkit-core-streams-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(libnatkit-core-streams-cxx PUBLIC
  libnatkit-util-cxx
  nlohmann
)
target_include_directories(libnatkit-core-streams-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_CORE_STREAMS_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET libnatkit-core-streams-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


if (BUILD_TESTING)
  set(LIBNATKIT_CORE_STREAMS_UNITTEST_C_FILES
    ${LIBNATKIT_CORE_STREAMS_ROOT}/unittest/c/libnatkit/core/streams/stream/StreamTest.cpp
    )

set(LIBNATKIT_CORE_STREAMS_UNITTEST_CXX_FILES
    ${LIBNATKIT_CORE_STREAMS_ROOT}/unittest/cxx/libnatkit/core/streams/stream/StreamTest.cpp
    ${LIBNATKIT_CORE_STREAMS_ROOT}/unittest/cxx/libnatkit/core/streams/registry/RegistryTest.cpp
    ${LIBNATKIT_CORE_STREAMS_ROOT}/unittest/cxx/libnatkit/core/streams/schemas/BasicMetaInfoSchemaTest.cpp
    )

  add_executable(libnatkit-kafka-core-unittest-cxx ${LIBNATKIT_CORE_STREAMS_UNITTEST_CXX_FILES})
  add_gtest(libnatkit-kafka-core-unittest-cxx)
  target_link_libraries(libnatkit-kafka-core-unittest-cxx PUBLIC
    libnatkit-core-streams-cxx
  )
  target_include_directories(libnatkit-kafka-core-unittest-cxx
    PUBLIC
    $<BUILD_INTERFACE:${LIBNATKIT_CORE_STREAMS_ROOT}/unittest/cxx/>
    $<INSTALL_INTERFACE:include>
  )

  set_target_properties(libnatkit-kafka-core-unittest-cxx PROPERTIES LINKER_LANGUAGE CXX)
endif()
