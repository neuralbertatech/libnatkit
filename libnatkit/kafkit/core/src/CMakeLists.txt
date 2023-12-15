set(KAFKIT_CORE_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(KAFKIT_CORE_C_FILES
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/Stream.h
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/Stream.c
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/StreamExtension.h
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/StreamId.h
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/StreamType.h
  )

set(KAFKIT_CORE_CXX_FILES
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/schemas/BasicMetaInfoSchema.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/schemas/Schema.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/stream/BasicTopicInformation.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/stream/RawStream.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/stream/Stream.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/stream/StreamInfo.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/stream/StreamType.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/stream/StreamType.cpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/broker/BrokerManager.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/broker/BrokerManagerImpl.cpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/registry/SerializationType.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/registry/SerializationType.cpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/registry/Message.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/registry/TopicTranslator.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/registry/Registry.hpp
  ${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/registry/Registry.cpp
  #${KAFKIT_CORE_ROOT}/main/cxx/libnatkit/kafkit/core/mqtt/test.cpp
  )


add_library(kafkit-core-cxx SHARED ${KAFKIT_CORE_CXX_FILES})
set_target_properties(kafkit-core-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(kafkit-core-cxx PUBLIC
  librdkafka-cxx
  libnatkit-util-cxx
  nlohmann
  #libmosquitto
)
target_include_directories(kafkit-core-cxx
  PUBLIC
  $<BUILD_INTERFACE:${KAFKIT_CORE_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET kafkit-core-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)


if (BUILD_TESTING)
  set(KAFKIT_CORE_UNITTEST_C_FILES
    ${KAFKIT_CORE_ROOT}/unittest/c/libnatkit/kafkit/core/stream/StreamTest.cpp
    )

set(KAFKIT_CORE_UNITTEST_CXX_FILES
    ${KAFKIT_CORE_ROOT}/unittest/cxx/libnatkit/kafkit/core/stream/StreamTest.cpp
    ${KAFKIT_CORE_ROOT}/unittest/cxx/libnatkit/kafkit/core/registry/RegistryTest.cpp
    ${KAFKIT_CORE_ROOT}/unittest/cxx/libnatkit/kafkit/core/schemas/BasicMetaInfoSchemaTest.cpp
    #${KAFKIT_CORE_ROOT}/unittest/cxx/libnatkit/kafkit/core/broker/BrokerManagerTest.cpp
    #${KAFKIT_CORE_ROOT}/unittest/cxx/libnatkit/kafkit/core/broker/BrokerManagerMock.hpp
    )

  add_executable(kafkit-core-unittest-cxx ${KAFKIT_CORE_UNITTEST_CXX_FILES})
  add_gtest(kafkit-core-unittest-cxx)
  target_link_libraries(kafkit-core-unittest-cxx PUBLIC
    kafkit-core-cxx
  )
  target_include_directories(kafkit-core-unittest-cxx
    PUBLIC
    $<BUILD_INTERFACE:${KAFKIT_CORE_ROOT}/unittest/cxx/>
    $<INSTALL_INTERFACE:include>
  )

  set_target_properties(kafkit-core-unittest-cxx PROPERTIES LINKER_LANGUAGE CXX)
endif()
