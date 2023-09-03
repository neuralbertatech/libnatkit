set(KAFKIT_CORE_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(KAFKIT_CORE_C_FILES
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/Stream.h
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/StreamExtension.h
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/StreamId.h
  ${KAFKIT_CORE_ROOT}/main/c/libnatkit/kafkit/core/stream/StreamType.h
  )

add_library(kafkit-core-c ${KAFKIT_CORE_C_FILES})
set_target_properties(kafkit-core-c PROPERTIES LINKER_LANGUAGE C)
target_link_libraries(kafkit-core-c PUBLIC librdkafka-c)
target_include_directories(kafkit-core-c PUBLIC ${KAFKIT_CORE_ROOT}/main/c/)

if (BUILD_TESTING)
  set(KAFKIT_CORE_UNITTEST_C_FILES
    ${KAFKIT_CORE_ROOT}/unittest/c/libnatkit/kafkit/core/stream/StreamTest.cpp
    )

  add_executable(kafkit-core-unittest-c ${KAFKIT_CORE_UNITTEST_C_FILES})
  target_link_libraries(kafkit-core-unittest-c PUBLIC
    kafkit-core-c
    libgtest
    libgtest-main
  )
  set_target_properties(kafkit-core-unittest-c PROPERTIES LINKER_LANGUAGE CXX)
  add_test(NAME "Kafkit-Core"
           COMMAND kafkit-core-unittest-c)
endif()
