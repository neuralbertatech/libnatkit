add_executable(mytest test.cpp)
target_link_libraries(mytest PUBLIC librdkafka-cpp)
target_include_directories(mytest PRIVATE ${LIBRDKAFKA_INCLUDE_DIR})
set_target_properties(mytest PROPERTIES LINKER_LANGUAGE CXX)
#add_dependencies(mytest librdkafka-module)

add_subdirectory(kafkit/core/src)
