set(LIBNATKIT_TOOLS_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(list-topics ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListTopics.cpp)
target_link_libraries(list-topics PRIVATE
	kafkit-core-cxx
	)

add_executable(list-basic-streams ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListBasicStreamInfo.cpp)
target_link_libraries(list-basic-streams PRIVATE
	kafkit-core-cxx
	)
