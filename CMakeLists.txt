cmake_minimum_required(VERSION 3.22)
project(libnatkit)

set(PROJECT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_ROOT}/cmake")
set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3 -O0")
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} -g3 -O0")

include(ExternalProject)
include(CTest)

set(LIBRDKAFKA_SOURCE_DIR "${PROJECT_ROOT}/third-party/librdkafka")
set(LIBRDKAFKA_BINARY_DIR "${CMAKE_BINARY_DIR}/librdkafka")
set(LIBRDKAFKA_INSTALL_DIR "${LIBRDKAFKA_BINARY_DIR}/_install")
ExternalProject_Add(librdkafka-module
  GIT_REPOSITORY "https://github.com/confluentinc/librdkafka"
  GIT_TAG v2.2.0
  UPDATE_COMMAND ""
  UPDATE_DISCONNECTED ON
  SOURCE_DIR "${LIBRDKAFKA_SOURCE_DIR}"
  BINARY_DIR "${LIBRDKAFKA_BINARY_DIR}"
  INSTALL_DIR "${LIBRDKAFKA_INSTALL_DIR}"
  CONFIGURE_COMMAND cd ${LIBRDKAFKA_SOURCE_DIR} && ./configure --install-deps --prefix=${LIBRDKAFKA_INSTALL_DIR}
  BUILD_COMMAND make -C ${LIBRDKAFKA_SOURCE_DIR}
  INSTALL_COMMAND make -C ${LIBRDKAFKA_SOURCE_DIR} install
  )
set(LIBRDKAFKA_INCLUDE_DIR "${LIBRDKAFKA_INSTALL_DIR}/include")
set(LIBRDKAFKA_LIBRARY_DIR "${LIBRDKAFKA_INSTALL_DIR}/lib")
set(LIBRDKAFKA_CXX_LIBRARY ${LIBRDKAFKA_LIBRARY_DIR}/librdkafka++.so)
set(LIBRDKAFKA_C_LIBRARY ${LIBRDKAFKA_LIBRARY_DIR}/librdkafka.so)
add_library(librdkafka-cpp SHARED IMPORTED)
add_library(librdkafka-c SHARED IMPORTED)
set_property(TARGET librdkafka-cpp PROPERTY IMPORTED_LOCATION ${LIBRDKAFKA_CXX_LIBRARY})
set_property(TARGET librdkafka-c PROPERTY IMPORTED_LOCATION ${LIBRDKAFKA_C_LIBRARY})


set(GOOGLETEST_SOURCE_DIR "${PROJECT_ROOT}/third-party/googletest")
set(GOOGLETEST_BINARY_DIR "${CMAKE_BINARY_DIR}/googletest")
set(GOOGLETEST_INSTALL_DIR "${GOOGLETEST_BINARY_DIR}/_install")
ExternalProject_Add(googletest-module
  GIT_REPOSITORY "https://github.com/google/googletest"
  GIT_TAG v1.14.0
  SOURCE_DIR "${GOOGLETEST_SOURCE_DIR}"
  BINARY_DIR "${GOOGLETEST_BINARY_DIR}"
  INSTALL_DIR "${GOOGLETEST_INSTALL_DIR}"
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${GOOGLETEST_INSTALL_DIR}
  )
set(GOOGLETEST_INCLUDE_DIR "${GOOGLETEST_INSTALL_DIR}/include")
set(GOOGLETEST_LIBRARY_DIR "${GOOGLETEST_INSTALL_DIR}/lib")
set(GOOGLETEST_LIBRARY ${GOOGLETEST_LIBRARY_DIR}/libgtest.a)
set(GOOGLETEST_LIBRARY_WITH_MAIN ${GOOGLETEST_LIBRARY_DIR}/libgtest_main.a)

add_library(libgtest STATIC IMPORTED)
add_library(libgtest-main STATIC IMPORTED)
#target_link_libraries(libgtest INTERFACE googletest-module)
#target_link_libraries(libgtest-main INTERFACE googletest-module)
#set_target_properties(libgtest PROPERTIES INTERFACE_LINK_LIBRARIES googletest-module)
#set_target_properties(libgtest-main PROPERTIES INTERFACE_LINK_LIBRARIES googletest-module)

set_property(TARGET libgtest PROPERTY IMPORTED_LOCATION ${GOOGLETEST_LIBRARY})
set_property(TARGET libgtest-main PROPERTY IMPORTED_LOCATION ${GOOGLETEST_LIBRARY_WITH_MAIN})
target_include_directories(libgtest INTERFACE ${GOOGLETEST_INCLUDE_DIR})
target_include_directories(libgtest-main INTERFACE ${GOOGLETEST_INCLUDE_DIR})

file(MAKE_DIRECTORY ${GOOGLETEST_INCLUDE_DIR})

if(BUILD_TESTING)
  enable_testing()
endif()


add_subdirectory(libnatkit)
