cmake_minimum_required(VERSION 3.20)
project(libnatkit VERSION 0.0
                  LANGUAGES C CXX)

set(PROJECT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(THIRD_PARTY_DIR ${PROJECT_ROOT}/third-party)
set(BUILD_ROOT ${CMAKE_CURRENT_BINARY_DIR})
set(SCRIPTS_DIR ${PROJECT_ROOT}/scripts)
set(INSTALL_DIR ${BUILD_ROOT})
set(LIBRARY_INSTALL_DIR ${INSTALL_DIR}/lib/Debug)
set(INCLUDE_INSTALL_DIR ${INSTALL_DIR}/include)
set(BINARY_INSTALL_DIR ${INSTALL_DIR}/bin)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_ROOT}/cmake)

if(WIN32)
  set (CMAKE_LIBRARY_OUTPUT_DIRECTORY ${LIBRARY_INSTALL_DIR})
  set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${BINARY_INSTALL_DIR})
  message("Detected OS is Windows")
elseif(UNIX)
  if (APPLE)
    message("Detected OS is MacOS")
  else()
    message("Detected OS is Linux")
  endif()
else()
  message(WARNING "Unsupported OS!")
endif()

if(MSVC)
  message("------------------ Compiler is MSVC ------------------")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /D__msvc_cplusplus /std:c++20")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /std:c11")
  set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
  set(BUILD_SHARED_LIBS ON)
else()
message("------------------ Compiler is Something Else ------------------")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g3 -O0 -std=c++20")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g3 -O0")
  if (NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--disable-new-dtags")
  endif()
endif()
set(CMAKE_VERBOSE_MAKEFILE ON)


include(ExternalProject)

#find_program(MEMORYCHECK_COMMAND valgrind)
#set(MEMORYCHECK_COMMAND_OPTIONS "--leak-check=full --error-exitcode=1")
include(CTest)

if(UNIX)
  set(LIBRARY_PREFIX "lib")
  if(APPLE)
    set(DYNAMIC_LIBRARY_EXTENSION ".dylib")
    set(STATIC_LIBRARY_EXTENSION ".a")
  else()
    set(DYNAMIC_LIBRARY_EXTENSION ".so")
    set(STATIC_LIBRARY_EXTENSION ".a")
  endif()
endif(UNIX)

if(WIN32)
  set(DYNAMIC_LIBRARY_EXTENSION ".dll")
  set(STATIC_LIBRARY_EXTENSION ".lib")
  set(LIBRARY_PREFIX "")
endif(WIN32)


if(WIN32)
  set(LIBRDKAFKA_SOURCE_DIR "${BUILD_ROOT}/third-party/librdkafka")
	set(LIBRDKAFKA_BINARY_DIR "${CMAKE_BINARY_DIR}/archive/librdkafka")
  set(LIBRDKAFKA_INCLUDE_DIR ${INCLUDE_INSTALL_DIR})
  set(LIBRDKAFKA_INCLUDE_INSTALL_DIR ${INCLUDE_INSTALL_DIR}/librdkafka)
  set(LIBRDKAFKA_LIBRARY_DIR ${LIBRARY_INSTALL_DIR})
	cmake_path(CONVERT ${LIBRDKAFKA_SOURCE_DIR} TO_NATIVE_PATH_LIST WIN_LIBRDKAFKA_SOURCE_DIR)
	cmake_path(CONVERT ${LIBRDKAFKA_SOURCE_DIR}/librdkafka.redist.2.2.0 TO_NATIVE_PATH_LIST WIN_LIBRDKAFKA_DOWNLOADED_SOURCE_DIR)
	cmake_path(CONVERT ${LIBRDKAFKA_INCLUDE_INSTALL_DIR} TO_NATIVE_PATH_LIST WIN_LIBRDKAFKA_INCLUDE_INSTALL_DIR)
  cmake_path(CONVERT ${LIBRDKAFKA_LIBRARY_DIR} TO_NATIVE_PATH_LIST WIN_LIBRDKAFKA_LIBRARY_DIR)
  cmake_path(CONVERT ${LIBRDKAFKA_BINARY_DIR} TO_NATIVE_PATH_LIST WIN_LIBRDKAFKA_BINARY_DIR)
	set(LIBRDKAFKA_CXX_LIBRARY "${LIBRDKAFKA_LIBRARY_DIR}/librdkafkacpp${STATIC_LIBRARY_EXTENSION}")
	set(LIBRDKAFKA_C_LIBRARY "${LIBRDKAFKA_LIBRARY_DIR}/librdkafka${STATIC_LIBRARY_EXTENSION}")

  set(LIBRDKAFKA_INSTALL_COMMAND "${SCRIPTS_DIR}/install_librdkafka.ps1 ${WIN_LIBRDKAFKA_BINARY_DIR}")
  set(LIBRDKAFKA_INSTALL_ARGS "${WIN_LIBRDKAFKA_LIBRARY_DIR} ${WIN_LIBRDKAFKA_INCLUDE_INSTALL_DIR}")
  message("Installing librdkafka with the following command:")
  message("    ${LIBRDKAFKA_INSTALL_COMMAND} ${LIBRDKAFKA_INSTALL_ARGS}")
  execute_process(COMMAND powershell ${LIBRDKAFKA_INSTALL_COMMAND} ${LIBRDKAFKA_INSTALL_ARGS}
                  RESULT_VARIABLE LIBRDKAFKA_INSTALL_RESULT
                  OUTPUT_VARIABLE LIBRDKAFKA_INSTALL_RESULT)
  message("WORLD        -                 ---------- ${LIBRDKAFKA_INSTALL_RESULT} ${}")
  add_library(librdkafka-cxx STATIC IMPORTED)
  add_library(librdkafka-c STATIC IMPORTED)

else()
  set(LIBRDKAFKA_SOURCE_DIR "${PROJECT_ROOT}/third-party/librdkafka")
	set(LIBRDKAFKA_BINARY_DIR "${CMAKE_BINARY_DIR}/librdkafka")
  set(LIBRDKAFKA_BUILD_DIR "${CMAKE_BINARY_DIR}/third-party/librdkafka")
  set(LIBRDKAFKA_INSTALL_DIR ${INSTALL_DIR})
	set(LIBRDKAFKA_INCLUDE_DIR "${LIBRDKAFKA_INSTALL_DIR}/include")
	set(LIBRDKAFKA_LIBRARY_DIR "${LIBRDKAFKA_INSTALL_DIR}/lib")
	set(LIBRDKAFKA_CXX_LIBRARY "${LIBRDKAFKA_LIBRARY_DIR}/${LIBRARY_PREFIX}rdkafka++${DYNAMIC_LIBRARY_EXTENSION}")
	set(LIBRDKAFKA_C_LIBRARY "${LIBRDKAFKA_LIBRARY_DIR}/${LIBRARY_PREFIX}rdkafka${DYNAMIC_LIBRARY_EXTENSION}")

  execute_process(COMMAND ${SCRIPTS_DIR}/install_librdkafka.sh ${LIBRDKAFKA_SOURCE_DIR} ${LIBRDKAFKA_BUILD_DIR} ${LIBRDKAFKA_INSTALL_DIR})

  add_library(librdkafka-cxx SHARED IMPORTED)
  add_library(librdkafka-c SHARED IMPORTED)
endif()
message("librdkafka c++ is located at ${LIBRDKAFKA_CXX_LIBRARY}")
message("librdkafka headers are located at ${LIBRDKAFKA_INCLUDE_DIR}")
set_target_properties(librdkafka-cxx PROPERTIES
  IMPORTED_LOCATION ${LIBRDKAFKA_CXX_LIBRARY}
  INTERFACE_INCLUDE_DIRECTORIES ${LIBRDKAFKA_INCLUDE_DIR}
  )
  message("librdkafka c is located at ${LIBRDKAFKA_C_LIBRARY}")
set_target_properties(librdkafka-c PROPERTIES
  IMPORTED_LOCATION ${LIBRDKAFKA_C_LIBRARY}
  INTERFACE_INCLUDE_DIRECTORIES ${LIBRDKAFKA_INCLUDE_DIR}
  )
if (WIN32)
	set_target_properties(librdkafka-cxx PROPERTIES
	  IMPORTED_IMPLIB ${LIBRDKAFKA_CXX_LIBRARY}
	  IMPORTED_IMPLIB_DEBUG ${LIBRDKAFKA_CXX_LIBRARY}
	)
	set_target_properties(librdkafka-c PROPERTIES
	  IMPORTED_IMPLIB ${LIBRDKAFKA_C_LIBRARY}
	  IMPORTED_IMPLIB_DEBUG ${LIBRDKAFKA_C_LIBRARY}
	)
endif()

add_library(nlohmann INTERFACE)
target_include_directories(nlohmann INTERFACE ${THIRD_PARTY_DIR}/nlohmann/include)


if(BUILD_TESTING)
  enable_testing()
  include(AddGoogleTest)
endif()



add_subdirectory(libnatkit)
