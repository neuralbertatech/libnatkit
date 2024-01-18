set(LIBNATKIT_UTIL_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(LIBNATKIT_UTIL_C_FILES
  ${LIBNATKIT_UTIL_ROOT}/main/c/libnatkit/util/Util.c
  ${LIBNATKIT_UTIL_ROOT}/main/c/libnatkit/util/String.h
  ${LIBNATKIT_UTIL_ROOT}/main/c/libnatkit/util/Bool.h
  ${LIBNATKIT_UTIL_ROOT}/main/c/libnatkit/util/Result.h
  ${LIBNATKIT_UTIL_ROOT}/main/c/libnatkit/util/Result.c
  )

set(LIBNATKIT_UTIL_CXX_FILES
  ${LIBNATKIT_UTIL_ROOT}/main/cxx/libnatkit/util/Strings.hpp
  ${LIBNATKIT_UTIL_ROOT}/main/cxx/libnatkit/util/Strings.cpp
  ${LIBNATKIT_UTIL_ROOT}/main/cxx/libnatkit/util/Vectors.hpp
  ${LIBNATKIT_UTIL_ROOT}/main/cxx/libnatkit/util/Casting.hpp
  )

#add_library(libnatkit-util-c SHARED ${LIBNATKIT_UTIL_C_FILES})
#set_target_properties(libnatkit-util-c PROPERTIES LINKER_LANGUAGE C)
#target_include_directories(libnatkit-util-c
#  PUBLIC
#  $<BUILD_INTERFACE:${LIBNATKIT_UTIL_ROOT}/main/c/>
#  $<INSTALL_INTERFACE:include>
#)
#set_property(TARGET libnatkit-util-c PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)

if(LIBNATKIT_ESP_BUILD)
  add_library(libnatkit-util-cxx STATIC ${LIBNATKIT_UTIL_CXX_FILES})
else()
  add_library(libnatkit-util-cxx SHARED ${LIBNATKIT_UTIL_CXX_FILES})
endif()
set_target_properties(libnatkit-util-cxx PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(libnatkit-util-cxx
  PUBLIC
  $<BUILD_INTERFACE:${LIBNATKIT_UTIL_ROOT}/main/cxx/>
  $<INSTALL_INTERFACE:include>
)
set_property(TARGET libnatkit-util-cxx PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS true)

if (BUILD_TESTING)
  #set(LIBNATKIT_UTIL_UNITTEST_C_FILES
  #  ${LIBNATKIT_UTIL_ROOT}/unittest/c/libnatkit/util/ResultTest.cpp
  #  ${LIBNATKIT_UTIL_ROOT}/unittest/c/libnatkit/util/StringTest.cpp
  #  )

  #add_executable(libnatkit-util-unittest-c ${LIBNATKIT_UTIL_UNITTEST_C_FILES})
  #add_gtest(libnatkit-util-unittest-c)
  #target_link_libraries(libnatkit-util-unittest-c PUBLIC
  #  libnatkit-util-c
  #)

  #set_target_properties(libnatkit-util-unittest-c PROPERTIES LINKER_LANGUAGE CXX)


  set(LIBNATKIT_UTIL_UNITTEST_CXX_FILES
    ${LIBNATKIT_UTIL_ROOT}/unittest/cxx/libnatkit/util/StringsTest.cpp
    )

  add_executable(libnatkit-util-unittest-cxx ${LIBNATKIT_UTIL_UNITTEST_CXX_FILES})
  add_gtest(libnatkit-util-unittest-cxx)
  target_link_libraries(libnatkit-util-unittest-cxx PUBLIC
    libnatkit-util-cxx
  )

  set_target_properties(libnatkit-util-unittest-cxx PROPERTIES LINKER_LANGUAGE CXX)

endif()
