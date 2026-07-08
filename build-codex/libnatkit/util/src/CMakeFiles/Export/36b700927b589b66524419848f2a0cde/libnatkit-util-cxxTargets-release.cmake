#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "nat::util::libnatkit-util-cxx" for configuration "Release"
set_property(TARGET nat::util::libnatkit-util-cxx APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(nat::util::libnatkit-util-cxx PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib64/liblibnatkit-util-cxx.so"
  IMPORTED_SONAME_RELEASE "liblibnatkit-util-cxx.so"
  )

list(APPEND _cmake_import_check_targets nat::util::libnatkit-util-cxx )
list(APPEND _cmake_import_check_files_for_nat::util::libnatkit-util-cxx "${_IMPORT_PREFIX}/lib64/liblibnatkit-util-cxx.so" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
