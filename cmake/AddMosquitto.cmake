#
if(CMAKE_VERSION VERSION_LESS 3.11)
  messagae(FATAL_ERROR "CMake version 3.11 or higher is required!")
else()
    message("----------------- Setting up Mosquitto -------------------------")
    
    include(FetchContent)
    FetchContent_Declare(natmosquitto
      SOURCE_DIR          ${THIRD_PARTY_DIR}/mosquitto
    )
    FetchContent_GetProperties(natmosquitto)
    if(NOT natmosquitto_POPULATED)
        message("Mosquitto is not populated ------------------")
        FetchContent_Populate(natmosquitto)
        #FetchContent_MakeAvailable(nat-mosquitto)
        #set_target_properties(mosquitto PROPERTIES DOCUMENTATION OFF WITH_DLT ON WITH_CJSON OFF)
        set(DOCUMENTATION OFF)
        set(WITH_CJSON OFF)
        set(WITH_DOCS OFF)
        if (WIN32)
          set(WITH_THREADING OFF)
        endif()
        set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1 CACHE BOOL "")
        add_subdirectory(${natmosquitto_SOURCE_DIR} ${natmosquitto_BINARY_DIR} EXCLUDE_FROM_ALL)
        unset(CMAKE_SUPPRESS_DEVELOPER_WARNINGS)
    else()
        message("Mosquitto is already populated ------------------")
    endif()

    message("----------------- Finished Setting up Mosquitto -------------------------")
endif()

target_include_directories(libmosquitto INTERFACE ${natmosquitto_SOURCE_DIR}/include)

set_target_properties(libmosquitto
    PROPERTIES FOLDER "Extern")
