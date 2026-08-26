set(LIBNATKIT_TOOLS_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(libnatkit-list-topics ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListTopics.cpp)
target_link_libraries(libnatkit-list-topics PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-list-basic-streams ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ListBasicStreamInfo.cpp)
target_link_libraries(libnatkit-list-basic-streams PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-produce-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ProduceTopic.cpp)
target_link_libraries(libnatkit-produce-topic PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-consume-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ConsumeTopic.cpp)
target_link_libraries(libnatkit-consume-topic PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-producer-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ProducerDemo.cpp)
target_link_libraries(libnatkit-producer-demo PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-delete-topic ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/DeleteTopic.cpp)
target_link_libraries(libnatkit-delete-topic PRIVATE
	libnatkit-core-kafka-cxx
)

add_executable(libnatkit-mosquitto-subscriber ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoSubscriber.cpp)
target_link_libraries(libnatkit-mosquitto-subscriber PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-mosquitto-publisher ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoPublisher.cpp)
target_link_libraries(libnatkit-mosquitto-publisher PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-mosquitto-subscriber-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoSubscriberDemo.cpp)
target_link_libraries(libnatkit-mosquitto-subscriber-demo PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-mosquitto-publisher-demo ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/MosquittoPublisherDemo.cpp)
target_link_libraries(libnatkit-mosquitto-publisher-demo PRIVATE
  libnatkit-core-mqtt-cxx
)

add_executable(libnatkit-kafka-mosquitto-bridge ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/KafkaMosquittoBridge.cpp)
target_link_libraries(libnatkit-kafka-mosquitto-bridge PRIVATE
  libnatkit-core-bridge-cxx
)

add_executable(libnatkit-natkit-menu ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/NatKitMenu.cpp)
target_link_libraries(libnatkit-natkit-menu PRIVATE
  libnatkit-core-kafka-cxx
)

if(BUILD_NATKIT_BACKEND)
  # Parquet export (GET /api/streams/{id}/parquet) needs Arrow's Parquet C++
  # writer. Optional so a build without libparquet-dev still works: when it is
  # absent the endpoint compiles to a clear 501 instead of failing the build.
  find_package(Parquet QUIET)
  if(NOT Parquet_FOUND)
    find_library(PARQUET_LIBRARY NAMES parquet)
    find_library(ARROW_LIBRARY NAMES arrow)
    find_path(PARQUET_INCLUDE_DIR NAMES parquet/api/writer.h)
    if(PARQUET_LIBRARY AND ARROW_LIBRARY AND PARQUET_INCLUDE_DIR)
      set(NATKIT_HAVE_PARQUET ON)
    endif()
  else()
    set(NATKIT_HAVE_PARQUET ON)
  endif()

  add_executable(libnatkit-natkit-backend
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/Auth.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/CohortExport.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/DeviceHealth.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/DeviceControls.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/StreamAliases.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/InProcessTransport.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/NatKitBackend.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ParquetExport.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/ReplaySource.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/RecordingState.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/StreamViewerWebSocket.cpp
  )

  if(NATKIT_HAVE_PARQUET)
    message(STATUS "natkit-backend: Parquet export ENABLED")
    target_compile_definitions(libnatkit-natkit-backend PRIVATE NATKIT_HAVE_PARQUET=1)
    if(Parquet_FOUND)
      target_link_libraries(libnatkit-natkit-backend PRIVATE Parquet::parquet_shared)
    else()
      target_include_directories(libnatkit-natkit-backend PRIVATE ${PARQUET_INCLUDE_DIR})
      target_link_libraries(libnatkit-natkit-backend PRIVATE ${PARQUET_LIBRARY} ${ARROW_LIBRARY})
    endif()
  else()
    message(STATUS "natkit-backend: Parquet export DISABLED (libparquet-dev not found)")
  endif()

  target_link_libraries(libnatkit-natkit-backend PRIVATE
    libnatkit-core-kafka-cxx
    nlohmann
    jsoncpp
    drogon
    z
    brotlidec
    sqlite3
    trantor
    ssl
    crypto
    pq
    mysqlclient
    uuid
    brotlienc
  )

  # c-ares, if the vendored trantor was built against it. trantor compiles its
  # AresResolver (async DNS) only when the c-ares headers are present at ITS
  # configure time, and then libtrantor.a carries undefined ares_* symbols that
  # this hardcoded link list must satisfy. Whether those headers exist is an
  # ambient property of the image, not something this project asked for --
  # installing libparquet-dev pulls libc-ares-dev in transitively (via
  # libgrpc/arrow-flight), which is exactly how the backend link started failing
  # when Parquet support was added. Linking it when present keeps the build
  # correct either way. Appended AFTER trantor above: static-archive symbol
  # resolution is order-sensitive.
  find_library(CARES_LIBRARY NAMES cares)
  if(CARES_LIBRARY)
    message(STATUS "natkit-backend: linking c-ares (${CARES_LIBRARY}) for trantor's AresResolver")
    target_link_libraries(libnatkit-natkit-backend PRIVATE ${CARES_LIBRARY})
  endif()
endif()

if(BUILD_TESTING)
  add_executable(libnatkit-tools-unittest-cxx
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/DeviceHealth.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/DeviceControls.cpp
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx/libnatkit/tools/InProcessTransport.cpp
    ${LIBNATKIT_TOOLS_ROOT}/unittest/cxx/libnatkit/tools/DeviceHealthTest.cpp
    ${LIBNATKIT_TOOLS_ROOT}/unittest/cxx/libnatkit/tools/DeviceControlsTest.cpp
    ${LIBNATKIT_TOOLS_ROOT}/unittest/cxx/libnatkit/tools/GraphTopicResolutionTest.cpp
    ${LIBNATKIT_TOOLS_ROOT}/unittest/cxx/libnatkit/tools/InProcessTransportTest.cpp
    ${LIBNATKIT_TOOLS_ROOT}/unittest/cxx/libnatkit/tools/GraphTransportPlanTest.cpp
  )
  add_gtest(libnatkit-tools-unittest-cxx)
  target_link_libraries(libnatkit-tools-unittest-cxx PRIVATE
    libnatkit-core-kafka-cxx
    nlohmann
  )
  target_include_directories(libnatkit-tools-unittest-cxx PRIVATE
    ${LIBNATKIT_TOOLS_ROOT}/main/cxx
  )
  set_target_properties(libnatkit-tools-unittest-cxx PROPERTIES LINKER_LANGUAGE CXX)
endif()
