cmake_minimum_required(VERSION 3.16)
project(virus_executor_service LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(MEMRPC_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
set(MEMRPC_MODULE_ROOT ${MEMRPC_ROOT}/memrpc)
include(${MEMRPC_ROOT}/cmake/ProjectOptions.cmake)
memrpc_configure_project_options()

function(ves_maybe_disable_clang_tidy target)
  if (MEMRPC_ENABLE_CLANG_TIDY AND MEMRPC_CLANG_TIDY_MAINLINE_ONLY)
    memrpc_disable_clang_tidy_for_target(${target})
  endif()
endfunction()

set(VIRUS_EXECUTOR_SERVICE_DEFAULT_TEST_TIMEOUT_SEC 30 CACHE STRING
    "Default timeout in seconds for virus_executor_service tests")
set(VIRUS_EXECUTOR_SERVICE_DT_TEST_TIMEOUT_SEC 90 CACHE STRING
    "Timeout in seconds for virus_executor_service DT tests" FORCE)
set(VIRUS_EXECUTOR_SERVICE_DT_TEST_TIMEOUT_SEC 90)
set(VIRUS_EXECUTOR_SERVICE_STRESS_TEST_TIMEOUT_SEC 60 CACHE STRING
    "Timeout in seconds for virus_executor_service stress/perf tests")

if (NOT TARGET memrpc_core)
  add_library(memrpc_core
      ${MEMRPC_MODULE_ROOT}/src/client/rpc_client.cpp
      ${MEMRPC_MODULE_ROOT}/src/server/rpc_server.cpp
      ${MEMRPC_MODULE_ROOT}/src/bootstrap/shared_memory_session_host.cpp
      ${MEMRPC_MODULE_ROOT}/src/core/byte_reader.cpp
      ${MEMRPC_MODULE_ROOT}/src/core/byte_writer.cpp
      ${MEMRPC_MODULE_ROOT}/src/core/file_payload.cpp
      ${MEMRPC_MODULE_ROOT}/src/core/log.cpp
      ${MEMRPC_MODULE_ROOT}/src/core/session.cpp
  )
  target_include_directories(memrpc_core PUBLIC
      ${MEMRPC_MODULE_ROOT}/include
  )
  target_include_directories(memrpc_core PRIVATE
      ${MEMRPC_MODULE_ROOT}/src
      ${MEMRPC_ROOT}/include
  )
  target_compile_features(memrpc_core PUBLIC cxx_std_17)
  target_link_libraries(memrpc_core PUBLIC pthread rt)
endif()

if (NOT TARGET memrpc)
  add_library(memrpc ALIAS memrpc_core)
endif()

if (NOT TARGET memrpc_test_support)
  add_library(memrpc_test_support
      ${MEMRPC_MODULE_ROOT}/tests/support/dev_bootstrap.cpp
  )
  target_include_directories(memrpc_test_support PUBLIC
      ${MEMRPC_MODULE_ROOT}/include
      ${MEMRPC_MODULE_ROOT}/tests/support/include
  )
  target_include_directories(memrpc_test_support PRIVATE
      ${MEMRPC_MODULE_ROOT}/src
      ${MEMRPC_ROOT}/include
  )
  target_compile_features(memrpc_test_support PUBLIC cxx_std_17)
  target_link_libraries(memrpc_test_support PUBLIC memrpc_core)
endif()

add_library(ohos_sa_mock
    ${MEMRPC_ROOT}/third_party/ohos_sa_mock/src/system_ability_manager.cpp
    ${MEMRPC_ROOT}/third_party/ohos_sa_mock/src/iremote_object.cpp
    ${MEMRPC_ROOT}/third_party/ohos_sa_mock/src/iremote_broker_registry.cpp
    ${MEMRPC_ROOT}/third_party/ohos_sa_mock/src/scm_rights.cpp
    ${MEMRPC_ROOT}/third_party/ohos_sa_mock/src/mock_service_socket.cpp
)
ves_maybe_disable_clang_tidy(ohos_sa_mock)
target_include_directories(ohos_sa_mock PUBLIC
    ${MEMRPC_ROOT}/third_party/ohos_sa_mock/include
)
target_compile_features(ohos_sa_mock PUBLIC cxx_std_17)

set(VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${MEMRPC_ROOT}/include
)

add_library(virus_executor_service_transport
    src/transport/registry_protocol.cpp
    src/transport/registry_server.cpp
    src/transport/registry_client.cpp
    src/transport/registry_backend.cpp
    src/transport/ves_control_proxy.cpp
)
target_include_directories(virus_executor_service_transport PUBLIC
    ${VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS}
)
target_link_libraries(virus_executor_service_transport PUBLIC memrpc_core ohos_sa_mock)
target_compile_features(virus_executor_service_transport PUBLIC cxx_std_17)

add_library(virus_executor_service_ves
    src/ves/ves_sample_rules.cpp
)
target_include_directories(virus_executor_service_ves PUBLIC
    ${VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS}
)
target_link_libraries(virus_executor_service_ves PUBLIC memrpc_core)
target_compile_features(virus_executor_service_ves PUBLIC cxx_std_17)

add_library(virus_executor_service_client_lib
    src/client/ves_client.cpp
)
target_include_directories(virus_executor_service_client_lib PUBLIC
    ${VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS}
)
target_link_libraries(virus_executor_service_client_lib PUBLIC
    memrpc_core
    virus_executor_service_transport
)
target_compile_features(virus_executor_service_client_lib PUBLIC cxx_std_17)

add_library(virus_executor_service_testkit
    src/testkit/testkit_service.cpp
    src/testkit/testkit_async_client.cpp
    src/testkit/testkit_client.cpp
)
ves_maybe_disable_clang_tidy(virus_executor_service_testkit)
target_include_directories(virus_executor_service_testkit PUBLIC
    ${VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS}
)
target_link_libraries(virus_executor_service_testkit PUBLIC memrpc_core)
target_compile_features(virus_executor_service_testkit PUBLIC cxx_std_17)

add_library(virus_executor_service_service
    src/service/virus_executor_service.cpp
    src/service/ves_session_service.cpp
    src/service/ves_engine_service.cpp
)
target_include_directories(virus_executor_service_service PUBLIC
    ${VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS}
)
target_link_libraries(virus_executor_service_service PUBLIC
    memrpc_core
    virus_executor_service_ves
    virus_executor_service_testkit
    ohos_sa_mock
)
target_compile_features(virus_executor_service_service PUBLIC cxx_std_17)

add_library(virus_executor_service_lib INTERFACE)
target_include_directories(virus_executor_service_lib INTERFACE
    ${VIRUS_EXECUTOR_SERVICE_PUBLIC_INCLUDE_DIRS}
)
target_link_libraries(virus_executor_service_lib INTERFACE
    virus_executor_service_transport
    virus_executor_service_ves
    virus_executor_service_client_lib
    virus_executor_service_testkit
    virus_executor_service_service
)

add_executable(virus_executor_service_supervisor src/app/ves_supervisor_main.cpp)
target_link_libraries(virus_executor_service_supervisor PRIVATE virus_executor_service_transport)

add_executable(virus_executor_service src/app/virus_executor_service_main.cpp)
set_target_properties(virus_executor_service PROPERTIES OUTPUT_NAME "VirusExecutorService")
target_link_libraries(virus_executor_service PRIVATE
    virus_executor_service_transport
    virus_executor_service_service
)

add_executable(virus_executor_service_client src/app/ves_client_main.cpp)
target_link_libraries(virus_executor_service_client PRIVATE
    virus_executor_service_transport
    virus_executor_service_client_lib
)

add_executable(virus_executor_service_stress_client src/app/ves_stress_client_main.cpp)
target_link_libraries(virus_executor_service_stress_client PRIVATE
    virus_executor_service_transport
    virus_executor_service_client_lib
    virus_executor_service_ves
)

option(VIRUS_EXECUTOR_SERVICE_ENABLE_TESTS "Build virus_executor_service tests" OFF)
option(VIRUS_EXECUTOR_SERVICE_ENABLE_FUZZ "Build virus_executor_service fuzz targets" OFF)

if (VIRUS_EXECUTOR_SERVICE_ENABLE_TESTS)
  enable_testing()
  find_package(GTest REQUIRED)
  target_link_libraries(virus_executor_service_lib INTERFACE memrpc_test_support)

  add_executable(virus_executor_service_session_service_test
      tests/unit/ves/ves_session_service_test.cpp)
  target_link_libraries(virus_executor_service_session_service_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_session_service_test
      COMMAND virus_executor_service_session_service_test)

  add_executable(virus_executor_service_transport_selection_test
      tests/unit/ves/ves_transport_selection_test.cpp)
  target_link_libraries(virus_executor_service_transport_selection_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_transport_selection_test
      COMMAND virus_executor_service_transport_selection_test)

  add_executable(virus_executor_service_heartbeat_test
      tests/unit/ves/ves_heartbeat_test.cpp)
  target_link_libraries(virus_executor_service_heartbeat_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_heartbeat_test
      COMMAND virus_executor_service_heartbeat_test)

  add_executable(virus_executor_service_recovery_reason_test
      tests/unit/ves/ves_recovery_reason_test.cpp)
  target_link_libraries(virus_executor_service_recovery_reason_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_recovery_reason_test
      COMMAND virus_executor_service_recovery_reason_test)

  add_executable(virus_executor_service_codec_test
      tests/unit/ves/ves_codec_test.cpp)
  target_link_libraries(virus_executor_service_codec_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_codec_test
      COMMAND virus_executor_service_codec_test)

  add_executable(virus_executor_service_testkit_headers_test
      tests/unit/testkit/testkit_headers_test.cpp)
  target_link_libraries(virus_executor_service_testkit_headers_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_headers_test
      COMMAND virus_executor_service_testkit_headers_test)

  add_executable(virus_executor_service_testkit_codec_test
      tests/unit/testkit/testkit_codec_test.cpp)
  target_link_libraries(virus_executor_service_testkit_codec_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_codec_test
      COMMAND virus_executor_service_testkit_codec_test)

  add_executable(virus_executor_service_testkit_service_test
      tests/unit/testkit/testkit_service_test.cpp)
  target_link_libraries(virus_executor_service_testkit_service_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_service_test
      COMMAND virus_executor_service_testkit_service_test)

  add_executable(virus_executor_service_testkit_baseline_test
      tests/unit/testkit/testkit_baseline_test.cpp)
  target_link_libraries(virus_executor_service_testkit_baseline_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_baseline_test
      COMMAND virus_executor_service_testkit_baseline_test)

  add_executable(virus_executor_service_testkit_client_test
      tests/unit/testkit/testkit_client_test.cpp)
  target_link_libraries(virus_executor_service_testkit_client_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_client_test
      COMMAND virus_executor_service_testkit_client_test)

  add_executable(virus_executor_service_testkit_dfx_test
      tests/unit/testkit/testkit_dfx_test.cpp
      tests/unit/testkit/testkit_resilient_invoker.cpp
  )
  target_link_libraries(virus_executor_service_testkit_dfx_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_dfx_test
      COMMAND virus_executor_service_testkit_dfx_test)

  add_executable(virus_executor_service_testkit_backpressure_test
      tests/unit/testkit/testkit_backpressure_test.cpp)
  target_link_libraries(virus_executor_service_testkit_backpressure_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_backpressure_test
      COMMAND virus_executor_service_testkit_backpressure_test)

  add_executable(virus_executor_service_testkit_async_pipeline_test
      tests/unit/testkit/testkit_async_pipeline_test.cpp)
  target_link_libraries(virus_executor_service_testkit_async_pipeline_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_async_pipeline_test
      COMMAND virus_executor_service_testkit_async_pipeline_test)
  set_tests_properties(virus_executor_service_testkit_async_pipeline_test PROPERTIES
      RUN_SERIAL TRUE)

  add_executable(virus_executor_service_testkit_latency_test
      tests/unit/testkit/testkit_latency_test.cpp)
  target_link_libraries(virus_executor_service_testkit_latency_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_latency_test
      COMMAND virus_executor_service_testkit_latency_test)

  add_executable(virus_executor_service_testkit_throughput_test
      tests/stress/testkit_throughput_test.cpp)
  target_link_libraries(virus_executor_service_testkit_throughput_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_throughput_test
      COMMAND virus_executor_service_testkit_throughput_test)
  set_tests_properties(virus_executor_service_testkit_throughput_test PROPERTIES
      ENVIRONMENT
        "MEMRPC_PERF_BASELINE_PATH=${CMAKE_CURRENT_SOURCE_DIR}/perf_baselines/testkit_throughput.baseline"
  )

  add_executable(virus_executor_service_testkit_dt_stability_test
      tests/dt/testkit_dt_stability_test.cpp)
  target_link_libraries(virus_executor_service_testkit_dt_stability_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_dt_stability_test
      COMMAND virus_executor_service_testkit_dt_stability_test)
  set_tests_properties(virus_executor_service_testkit_dt_stability_test PROPERTIES
      LABELS "dt_stability")

  add_executable(virus_executor_service_testkit_dt_perf_test
      tests/dt/testkit_dt_perf_test.cpp)
  target_link_libraries(virus_executor_service_testkit_dt_perf_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_dt_perf_test
      COMMAND virus_executor_service_testkit_dt_perf_test)
  set_tests_properties(virus_executor_service_testkit_dt_perf_test PROPERTIES
      LABELS "dt_perf"
      RUN_SERIAL TRUE
      ENVIRONMENT
        "MEMRPC_DT_BASELINE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/perf_baselines"
  )

  add_library(virus_executor_service_testkit_stress_config
      tests/stress/testkit_stress_config.cpp)
  ves_maybe_disable_clang_tidy(virus_executor_service_testkit_stress_config)
  target_link_libraries(virus_executor_service_testkit_stress_config PUBLIC
      virus_executor_service_lib)
  target_compile_features(virus_executor_service_testkit_stress_config PUBLIC cxx_std_17)

  add_executable(virus_executor_service_testkit_stress_config_test
      tests/stress/testkit_stress_config_test.cpp)
  target_link_libraries(virus_executor_service_testkit_stress_config_test PRIVATE
      virus_executor_service_testkit_stress_config
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_testkit_stress_config_test
      COMMAND virus_executor_service_testkit_stress_config_test)
  set_tests_properties(virus_executor_service_testkit_stress_config_test PROPERTIES
      LABELS "stress")

  add_executable(virus_executor_service_testkit_stress_runner
      tests/stress/testkit_stress_runner.cpp)
  target_link_libraries(virus_executor_service_testkit_stress_runner PRIVATE
      virus_executor_service_testkit_stress_config
      virus_executor_service_lib
  )
  target_compile_features(virus_executor_service_testkit_stress_runner PRIVATE cxx_std_17)

  add_test(NAME virus_executor_service_testkit_stress_smoke
      COMMAND virus_executor_service_testkit_stress_runner)
  set_tests_properties(virus_executor_service_testkit_stress_smoke PROPERTIES
      LABELS "stress"
      ENVIRONMENT
        "MEMRPC_STRESS_DURATION_SEC=1;MEMRPC_STRESS_WARMUP_SEC=0;MEMRPC_STRESS_THREADS=1;MEMRPC_STRESS_ECHO_WEIGHT=100;MEMRPC_STRESS_ADD_WEIGHT=0;MEMRPC_STRESS_SLEEP_WEIGHT=0;MEMRPC_STRESS_PAYLOAD_SIZES=0,16;MEMRPC_STRESS_MAX_SLEEP_MS=1;MEMRPC_STRESS_SEED=1"
  )

  add_executable(virus_executor_service_sample_rules_test
      tests/unit/ves/ves_sample_rules_test.cpp)
  target_link_libraries(virus_executor_service_sample_rules_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_sample_rules_test
      COMMAND virus_executor_service_sample_rules_test)

  add_executable(virus_executor_service_health_test
      tests/unit/ves/ves_health_test.cpp)
  target_link_libraries(virus_executor_service_health_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_health_test
      COMMAND virus_executor_service_health_test)

  add_executable(virus_executor_service_policy_test
      tests/unit/ves/ves_policy_test.cpp)
  target_link_libraries(virus_executor_service_policy_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_policy_test
      COMMAND virus_executor_service_policy_test)

  add_executable(virus_executor_service_crash_recovery_test
      tests/dt/ves_crash_recovery_test.cpp)
  target_link_libraries(virus_executor_service_crash_recovery_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_crash_recovery_test
      COMMAND virus_executor_service_crash_recovery_test)
  set_tests_properties(virus_executor_service_crash_recovery_test PROPERTIES LABELS "dt")

  add_executable(virus_executor_service_shutdown_lifecycle_test
      tests/dt/ves_shutdown_lifecycle_test.cpp)
  target_link_libraries(virus_executor_service_shutdown_lifecycle_test PRIVATE
      virus_executor_service_lib
      GTest::gtest_main
  )
  add_test(NAME virus_executor_service_shutdown_lifecycle_test
      COMMAND virus_executor_service_shutdown_lifecycle_test)
  set_tests_properties(virus_executor_service_shutdown_lifecycle_test PROPERTIES LABELS "dt")

  add_test(NAME virus_executor_service_supervisor_integration_test
      COMMAND virus_executor_service_supervisor
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(virus_executor_service_supervisor_integration_test PROPERTIES
      LABELS "integration"
      TIMEOUT 60
      RUN_SERIAL TRUE)

  add_test(NAME virus_executor_service_stress_test
      COMMAND virus_executor_service_stress_client --threads 2 --iterations 100 --seed 42 --no-crash
      WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  set_tests_properties(virus_executor_service_stress_test PROPERTIES
      LABELS "stress"
      TIMEOUT 30
      RUN_SERIAL TRUE)

  set_tests_properties(
      virus_executor_service_session_service_test
      virus_executor_service_heartbeat_test
      virus_executor_service_codec_test
      virus_executor_service_testkit_headers_test
      virus_executor_service_testkit_codec_test
      virus_executor_service_testkit_service_test
      virus_executor_service_testkit_baseline_test
      virus_executor_service_testkit_client_test
      virus_executor_service_testkit_dfx_test
      virus_executor_service_testkit_backpressure_test
      virus_executor_service_testkit_async_pipeline_test
      virus_executor_service_testkit_latency_test
      virus_executor_service_sample_rules_test
      virus_executor_service_health_test
      virus_executor_service_policy_test
      PROPERTIES TIMEOUT ${VIRUS_EXECUTOR_SERVICE_DEFAULT_TEST_TIMEOUT_SEC})

  set_tests_properties(
      virus_executor_service_testkit_throughput_test
      virus_executor_service_testkit_stress_config_test
      virus_executor_service_testkit_stress_smoke
      virus_executor_service_stress_test
      PROPERTIES TIMEOUT ${VIRUS_EXECUTOR_SERVICE_STRESS_TEST_TIMEOUT_SEC})

  set_tests_properties(
      virus_executor_service_crash_recovery_test
      virus_executor_service_shutdown_lifecycle_test
      virus_executor_service_testkit_dt_stability_test
      virus_executor_service_testkit_dt_perf_test
      PROPERTIES TIMEOUT ${VIRUS_EXECUTOR_SERVICE_DT_TEST_TIMEOUT_SEC})

  set(virus_executor_service_warning_relaxed_targets
      virus_executor_service_session_service_test
      virus_executor_service_heartbeat_test
      virus_executor_service_codec_test
      virus_executor_service_testkit_headers_test
      virus_executor_service_testkit_codec_test
      virus_executor_service_testkit_service_test
      virus_executor_service_testkit_baseline_test
      virus_executor_service_testkit_client_test
      virus_executor_service_testkit_dfx_test
      virus_executor_service_testkit_backpressure_test
      virus_executor_service_testkit_async_pipeline_test
      virus_executor_service_testkit_latency_test
      virus_executor_service_testkit_throughput_test
      virus_executor_service_testkit_dt_stability_test
      virus_executor_service_testkit_dt_perf_test
      virus_executor_service_testkit_stress_config
      virus_executor_service_testkit_stress_config_test
      virus_executor_service_testkit_stress_runner
      virus_executor_service_sample_rules_test
      virus_executor_service_health_test
      virus_executor_service_policy_test
      virus_executor_service_crash_recovery_test
      virus_executor_service_shutdown_lifecycle_test)
  foreach(target IN LISTS virus_executor_service_warning_relaxed_targets)
    memrpc_relax_warnings_for_target(${target})
  endforeach()

  if (VIRUS_EXECUTOR_SERVICE_ENABLE_FUZZ)
    add_subdirectory(tests/fuzz)
  endif()

  if (MEMRPC_ENABLE_CLANG_TIDY AND MEMRPC_CLANG_TIDY_MAINLINE_ONLY)
    set(virus_executor_service_tidy_excluded_targets
        virus_executor_service_session_service_test
        virus_executor_service_heartbeat_test
        virus_executor_service_codec_test
        virus_executor_service_testkit_headers_test
        virus_executor_service_testkit_codec_test
        virus_executor_service_testkit_service_test
        virus_executor_service_testkit_baseline_test
        virus_executor_service_testkit_client_test
        virus_executor_service_testkit_dfx_test
        virus_executor_service_testkit_backpressure_test
        virus_executor_service_testkit_async_pipeline_test
        virus_executor_service_testkit_latency_test
        virus_executor_service_testkit_throughput_test
        virus_executor_service_testkit_dt_stability_test
        virus_executor_service_testkit_dt_perf_test
        virus_executor_service_testkit_stress_config_test
        virus_executor_service_testkit_stress_runner
        virus_executor_service_sample_rules_test
        virus_executor_service_health_test
        virus_executor_service_policy_test
        virus_executor_service_crash_recovery_test
        virus_executor_service_shutdown_lifecycle_test)
    foreach(target IN LISTS virus_executor_service_tidy_excluded_targets)
      ves_maybe_disable_clang_tidy(${target})
    endforeach()
  endif()
endif()
