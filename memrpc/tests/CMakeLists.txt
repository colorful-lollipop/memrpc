set(MEMRPC_DEFAULT_TEST_TIMEOUT_SEC 30 CACHE STRING
  "Default timeout in seconds for memrpc tests")
set(MEMRPC_DT_TEST_TIMEOUT_SEC 60 CACHE STRING
  "Timeout in seconds for memrpc DT tests")

function(memrpc_add_test target source)
  add_executable(${target} ${source})
  if (MEMRPC_ENABLE_CLANG_TIDY AND MEMRPC_CLANG_TIDY_MAINLINE_ONLY)
    memrpc_disable_clang_tidy_for_target(${target})
  endif()
  memrpc_relax_warnings_for_target(${target})
  target_link_libraries(${target} PRIVATE memrpc memrpc_test_support GTest::gtest_main)
  target_include_directories(${target} PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${MEMRPC_MODULE_ROOT}/src
  )
  target_compile_definitions(${target} PRIVATE
    MEMRPC_SOURCE_DIR=\"${MEMRPC_MODULE_ROOT}\"
    MEMRPC_REPO_ROOT=\"${PROJECT_SOURCE_DIR}\"
  )
  add_test(NAME ${target} COMMAND ${target})
  set_tests_properties(${target} PROPERTIES TIMEOUT ${MEMRPC_DEFAULT_TEST_TIMEOUT_SEC})
endfunction()

memrpc_add_test(memrpc_smoke_test smoke_test.cpp)
memrpc_add_test(memrpc_types_test types_test.cpp)
memrpc_add_test(memrpc_api_headers_test api_headers_test.cpp)
memrpc_add_test(memrpc_framework_split_headers_test framework_split_headers_test.cpp)
memrpc_add_test(memrpc_file_payload_test file_payload_test.cpp)
memrpc_add_test(memrpc_log_test log_test.cpp)
memrpc_add_test(memrpc_protocol_layout_test protocol_layout_test.cpp)
memrpc_add_test(memrpc_byte_codec_test byte_codec_test.cpp)
memrpc_add_test(memrpc_session_test session_test.cpp)
memrpc_add_test(memrpc_build_config_test build_config_test.cpp)
memrpc_add_test(memrpc_rpc_client_api_test rpc_client_api_test.cpp)
memrpc_add_test(memrpc_rpc_payload_limits_test rpc_payload_limits_test.cpp)
memrpc_add_test(memrpc_rpc_client_shutdown_race_test rpc_client_shutdown_race_test.cpp)
set_tests_properties(memrpc_rpc_client_shutdown_race_test PROPERTIES
  TIMEOUT 60)
memrpc_add_test(memrpc_rpc_server_api_test rpc_server_api_test.cpp)
memrpc_add_test(memrpc_bootstrap_health_check_test bootstrap_health_check_test.cpp)
memrpc_add_test(memrpc_rpc_client_timeout_watchdog_test rpc_client_timeout_watchdog_test.cpp)
target_compile_definitions(memrpc_rpc_client_timeout_watchdog_test PRIVATE
  MEMRPC_ASYNC_WATCHDOG_INTERVAL_MS=20
)
memrpc_add_test(memrpc_rpc_client_idle_callback_test rpc_client_idle_callback_test.cpp)
memrpc_add_test(memrpc_rpc_client_crash_recovery_test rpc_client_crash_recovery_test.cpp)
memrpc_add_test(memrpc_rpc_server_executor_test rpc_server_executor_test.cpp)
memrpc_add_test(memrpc_typed_future_test typed_future_test.cpp)
memrpc_add_test(memrpc_engine_death_handler_test engine_death_handler_test.cpp)
memrpc_add_test(memrpc_rpc_client_external_recovery_test rpc_client_external_recovery_test.cpp)
memrpc_add_test(memrpc_rpc_client_recovery_policy_test rpc_client_recovery_policy_test.cpp)
memrpc_add_test(memrpc_rpc_eventfd_fault_injection_test rpc_eventfd_fault_injection_test.cpp)
set_tests_properties(memrpc_rpc_eventfd_fault_injection_test PROPERTIES
  LABELS "dt_injection")

if (MEMRPC_ENABLE_DT_TESTS)
  memrpc_add_test(memrpc_dt_stability_test dt_stability_test.cpp)
  set_tests_properties(memrpc_dt_stability_test PROPERTIES
    LABELS "dt_stability"
    TIMEOUT ${MEMRPC_DT_TEST_TIMEOUT_SEC})
  memrpc_add_test(memrpc_dt_perf_test dt_perf_test.cpp)
  set_tests_properties(memrpc_dt_perf_test PROPERTIES
    LABELS "dt_perf"
    RUN_SERIAL TRUE
    TIMEOUT ${MEMRPC_DT_TEST_TIMEOUT_SEC})
endif()
