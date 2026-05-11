cmake_minimum_required(VERSION 3.14) # Phase 1: Minimum CMake version
project(DistriCache LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Export compile commands for VS Code IntelliSense
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# --- Dependencies ---

# 1. Google Test
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# 2. gRPC and Protobuf (Phase 2: Communication framework)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

# 3. Threads (Phase 3: For concurrency stress tests)
find_package(Threads REQUIRED)

# --- Code Generation ---

set(PROTO_NAME "KV_store.proto")
set(GEN_PROTO_DIR "${CMAKE_CURRENT_BINARY_DIR}/gens")
file(MAKE_DIRECTORY "${GEN_PROTO_DIR}")

set(PROTO_PB_CC "${GEN_PROTO_DIR}/KV_store.pb.cc")
set(PROTO_PB_H  "${GEN_PROTO_DIR}/KV_store.pb.h")
set(GRPC_PB_CC  "${GEN_PROTO_DIR}/KV_store.grpc.pb.cc")
set(GRPC_PB_H   "${GEN_PROTO_DIR}/KV_store.grpc.pb.h")

# Using relative paths from the build directory to satisfy protoc
add_custom_command(
  OUTPUT "${PROTO_PB_CC}" "${PROTO_PB_H}" "${GRPC_PB_CC}" "${GRPC_PB_H}"
  COMMAND ${PROTOBUF_PROTOC_EXECUTABLE}
    -I ..
    --cpp_out=gens
    --grpc_out=gens
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    "../${PROTO_NAME}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_NAME}"
  COMMENT "Generating gRPC and Protobuf C++ files from ${PROTO_NAME}"
  VERBATIM
)

# --- Targets ---

# Library for generated Proto/gRPC code
add_library(kv_grpc_proto_lib "${PROTO_PB_CC}" "${GRPC_PB_CC}")
target_link_libraries(kv_grpc_proto_lib PUBLIC gRPC::grpc++ ${PROTOBUF_LIBRARIES})
target_include_directories(kv_grpc_proto_lib PUBLIC "${GEN_PROTO_DIR}" ${PROTOBUF_INCLUDE_DIRS})

# KV Store Library
add_library(kv_store_lib KV_store.cpp KV_store.h raft_node.cpp raft_node.h)
target_include_directories(kv_store_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(kv_store_lib PUBLIC Threads::Threads kv_grpc_proto_lib)

# Server Executable
add_executable(kv_server KV_server.cpp)
target_link_libraries(kv_server kv_store_lib kv_grpc_proto_lib)

# Client Executable
add_executable(kv_client KV_client.cpp)
target_link_libraries(kv_client kv_grpc_proto_lib)

# Tests (Phase 1)
enable_testing()
add_executable(kv_store_test test_kv_store.cpp)
target_link_libraries(kv_store_test GTest::gtest_main kv_store_lib)
include(GoogleTest)
gtest_discover_tests(kv_store_test)

# Concurrency Test (Phase 3)
add_executable(concurrent_test concurrent_test.cpp)
target_link_libraries(concurrent_test kv_store_lib)

# --- Workflow Helpers ---

add_custom_target(refresh
    COMMAND ${CMAKE_COMMAND} -S ${CMAKE_SOURCE_DIR} -B ${CMAKE_BINARY_DIR}
    COMMENT "Refreshing CMake configuration..."
)

add_custom_target(run
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target kv_store_test
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target concurrent_test
    COMMAND ./kv_store_test
    COMMAND ./concurrent_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Building and running all tests (Unit & Concurrency)..."
)

add_custom_target(distclean
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/CMakeCache.txt
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/CMakeFiles
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/_deps
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/Makefile
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/cmake_install.cmake
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/kv_store_test
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/concurrent_test
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/kv_server
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/kv_client
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/libkv_store_lib.a
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/libkv_grpc_proto_lib.a
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${CMAKE_BINARY_DIR}/gens
    COMMENT "Wiping all build-generated files..."
)

# Coordinator Executable
add_executable(kv_coordinator kv_coordinator.cpp)
target_link_libraries(kv_coordinator kv_grpc_proto_lib)
