cmake_minimum_required(VERSION 3.14)
project(HeteroRT)

set(CMAKE_CXX_STANDARD 20)

add_executable(sched_test src/test.cpp src/scheduler.cpp)
target_include_directories(sched_test PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_include_directories(sched_test PUBLIC ${PROJECT_SOURCE_DIR}/include)