cmake_minimum_required(VERSION 3.10)
project(MatchingEngine)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -g") # Added -g for debug symbols

find_package(Boost 1.65 REQUIRED COMPONENTS system)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(matching_engine matching_engine.cpp)
target_link_libraries(matching_engine ${Boost_LIBRARIES} pthread)