cmake_minimum_required(VERSION 3.15)

project(gridometer VERSION 1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

#Find standard threading
find_package(Threads REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/external)
include_directories(${CMAKE_SOURCE_DIR}/external/nlohmann)
include_directories(${CMAKE_SOURCE_DIR}/external/sqlite3)

set(SOURCES
    src/main.cpp
)

set(SQLITE_SOURCE external/sqlite3/sqlite3.c)

add_executable(gridometer ${SOURCES} ${SQLITE_SOURCE})

target_link_libraries(gridometer PRIVATE Threads::Threads)

if(UNIX AND NOT APPLE)
    target_link_libraries(gridometer PRIVATE dl m)
endif()