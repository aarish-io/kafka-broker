cmake_minimum_required(VERSION 3.16)

project(kafka-broker LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(kafka-broker
    src/main.cpp
)

add_executable(test_record
    tests/test_record.cpp
)

add_executable(test_topic_log
    tests/test_topic_log.cpp
)
