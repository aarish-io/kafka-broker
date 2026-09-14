cmake_minimum_required(VERSION 3.16)

project(kafka-broker LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(kafka-broker
    src/main.cpp
    src/broker.cpp
    src/protocol.cpp
    src/tcp_server.cpp
    src/epoll_server.cpp
)

target_include_directories(kafka-broker PRIVATE ${CMAKE_SOURCE_DIR}/src)

add_executable(test_record
    tests/test_record.cpp
)

add_executable(test_topic_log
    tests/test_topic_log.cpp
)

add_executable(test_protocol_parse
    tests/test_protocol_parse.cpp
    src/protocol.cpp
)

add_executable(test_broker_replicate
    tests/test_broker_replicate.cpp
    src/broker.cpp
    src/protocol.cpp
)

target_include_directories(test_broker_replicate PRIVATE ${CMAKE_SOURCE_DIR}/src)
