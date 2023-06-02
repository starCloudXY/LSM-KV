cmake_minimum_required(VERSION 3.24)
project(LSMKV)

set(CMAKE_CXX_STANDARD 14)

add_executable(LSMKV correctness.cc BloomFilter.cpp kvstore.cc SSTable.cpp SkipList.cpp )
