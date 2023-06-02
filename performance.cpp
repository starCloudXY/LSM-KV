//
// Created by BOOK3 on 2023-05-29.
//
#include "kvstore.h"
#include <stdlib.h>
#include <ctime>
#include <string>
#include <fstream>
#include <windows.h>
#include <vector>
#include "chrono"
#include <iostream>
using namespace std;

void Test_put(std::ofstream &out);
void test(uint64_t size, std::ofstream &out);
void test_throwoutput(uint64_t size,std::ofstream &out);
void test_get(uint64_t size,  std::ofstream &out);
int main(int argc, char *argv[])
{
    std::ofstream out("with_cache.csv");
    std::cout<<"Test throw output."<<std::endl;
    test_throwoutput(1000,out);
    test_throwoutput(5000,out);
    test_throwoutput(10000,out);
    test_throwoutput(50000,out);
    out.close();
    return 0;
}
void test_throwoutput(uint64_t size,std::ofstream &out){
    static int n = 0;
    ++n;
    LARGE_INTEGER cpuFreq;
    KVStore store("./data"+std::to_string(n));
    double average_put=0;

    std::vector<uint64_t> ranKeys;
    // 获取当前时间点
    auto startTime = std::chrono::high_resolution_clock::now();
    auto endTime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = endTime - startTime;

    // 执行函数迭代
    for (int i = 0; i < size; ++i)
    {
        for (uint64_t i = 0; i < size; ++i) {
            startTime = std::chrono::high_resolution_clock::now();
            store.put(i, std::string(i,'h'));
            endTime = std::chrono::high_resolution_clock::now();
            duration = endTime - startTime;
            average_put += duration.count();
        }
    }


    // 计算函数的平均执行时间（以毫秒为单位）
    average_put /=size;

    // 计算函数的吞吐量（每秒执行次数）
    double put_throughput = 1000.0 / average_put;
    std::cout<<size<<"\t"<<put_throughput<<std::endl;
}
void test_get(uint64_t size,  std::ofstream &out)
{

    static int n = 0;
    ++n;

    srand((unsigned)time(0));
    LARGE_INTEGER cpuFreq;
    LARGE_INTEGER startTime;
    LARGE_INTEGER endTime;
    double runTime;
    double average = 0;
    KVStore store("./data" + std::to_string(n));
    std::vector<uint64_t> ranKeys;
    out<<"SIZE:"<<size<<std::endl;
    for (uint64_t i = 0; i < size; ++i) {
        uint64_t ranKey = rand();
        ranKeys.push_back(ranKey);
        store.put(ranKey, std::string(i,'h'));
    }
    out<<"GET With Bloomfilter and SSTable in cache:\t";

    for (uint64_t i = 0; i < size; ++i) {
        // 有概率是store中不存在的key
        uint64_t ranKey = rand() % 5 < 4 ? ranKeys[rand() % size] : rand();
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.get(ranKey);
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    out <<average/size<< std::endl;

    out<<"GET With SSTable in cache:\t";

    for (uint64_t i = 0; i < size; ++i) {
        // 有概率是store中不存在的key
        uint64_t ranKey = rand() % 5 < 4 ? ranKeys[rand() % size] : rand();
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.get(ranKey);
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    out <<average/size<< std::endl;
    out<<"GET Without SSTable in cache:\t";

    for (uint64_t i = 0; i < size; ++i) {
        // 有概率是store中不存在的key
        uint64_t ranKey = rand() % 5 < 4 ? ranKeys[rand() % size] : rand();
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.get_without_cache(ranKey);
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    out <<average/size<< std::endl;
    for (uint64_t i = 0; i < size; ++i) {
        // 有概率是store中不存在的key
        uint64_t ranKey = rand() % 5 < 4 ? ranKeys[rand() % size] : rand();
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.del(ranKey);
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    store.reset();
}
void Test_put(std::ofstream &out){
        out<<"Throw out put test:\t";
        int operationNum;
        int totalTime_ms =0 ;
        KVStore store("./data" );
        for(int i=0;i<1024 * 32;i++){
            auto startTime = std::chrono::high_resolution_clock::now();
            store.put(i,std::string(10000,'h'));
            auto endTime = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double, std::milli> duration = endTime - startTime;
            operationNum++;
            totalTime_ms+=duration.count() ;
            if(totalTime_ms>=10000){
                totalTime_ms-=10000;
                out<<operationNum<<",";
                operationNum=0;
            }
        }
        out<<std::endl;
        }

void test(uint64_t size,  std::ofstream &out)
{

    static int n = 0;
    ++n;

    srand((unsigned)time(0));
    LARGE_INTEGER cpuFreq;
    LARGE_INTEGER startTime;
    LARGE_INTEGER endTime;
    double runTime;
    double average = 0;
    KVStore store("./data" + std::to_string(n));
    std::vector<uint64_t> ranKeys;
    out<<"SIZE:"<<size<<std::endl;
    out<<"PUT:\t";
    for (uint64_t i = 0; i < size; ++i) {
        uint64_t ranKey = rand();
        ranKeys.push_back(ranKey);
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.put(ranKey, std::string(i,'h'));
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    out <<average/size<< std::endl;
    average = 0;
    out<<"GET:\t";

    for (uint64_t i = 0; i < size; ++i) {
        // 有概率是store中不存在的key
        uint64_t ranKey = rand() % 5 < 4 ? ranKeys[rand() % size] : rand();
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.get_in_cache(ranKey);
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    out <<average/size<< std::endl;
    average = 0;
    out<<"DEL:\t";
    for (uint64_t i = 0; i < size; ++i) {
        // 有概率是store中不存在的key
        uint64_t ranKey = rand() % 5 < 4 ? ranKeys[rand() % size] : rand();
        QueryPerformanceFrequency(&cpuFreq);
        QueryPerformanceCounter(&startTime);
        store.del(ranKey);
        QueryPerformanceCounter(&endTime);
        runTime = (((endTime.QuadPart - startTime.QuadPart) * 1000000.0f) / cpuFreq.QuadPart);
        average += runTime;
    }
    out <<average/size<< std::endl;
    store.reset();
}