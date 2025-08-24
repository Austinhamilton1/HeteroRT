#include <iostream>
#include <vector>
#include <numeric>
#include <chrono>
#include <string>
#include <cassert>

#include "task.hpp"
#include "scheduler.hpp"

int main(int argc, char *argv[]) {
    if(argc != 2) {
        std::cout << "Invalid usage: [program name] [n_jobs]" << std::endl;
        return -1;
    }

    const size_t N = 1 << 20; // ~1 million
    std::vector<int> data(N);

    // Fill data sequentially
    std::iota(data.begin(), data.end(), 0);

    heteroRT::Scheduler sched(std::stoi(argv[1]));

    /* Serial map */
    auto start = std::chrono::high_resolution_clock::now();
    for(size_t i = 0; i < data.size(); i++) {
        data[i] = data[i] * data[i];
    }
    auto end = std::chrono::high_resolution_clock::now();

    auto serial_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    assert(data[1000] == 1000 * 1000);

    // Fill data sequentially
    std::iota(data.begin(), data.end(), 0);

    /* Parallel map */
    start = std::chrono::high_resolution_clock::now();
    sched.parallel_for(0, N, [&](size_t i) {
        data[i] = data[i] * data[i];
    });
    end = std::chrono::high_resolution_clock::now();

    auto parallel_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    assert(data[1000] == 1000 * 1000);

    std::cout << "for time: "
              << serial_us
              << "us\n";

    std::cout << "parallel_for time: " 
              << parallel_us
              << "us\n";

    // Fill data sequentially
    std::iota(data.begin(), data.end(), 0);

    /* Serial reduce */
    start = std::chrono::high_resolution_clock::now();
    long long sum = 0;
    for(int i = 0; i < data.size(); i++) {
        sum += data[i];
    }
    end = std::chrono::high_resolution_clock::now();

    serial_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    assert(sum == data.size() * (data.size() - 1) / 2);

    std::iota(data.begin(), data.end(), 0);

    /* Parallel reduce */
    start = std::chrono::high_resolution_clock::now();
    sum = sched.parallel_reduce<long long>(data.begin(), data.end(), 0, [](long long a, long long b) {
        return a + b;
    });
    end = std::chrono::high_resolution_clock::now();

    parallel_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    assert(sum == data.size() * (data.size() - 1) / 2);

    std::cout << "for time: "
              << serial_us
              << "us\n";

    std::cout << "parallel_reduce time: " 
              << parallel_us
              << "us\n";

}