#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <cassert>
#include <chrono>
#include "KV_store.h"

void reader_worker(const kv_store& store, int id, int iterations) {
    for (int i = 0; i < iterations; ++i) {
        std::string key = "key_" + std::to_string(i % 100);
        auto val = store.get(key);
        // We don't assert has_value because writers might not have put it yet
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    std::cout << "Reader " << id << " finished.\n";
}

void writer_worker(kv_store& store, int id, int iterations) {
    for (int i = 0; i < iterations; ++i) {
        std::string key = "key_" + std::to_string(i % 100);
        std::string val = "value_" + std::to_string(id) + "_" + std::to_string(i);
        store.put(key, val);
        
        if (i % 10 == 0) {
            store.del(key);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(2));
    }
    std::cout << "Writer " << id << " finished.\n";
}

int main() {
    kv_store store;
    const int num_readers = 10;
    const int num_writers = 4;
    const int iterations = 1000;

    std::vector<std::thread> threads;

    std::cout << "Starting concurrency test with " << num_readers << " readers and " << num_writers << " writers...\n";

    auto start_time = std::chrono::high_resolution_clock::now();

    // Spawn readers
    for (int i = 0; i < num_readers; ++i) {
        threads.emplace_back(reader_worker, std::ref(store), i, iterations);
    }

    // Spawn writers
    for (int i = 0; i < num_writers; ++i) {
        threads.emplace_back(writer_worker, std::ref(store), i, iterations);
    }

    // Join all
    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;

    std::cout << "Concurrency test passed in " << diff.count() << " seconds!\n";

    return 0;
}