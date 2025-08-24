#pragma once

#include <mutex>
#include <queue>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <optional>
#include <tuple>
#include <iostream>
#include <future>

#include "task.hpp"
#include "wsq.hpp"

namespace heteroRT {
    class Scheduler {
        struct Worker {
            std::thread thread;
            WorkStealingQueue<TaskBase*> *deque;

            Worker(size_t capacity) {
                deque = new WorkStealingQueue<TaskBase*>(capacity);
            }

            ~Worker() { delete deque; }
        };
    private:
        // Holds tasks ready to be run
        std::queue<TaskBase*> global_queue;

        // Mutex for the global_queue
        std::mutex mMutex;

        // Used to determine when all tasks are complete
        std::mutex cvMutex;
        std::condition_variable taskCV;

        // Thread pool for completing tasks
        std::vector<std::unique_ptr<Worker>> workers;

        // Thread pool will run until this becomes true
        std::atomic<bool> stop{false};

        // Counts how many running tasks, so we can wait for the scheduler to finish
        std::atomic<size_t> running_tasks{0};

        // Each worker will go through this loop
        void worker_loop(int id);
        
        // Pop from the task queue
        std::optional<TaskBase*> next_task();

    public:
        Scheduler(size_t n_jobs);
        ~Scheduler();

        // Push to task queue
        void spawn(TaskBase *task);

        // Wait for all running tasks to complete
        void wait();

        /*
         * Run a for loop in parallel.
         * 
         * Arguments:
         *     size_t begin - Start at this index.
         *     size_t end - End at this index.
         *     Func&& func - Run this function.
         *     size_t grain_size - Split work into chunks of this size.
         */
        template<typename Func>
        void parallel_for(size_t begin, size_t end, Func&& func, size_t grain_size=1024) {
            if(end <= begin) return;

            // How many total iterations
            size_t total = end - begin;

            // Decide how many chunks 
            size_t num_chunks = (total + grain_size - 1) / grain_size;

            // Simple completion counter
            std::atomic<size_t> remaining(num_chunks);

            // Condition variable to wait for completion
            //std::mutex mtx;
            //std::condition_variable cv;

            for(size_t chunk = 0; chunk < num_chunks; chunk++) {
                // Calculate the begin and end of current chunk
                size_t chunk_begin = begin + chunk * grain_size;
                size_t chunk_end = std::min(chunk_begin + grain_size, end);

                // Define the task function
                auto task_func = [&, chunk_begin, chunk_end]() {
                    // Run the task on each chunk
                    for(size_t i = chunk_begin; i < chunk_end; i++) {
                        func(i);
                    }
                };

                // Create the task
                auto task = new Task(task_func);

                // Spawn the task
                spawn(task);
            }

            // Wait until there are no more remaining tasks
            wait();
        }

        template<typename T, typename Iterator, typename Func>
        T parallel_reduce(Iterator begin, Iterator end, T init, Func&& func) {
            // How many total iterations
            size_t n = std::distance(begin, end);

            // Calculate the number of tasks and the size they will iterate over
            size_t num_tasks = workers.size();
            size_t chunk_size = (n + num_tasks - 1) / num_tasks;

            std::vector<T> results(num_tasks, init);

            /* Enqueue tasks */
            for(size_t t = 0; t < num_tasks; t++) {
                Iterator chunk_begin = begin + t * chunk_size;
                Iterator chunk_end = (t+1) * chunk_size < n ? begin + (t+1) * chunk_size : end;

                // Don't go past the end of the array
                if(chunk_begin >= end) break;

                // Define the task function
                auto task_func = [&, chunk_begin, chunk_end, t]() {
                    T local = init;
                    for(Iterator it = chunk_begin; it != chunk_end; it++) {
                        local = func(local, static_cast<T>(*it));
                    }
                    results[t] = local;
                };

                // Create the task
                auto task = new Task(task_func);

                // Spawn the task
                spawn(task);
            }

            // Wait for all tasks to finish
            wait();

            // Last reduction
            T result = init;
            for(size_t i = 0; i < results.size(); i++) {
                result = func(result, results[i]);
            }

            return result;
        }
    };
};