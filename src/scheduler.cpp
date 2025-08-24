#include <memory>
#include <mutex>
#include <random>
#include <atomic>

#include "scheduler.hpp"

// Used to steal from other workers
static thread_local std::mt19937 generator(std::random_device{}());

/*
 * Initialize a Scheduler instance.
 *
 * Arguments:
 *     size_t n_jobs - How many workers to spawn.
 */
heteroRT::Scheduler::Scheduler(size_t n_jobs) {
    workers.reserve(n_jobs);

    // Create workers
    for(size_t i = 0; i < n_jobs; i++) {
        workers.emplace_back(std::make_unique<Worker>(1024));
    }

    // Start workers
    for(size_t i = 0; i < workers.size(); i++) {
        workers[i]->thread = std::thread([this, i]() { worker_loop(i); });
    }
}

/*
 * Destroy a Scheduler instance.
 */
heteroRT::Scheduler::~Scheduler() {
    stop.store(true, std::memory_order_release);
    for(auto& worker : workers) {
        if(worker->thread.joinable()) worker->thread.join();
    }
}

/*
 * Each thread will go through this loop.
 *
 * Arguments:
 *     int id - Run the loop for this worker.
 */
void heteroRT::Scheduler::worker_loop(int id) {
    // Grab the current worker
    Worker *worker = workers[id].get();
    
    while(!stop.load(std::memory_order_acquire)) {        
        /* Try to pull a local task first */
        auto local_task = worker->deque->pop();
        if(local_task.has_value()) {
            local_task.value()->execute();

            // If we grabbed a local task, add the independent children back to 
            // the deque
            for(auto& task : local_task.value()->children) {
                if(task->waiting_on.load(std::memory_order_acquire) == 0) {
                    worker->deque->push(task);
                    running_tasks.fetch_add(1, std::memory_order_acq_rel);
                }
            }

            delete local_task.value();

            // One task complete
            if(running_tasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                std::lock_guard<std::mutex> guard(cvMutex);
                taskCV.notify_one();
            }
        } else {
            /* Fall back to the global task queue */
            auto global_task = next_task();
            if(global_task.has_value()) {
                global_task.value()->execute();

                // If we grabbed a global task, add the independent children
                // to the local deque
                for(auto& task : global_task.value()->children) {
                    if(task->waiting_on.load(std::memory_order_acquire) == 0) {
                        worker->deque->push(task);
                        running_tasks.fetch_add(1, std::memory_order_acq_rel);
                    }
                }

                delete global_task.value();

                // One task complete
                if(running_tasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    std::lock_guard<std::mutex> guard(cvMutex);
                    taskCV.notify_one();
                }
            } else {
                /* Finally, rely on stealing tasks (random victim) */
                std::uniform_int_distribution<> distribution(0, workers.size() - 2);
                int index = distribution(generator);
                if(index >= id) index++;
                
                auto stolen_task = workers[index]->deque->steal();
                if(stolen_task.has_value()) {
                    stolen_task.value()->execute();

                    // If we stole a task, add the independent children
                    // to the local deque
                    for(auto& task : stolen_task.value()->children) {
                        if(task->waiting_on.load(std::memory_order_acquire) == 0) {
                            worker->deque->push(task);
                            running_tasks.fetch_add(1, std::memory_order_acq_rel);
                        }
                    }

                    delete stolen_task.value();

                    if(running_tasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                        std::lock_guard<std::mutex> guard(cvMutex);
                        taskCV.notify_one();
                    }
                } else {
                    std::this_thread::yield();
                }
            }
        }
    }
}

/*
 * Pop a task from the task queue.
 *
 * Returns:
 *     std::optional<heteroRT::TaskBase*> - A pointer to the next Task.
 */
std::optional<heteroRT::TaskBase*> heteroRT::Scheduler::next_task() {
    std::lock_guard<std::mutex> gaurd(mMutex);
    if(global_queue.empty()) return std::nullopt;
    heteroRT::TaskBase *front = global_queue.front();
    global_queue.pop();
    return front;
}

/*
 * Add a task to the task queue.
 * 
 * Arguments:
 *     heteroRT::TaskBase *task - The task to enqueue.
 */
void heteroRT::Scheduler::spawn(heteroRT::TaskBase *task) {
    std::lock_guard<std::mutex> gaurd(mMutex);
    if(task->waiting_on.load(std::memory_order_acquire) == 0) {
        running_tasks.fetch_add(1, std::memory_order_acq_rel);
        global_queue.emplace(task);
    }
}

/*
 * Wait for all running tasks to complete.
 */
void heteroRT::Scheduler::wait() {
    std::unique_lock<std::mutex> lock(cvMutex);
    taskCV.wait(lock, [&] { return running_tasks == 0; });
}