#pragma once

#include <tuple>
#include <utility>
#include <vector>
#include <cstdint>
#include <atomic>
#include <memory>
#include <mutex>
#include <condition_variable>

namespace heteroRT {
    /*
     * Base class for Tasks.
     */
    class TaskBase {
        public:
            // How many tasks must complete before this task can start?
            std::atomic<int64_t> waiting_on;
            // Which tasks are dependent on this task?
            std::vector<TaskBase*> children;

            /* Execute this task */
            virtual void execute() = 0;

            /* This task depends on another task */
            virtual void depends_on(TaskBase *other) = 0;

            /* Default destructor */
            virtual ~TaskBase() = default;
    };

    /*
     * Represents a Task for the Scheduler to complete.
     * Tasks can be run on different backends and can
     * have dependencies to other tasks.
     */
    template<typename Func, typename... Args>
    class Task : public TaskBase {
    private:
        Func func_; // Function to execute
        std::tuple<Args...> args_; // Arguments to pass function

        /*
         * Invoke the Task's function with the Task's arguments.
         */
        template<std::size_t... IndexSequence>
        void invoke(std::index_sequence<IndexSequence...>) {
            func_(std::get<IndexSequence>(args_)...);
        }
    public:
        /*
         * Instantiate a Task instance.
         *
         * Arguments:
         *     Func& f - The task will run this function.
         *     Args&... - The task will call these arguments.
         */
        Task(Func& f, Args&... args)
            : func_(std::forward<Func>(f)),
              args_(std::forward<Args>(args)...) {
                waiting_on.store(0);
              }

        /*
         * Execute the task.
         */
        void execute() override {
            invoke(std::index_sequence_for<Args...>{});
            for(auto& child : children) {
                child->waiting_on.fetch_sub(1);
            }
        }

        /*
         * Create a dependency on another task.
         *
         * Arguments:
         *     TaskBase *other - This task depends on other task.
         */
        void depends_on(TaskBase *other) override {
            waiting_on.fetch_add(1); // This task is waiting on one other task to complete
            other->children.emplace_back(this); // Other task is necessary for this task
        }
    };
};