#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>

namespace heteroRT {
    template<typename T>
    class WSDeque {
    private:
        // This is where other threads will steal from
        alignas(64) std::atomic<int64_t> top;

        // This is where the owner thread will push/pop from
        alignas(64) std::atomic<int64_t> bottom;

        // This is where the data will go
        alignas(64) std::unique_ptr<T[]> data;

        // The size of the data array
        size_t size_;
        size_t mask_;

        /*
         * Calculate the index of i in the ring buffer.
         *
         * Arguments:
         *     int64_t i - The number to calculate the index of.
         * Returns:
         *     size_t - The index of i.
         */
        inline size_t idx(int64_t i) const noexcept {
            return (mask_ ? (static_cast<size_t>(i) & mask_) : (static_cast<size_t>(i) % size_));
        }
    public:
        /*
         * Initialize a work-stealing deque with a size.
         *
         * Arguments:
         *     size_t capacity - Create a deque with this size.
         * Returns:
         *     WSDeque - An initialized WSDeque.
         */
        explicit WSDeque(size_t capacity) : 
            top(0), bottom(0),
            data(new T[capacity]),
            size_(capacity),
            mask_(((capacity & (capacity - 1)) == 0) ? (capacity - 1) : 0) 
        {}

        /*
         * Push an object onto the deque from the current thread.
         *
         * Arguments:
         *     const T& obj - Push this object onto the deque.
         * Returns:
         *     int - 0 if successful, -1 if failed.
         */
        int push(const T& obj) {
            /* Read the volatile states */
            int64_t b = bottom.load(std::memory_order_relaxed);
            int64_t t = top.load(std::memory_order_acquire);

            /* Check if the array is full */
            int64_t s = b - t;
            if(s >= static_cast<int64_t>(size_) - 1)
                return -1;

            /* Add the item to the bottom of the deque and increment the bottom pointer */
            data[idx(b)] = obj;
            bottom.store(b + 1, std::memory_order_release);

            return 0;
        }

        /*
         * Steal from the current thread's data.
         *
         * Returns:
         *     std::optional<T> - The data if successful, std::nullopt if failure.
         */
        std::optional<T> steal() {
            /* Read the volatile states */
            int64_t t = top.load(std::memory_order_acquire);
            int64_t b = bottom.load(std::memory_order_acquire);

            /* Check if the array is empty */
            int64_t s = b - t;
            if(s <= 0)
                return std::nullopt;

            T obj = data[idx(t)];

            /* Check if another worker stole before this thread could*/
            if(!top.compare_exchange_strong(t, t + 1, std::memory_order_acq_rel, std::memory_order_acquire))
                return std::nullopt;
            
            return obj;
        }

        /*
         * Pop from the deque within the current thread.
         *
         * Returns:
         *     std::optional<T> - The data if successful, std::nullopt if empty.
         */
        std::optional<T> pop() {
            /* Need to get the next item in the deque */
            int64_t b = bottom.load(std::memory_order_relaxed) - 1;
            bottom.store(b, std::memory_order_relaxed);

            /* Check if the deque is empty */
            int64_t t = top.load(std::memory_order_acquire);
            int64_t s = b - t;
            if(s < 0) {
                bottom.store(t, std::memory_order_release);
                return std::nullopt;
            }

            /* If size is greater than 0, no race occurred */
            std::optional<T> obj = data[idx(b)];
            if(s > 0)
                return obj;

            /* If size is 0, need to check if top has changed, potential race */
            if(!top.compare_exchange_strong(t, t + 1, std::memory_order_acq_rel, std::memory_order_acquire))
                obj = std::nullopt;

            // Regardless of top logic, top should equal t+1 now
            bottom.store(t + 1, std::memory_order_release);
            return obj;
        }
    };
};