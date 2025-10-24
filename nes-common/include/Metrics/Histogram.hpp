/*
    Licensed under the Apache License, Version 2.0
*/
#pragma once
#include <cstdint>
#include <vector>
#include <mutex>

namespace NES::Metrics
{

class FixedLatencyHistogram
{
public:
    FixedLatencyHistogram()
        : bounds{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 2000, 5000, 10000, 20000, 60000} // ms (explicit 0ms bucket)
        , counts(bounds.size() + 1, 0)
        , total(0)
    {
    }

    void observe(uint64_t valueMs)
    {
        std::lock_guard<std::mutex> lk(mtx);
        size_t idx = 0;
        while (idx < bounds.size() && valueMs > bounds[idx])
            ++idx;
        counts[idx] += 1;
        total += 1;
    }

    uint64_t getTotal() const { return total; }

    uint64_t percentile(double p) const
    {
        std::lock_guard<std::mutex> lk(mtx);
        if (total == 0) return 0;
        uint64_t rank = static_cast<uint64_t>(p * total);
        if (rank == 0) rank = 1;
        uint64_t acc = 0;
        for (size_t i = 0; i < counts.size(); ++i)
        {
            acc += counts[i];
            if (acc >= rank)
            {
                // Return the bucket's lower bound (avoid upward bias)
                if (i == 0)
                {
                    return 0;
                }
                if (i < bounds.size())
                {
                    // bounds are inclusive upper bounds; lower bound is previous bound + 1
                    return bounds[i - 1] + 1;
                }
                // overflow bucket lower bound
                return bounds.back() + 1;
            }
        }
        // Fallback: overflow lower bound
        return bounds.back() + 1;
    }

private:
    std::vector<uint64_t> bounds;
    std::vector<uint64_t> counts;
    uint64_t total;
    mutable std::mutex mtx;
};

} // namespace NES::Metrics
