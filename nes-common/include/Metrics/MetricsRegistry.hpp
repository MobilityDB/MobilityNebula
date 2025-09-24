/*
    Licensed under the Apache License, Version 2.0 (the "License");
*/

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <Metrics/Histogram.hpp>

namespace NES::Metrics
{

class MetricsRegistry
{
public:
    static MetricsRegistry& instance();

    void incCounter(const std::string& name, uint64_t delta = 1)
    {
        std::lock_guard<std::mutex> lk(mtx);
        counters[name] += delta;
    }

    void observeLatencyMs(uint64_t value)
    {
        // accumulate count/sum + histogram for percentiles
        {
            std::lock_guard<std::mutex> lk(mtx);
            counters["latency_count"] += 1;
            counters["latency_sum_ms"] += value;
        }
        histogram.observe(value);
    }

    std::unordered_map<std::string, uint64_t> snapshot()
    {
        std::lock_guard<std::mutex> lk(mtx);
        auto out = counters;
        // add p50/p95/p99 estimates
        out["latency_p50_ms"] = histogram.percentile(0.50);
        out["latency_p95_ms"] = histogram.percentile(0.95);
        out["latency_p99_ms"] = histogram.percentile(0.99);
        return out;
    }

private:
    std::mutex mtx;
    std::unordered_map<std::string, uint64_t> counters;
    FixedLatencyHistogram histogram;
};

} // namespace NES::Metrics
