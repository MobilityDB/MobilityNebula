/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <StatisticPrinter.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <ios>
#include <stop_token>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <Metrics/MetricsRegistry.hpp>

namespace NES
{

namespace
{
void threadRoutine(
    const std::stop_token& token, std::ofstream& file, folly::MPMCQueue<PrintingStatisticListener::CombinedEventType>& events)
{
    setThreadName("StatPrinter");
    // Track baseline metrics per query to compute deltas and rates on Stop
    struct Baseline {
        std::unordered_map<std::string, uint64_t> counters;
        std::chrono::steady_clock::time_point t0;
    };
    std::unordered_map<QueryId::Underlying, Baseline> baselines;
    auto lastMetricsDump = std::chrono::steady_clock::now();
    while (!token.stop_requested())
    {
        PrintingStatisticListener::CombinedEventType event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        // Periodically dump metrics (every 5s)
        auto now = std::chrono::steady_clock::now();
        if (now - lastMetricsDump >= std::chrono::seconds(5))
        {
            lastMetricsDump = now;
            const auto snap = NES::Metrics::MetricsRegistry::instance().snapshot();
            if (!snap.empty())
            {
                file << fmt::format("{} METRICS ", std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
                bool first = true;
                for (const auto& [k, v] : snap)
                {
                    file << (first ? "" : ", ") << k << ": " << v;
                    first = false;
                }
                file << "\n";
                file.flush();
            }
        }

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            continue;
        }
        std::visit(
            Overloaded{
                [&](SubmitQuerySystemEvent startQuery)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Submit Query {}:\n{}\n", startQuery.timestamp, startQuery.queryId, startQuery.query);
                    // Capture baseline for this query at submit time
                    baselines[startQuery.queryId.getRawValue()] = Baseline{
                        .counters = NES::Metrics::MetricsRegistry::instance().snapshot(), .t0 = std::chrono::steady_clock::now()};
                },
                [&](StartQuerySystemEvent startQuery)
                { file << fmt::format("{:%Y-%m-%d %H:%M:%S} Start Query {}\n", startQuery.timestamp, startQuery.queryId); },
                [&](StopQuerySystemEvent stopQuery)
                {
                    file << fmt::format("{:%Y-%m-%d %H:%M:%S} Stop Query {}\n", stopQuery.timestamp, stopQuery.queryId);
                    // On query stop: write metrics CSV snapshot (M0–M2)
                    const auto snap = NES::Metrics::MetricsRegistry::instance().snapshot();
                    const auto base = fmt::format("EngineStats_Q{}", stopQuery.queryId.getRawValue());
                    const auto csvPath = fmt::format("{}_metrics.csv", base);
                    std::ofstream csv(csvPath, std::ios::out | std::ios::trunc);
                    if (csv.is_open())
                    {
                        csv << "metric,value\n";
                        // Derived metrics if we saw a baseline for this query
                        const auto itB = baselines.find(stopQuery.queryId.getRawValue());
                        if (itB != baselines.end())
                        {
                            const auto& b = itB->second.counters;
                            const auto t0 = itB->second.t0;
                            const auto elapsedSec
                                = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::steady_clock::now() - t0)
                                      .count()
                                / 1000.0;
                            const auto get = [&](const std::unordered_map<std::string, uint64_t>& m,
                                                 const std::string& k) -> uint64_t
                            { return (m.contains(k) ? m.at(k) : 0ULL); };
                            const uint64_t src0 = get(b, "source_in_total");
                            const uint64_t src1 = (snap.contains("source_in_total") ? snap.at("source_in_total") : 0ULL);
                            const uint64_t snk0 = get(b, "sink_out_total");
                            const uint64_t snk1 = (snap.contains("sink_out_total") ? snap.at("sink_out_total") : 0ULL);
                            const uint64_t dSrc = (src1 > src0) ? (src1 - src0) : 0ULL;
                            const uint64_t dSnk = (snk1 > snk0) ? (snk1 - snk0) : 0ULL;
                            const double epsIn = elapsedSec > 0 ? static_cast<double>(dSrc) / elapsedSec : 0.0;
                            const double epsOut = elapsedSec > 0 ? static_cast<double>(dSnk) / elapsedSec : 0.0;
                            const double selE2E = dSrc > 0 ? static_cast<double>(dSnk) / static_cast<double>(dSrc) : 0.0;
                            csv << "elapsed_secs," << elapsedSec << "\n";
                            csv << "eps_in_avg," << epsIn << "\n";
                            csv << "eps_out_avg," << epsOut << "\n";
                            csv << "selectivity_e2e," << selE2E << "\n";
                            // Per-pipeline selectivity (if present)
                            for (const auto& [k, v1] : snap)
                            {
                                if (k.rfind("pipe_", 0) == 0 && k.find("_in_total") != std::string::npos)
                                {
                                    const auto pid = k.substr(5, k.size() - 5 - std::string("_in_total").size());
                                    const auto kin0 = k;
                                    const auto kout = fmt::format("pipe_{}_out_total", pid);
                                    const uint64_t in0 = get(b, kin0);
                                    const uint64_t in1 = (snap.contains(kin0) ? snap.at(kin0) : 0ULL);
                                    const uint64_t out0 = get(b, kout);
                                    const uint64_t out1 = (snap.contains(kout) ? snap.at(kout) : 0ULL);
                                    const uint64_t dIn = (in1 > in0) ? (in1 - in0) : 0ULL;
                                    const uint64_t dOut = (out1 > out0) ? (out1 - out0) : 0ULL;
                                    const double sel = dIn > 0 ? static_cast<double>(dOut) / static_cast<double>(dIn) : 0.0;
                                    csv << fmt::format("pipe_{}_in_delta,{}\n", pid, dIn);
                                    csv << fmt::format("pipe_{}_out_delta,{}\n", pid, dOut);
                                    csv << fmt::format("pipe_{}_selectivity,{}\n", pid, sel);
                                }
                            }
                        }
                        // Raw snapshot
                        for (const auto& [k, v] : snap)
                        {
                            csv << k << "," << v << "\n";
                        }
                        csv.close();
                        file << fmt::format("Wrote metrics CSV: {}\n", csvPath);
                    }
                    // Also write a simple JSON for automation (M5)
                    const auto jsonPath = fmt::format("{}_metrics.json", base);
                    std::ofstream json(jsonPath, std::ios::out | std::ios::trunc);
                    if (json.is_open())
                    {
                        json << "{\n";
                        bool first = true;
                        auto outKV = [&](const std::string& k, double v)
                        {
                            json << (first ? "  \"" : ",\n  \"") << k << "\": " << v;
                            first = false;
                        };
                        // Derived
                        const auto itB2 = baselines.find(stopQuery.queryId.getRawValue());
                        if (itB2 != baselines.end())
                        {
                            const auto& b = itB2->second.counters;
                            const auto t0 = itB2->second.t0;
                            const auto elapsedSec
                                = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      std::chrono::steady_clock::now() - t0)
                                      .count()
                                / 1000.0;
                            const auto get = [&](const std::unordered_map<std::string, uint64_t>& m,
                                                 const std::string& k) -> uint64_t
                            { return (m.contains(k) ? m.at(k) : 0ULL); };
                            const uint64_t src0 = get(b, "source_in_total");
                            const uint64_t src1 = (snap.contains("source_in_total") ? snap.at("source_in_total") : 0ULL);
                            const uint64_t snk0 = get(b, "sink_out_total");
                            const uint64_t snk1 = (snap.contains("sink_out_total") ? snap.at("sink_out_total") : 0ULL);
                            const uint64_t dSrc = (src1 > src0) ? (src1 - src0) : 0ULL;
                            const uint64_t dSnk = (snk1 > snk0) ? (snk1 - snk0) : 0ULL;
                            const double epsIn = elapsedSec > 0 ? static_cast<double>(dSrc) / elapsedSec : 0.0;
                            const double epsOut = elapsedSec > 0 ? static_cast<double>(dSnk) / elapsedSec : 0.0;
                            const double selE2E = dSrc > 0 ? static_cast<double>(dSnk) / static_cast<double>(dSrc) : 0.0;
                            outKV("elapsed_secs", elapsedSec);
                            outKV("eps_in_avg", epsIn);
                            outKV("eps_out_avg", epsOut);
                            outKV("selectivity_e2e", selE2E);
                            for (const auto& [k, v1] : snap)
                            {
                                if (k.rfind("pipe_", 0) == 0 && k.find("_in_total") != std::string::npos)
                                {
                                    const auto pid = k.substr(5, k.size() - 5 - std::string("_in_total").size());
                                    const auto kin0 = k;
                                    const auto kout = fmt::format("pipe_{}_out_total", pid);
                                    const uint64_t in0 = get(b, kin0);
                                    const uint64_t in1 = (snap.contains(kin0) ? snap.at(kin0) : 0ULL);
                                    const uint64_t out0 = get(b, kout);
                                    const uint64_t out1 = (snap.contains(kout) ? snap.at(kout) : 0ULL);
                                    const uint64_t dIn = (in1 > in0) ? (in1 - in0) : 0ULL;
                                    const uint64_t dOut = (out1 > out0) ? (out1 - out0) : 0ULL;
                                    const double sel = dIn > 0 ? static_cast<double>(dOut) / static_cast<double>(dIn) : 0.0;
                                    outKV(fmt::format("pipe_{}_in_delta", pid), static_cast<double>(dIn));
                                    outKV(fmt::format("pipe_{}_out_delta", pid), static_cast<double>(dOut));
                                    outKV(fmt::format("pipe_{}_selectivity", pid), sel);
                                }
                            }
                        }
                        // Raw
                        for (const auto& [k, v] : snap)
                        {
                            json << (first ? "  \"" : ",\n  \"") << k << "\": " << v;
                            first = false;
                        }
                        json << "\n}\n";
                        json.close();
                        file << fmt::format("Wrote metrics JSON: {}\n", jsonPath);
                    }
                },
                [&](TaskExecutionStart taskStartEvent)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Started. Number of Tuples: {}\n",
                        taskStartEvent.timestamp,
                        taskStartEvent.taskId,
                        taskStartEvent.pipelineId,
                        taskStartEvent.queryId,
                        taskStartEvent.numberOfTuples);
                },
                [&](TaskEmit emitEvent)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} emits to Pipeline {} of Query {}. Number of Tuples: {}\n",
                        emitEvent.timestamp,
                        emitEvent.taskId,
                        emitEvent.fromPipeline,
                        emitEvent.toPipeline,
                        emitEvent.queryId,
                        emitEvent.numberOfTuples);
                },
                [&](TaskExecutionComplete taskStopEvent)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Completed\n",
                        taskStopEvent.timestamp,
                        taskStopEvent.taskId,
                        taskStopEvent.pipelineId,
                        taskStopEvent.queryId);
                },
                [](auto) {}},
            event);
    }
}
}

void PrintingStatisticListener::onEvent(Event event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

void PrintingStatisticListener::onEvent(SystemEvent event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

PrintingStatisticListener::PrintingStatisticListener(const std::filesystem::path& path)
    : file(path, std::ios::out | std::ios::app)
    , printThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken, file, events); })
{
    NES_INFO("Writing Statistics to: {}", path);
}
}
