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

#include <Sinks/PrintSink.hpp>

#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/PrintSink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <Metrics/MetricsRegistry.hpp>
#include <chrono>

namespace NES::Sinks
{

PrintSink::PrintSink(const SinkDescriptor& sinkDescriptor) : outputStream(&std::cout)
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersPrint::INPUT_FORMAT))
    {
        case Configurations::InputFormat::CSV:
            outputParser = std::make_unique<CSVFormat>(sinkDescriptor.schema);
            break;
        case Configurations::InputFormat::JSON:
            outputParser = std::make_unique<JSONFormat>(sinkDescriptor.schema);
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}
void PrintSink::start(PipelineExecutionContext&)
{
}

void PrintSink::stop(PipelineExecutionContext&)
{
}

void PrintSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in PrintSink.");

    // Minimal metrics (M1-M2): egress count and e2e latency
    const auto tuples = inputBuffer.getNumberOfTuples();
    NES::Metrics::MetricsRegistry::instance().incCounter("sink_out_total", tuples);
    // Ignore empty buffers for latency to avoid skew from control/flush buffers
    if (tuples == 0) {
        return;
    }
    const auto nowMsSigned = std::chrono::time_point_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now())
                                 .time_since_epoch()
                                 .count();
    const auto tsInMs = inputBuffer.getCreationTimestampInMS().getRawValue();
    if (tsInMs == NES::Timestamp::INVALID_VALUE || tsInMs == NES::Timestamp::INITIAL_VALUE)
    {
        NES::Metrics::MetricsRegistry::instance().incCounter("latency_missing_count", 1);
    }
    else if (nowMsSigned >= 0)
    {
        const auto nowMs = static_cast<uint64_t>(nowMsSigned);
        const auto lat = (nowMs >= tsInMs) ? static_cast<uint64_t>(nowMs - tsInMs) : 0ULL; // saturate at 0
        if (nowMs < tsInMs)
        {
            NES::Metrics::MetricsRegistry::instance().incCounter("latency_future_count", 1);
        }
        NES::Metrics::MetricsRegistry::instance().observeLatencyMs(lat);
    }

    const auto bufferAsString = outputParser->getFormattedBuffer(inputBuffer);
    *(*outputStream.wlock()) << bufferAsString << '\n';
}

std::ostream& PrintSink::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK(Writing to: std::cout, using outputParser: {}", *outputParser);
    return str;
}

Configurations::DescriptorConfig::Config PrintSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersPrint>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterPrintSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return PrintSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterPrintSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<PrintSink>(sinkRegistryArguments.sinkDescriptor);
}

}
