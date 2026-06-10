#define NES_PLUGIN_OPERATOR_TU
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

#include <Functions/Meos/Cbuffer/EverNeTcbufferCbufferPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <fmt/format.h>
#include <function.hpp>
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_cbuffer.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterEverNeTcbufferCbufferPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

EverNeTcbufferCbufferPhysicalFunction::EverNeTcbufferCbufferPhysicalFunction(PhysicalFunction lonFunction,
                                                          PhysicalFunction latFunction,
                                                          PhysicalFunction radiusFunction,
                                                          PhysicalFunction timestampFunction,
                                                          PhysicalFunction cbufferFunction)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(radiusFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(cbufferFunction));
}

VarVal EverNeTcbufferCbufferPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lon       = parameterValues[0].cast<nautilus::val<double>>();
    auto lat       = parameterValues[1].cast<nautilus::val<double>>();
    auto radius    = parameterValues[2].cast<nautilus::val<double>>();
    auto timestamp = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto cbufLit   = parameterValues[4].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double lonValue,
            double latValue,
            double radiusValue,
            uint64_t timestampValue,
            const char* cbufLitPtr,
            uint32_t cbufLitSize) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lonValue >= -180.0 && lonValue <= 180.0 && latValue >= -90.0 && latValue <= 90.0)) return 0;
                if (radiusValue < 0.0) return 0;

                const std::string timestampString = MEOS::Meos::convertEpochToTimestamp(timestampValue);
                std::string tcbufferWkt = fmt::format("Cbuffer(Point({} {}),{})@{}",
                                                     lonValue, latValue, radiusValue, timestampString);
                std::string cbufferLiteral(cbufLitPtr, cbufLitSize);

                while (!cbufferLiteral.empty() && (cbufferLiteral.front() == '\'' || cbufferLiteral.front() == '"'))
                    cbufferLiteral.erase(cbufferLiteral.begin());
                while (!cbufferLiteral.empty() && (cbufferLiteral.back() == '\'' || cbufferLiteral.back() == '"'))
                    cbufferLiteral.pop_back();

                if (tcbufferWkt.empty() || cbufferLiteral.empty()) return 0;

                Temporal* tcbuffer = tcbuffer_in(tcbufferWkt.c_str());
                if (!tcbuffer) return 0;
                Cbuffer* cb = cbuffer_in(cbufferLiteral.c_str());
                if (!cb) { free(tcbuffer); return 0; }

                int r = ever_ne_tcbuffer_cbuffer(tcbuffer, cb);
                free(tcbuffer);
                free(cb);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        lon, lat, radius, timestamp, cbufLit.getContent(), cbufLit.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverNeTcbufferCbufferPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "EverNeTcbufferCbufferPhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    return EverNeTcbufferCbufferPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4));
}

} // namespace NES
