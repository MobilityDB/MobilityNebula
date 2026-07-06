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

#include <Functions/Meos/TemporalEDWithinTNpointGeometryPhysicalFunction.hpp>

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
#include <meos_geo.h>
#include <meos_npoint.h>
}

namespace NES {

TemporalEDWithinTNpointGeometryPhysicalFunction::TemporalEDWithinTNpointGeometryPhysicalFunction(PhysicalFunction ridFunction,
                                                          PhysicalFunction fractionFunction,
                                                          PhysicalFunction timestampFunction,
                                                          PhysicalFunction geometryFunction,
                                                          PhysicalFunction distFunction)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(ridFunction));
    parameterFunctions.push_back(std::move(fractionFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(geometryFunction));
    parameterFunctions.push_back(std::move(distFunction));
}

VarVal TemporalEDWithinTNpointGeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto rid       = parameterValues[0].cast<nautilus::val<uint64_t>>();
    auto fraction  = parameterValues[1].cast<nautilus::val<double>>();
    auto timestamp = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto geometry  = parameterValues[3].cast<VariableSizedData>();
    auto dist      = parameterValues[4].cast<nautilus::val<double>>();

    const auto result = nautilus::invoke(
        +[](uint64_t ridValue, double fractionValue, uint64_t timestampValue,
            const char* geometryPtr, uint32_t geometrySize, double distValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();

                const std::string timestampString = MEOS::Meos::convertEpochToTimestamp(timestampValue);
                std::string tnpointWkt = fmt::format("NPoint({}, {})@{}", ridValue, fractionValue, timestampString);
                std::string staticGeometryWkt(geometryPtr, geometrySize);

                while (!staticGeometryWkt.empty() && (staticGeometryWkt.front() == '\'' || staticGeometryWkt.front() == '"'))
                    staticGeometryWkt.erase(staticGeometryWkt.begin());
                while (!staticGeometryWkt.empty() && (staticGeometryWkt.back() == '\'' || staticGeometryWkt.back() == '"'))
                    staticGeometryWkt.pop_back();

                if (tnpointWkt.empty() || staticGeometryWkt.empty()) return 0;

                Temporal* tnpoint = tnpoint_in(tnpointWkt.c_str());
                if (!tnpoint) return 0;
                Temporal* tgeo = tnpoint_to_tgeompoint(tnpoint);
                if (!tgeo) { free(tnpoint); return 0; }
                MEOS::Meos::StaticGeometry staticGeometry(staticGeometryWkt);
                if (!staticGeometry.getGeometry()) { free(tgeo); free(tnpoint); return 0; }

                int r = edwithin_tgeo_geo(tgeo, staticGeometry.getGeometry(), distValue);
                free(tgeo);
                free(tnpoint);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        rid, fraction, timestamp, geometry.getContent(), geometry.getContentSize(), dist);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTemporalEDWithinTNpointGeometryPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "TemporalEDWithinTNpointGeometryPhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    return TemporalEDWithinTNpointGeometryPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4));
}

} // namespace NES
