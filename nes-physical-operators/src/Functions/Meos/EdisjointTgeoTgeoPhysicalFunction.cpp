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

#include <Functions/Meos/EdisjointTgeoTgeoPhysicalFunction.hpp>
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
}

namespace NES {

EdisjointTgeoTgeoPhysicalFunction::EdisjointTgeoTgeoPhysicalFunction(PhysicalFunction lon1, PhysicalFunction lat1, PhysicalFunction ts1, PhysicalFunction lon2, PhysicalFunction lat2, PhysicalFunction ts2)
{
    paramFns.reserve(6);
    paramFns.push_back(std::move(lon1));
    paramFns.push_back(std::move(lat1));
    paramFns.push_back(std::move(ts1));
    paramFns.push_back(std::move(lon2));
    paramFns.push_back(std::move(lat2));
    paramFns.push_back(std::move(ts2));
}

VarVal EdisjointTgeoTgeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto lon1 = paramFns[0].execute(record, arena).cast<double>();
    auto lat1 = paramFns[1].execute(record, arena).cast<double>();
    auto ts1 = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto lon2 = paramFns[3].execute(record, arena).cast<double>();
    auto lat2 = paramFns[4].execute(record, arena).cast<double>();
    auto ts2 = paramFns[5].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](double lon1, double lat1, uint64_t ts1,
            double lon2, double lat2, uint64_t ts2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string wkt1 = fmt::format("SRID=4326;POINT({},{})@{}", lon1, lat1, ts1);
                Temporal* t1 = tgeompoint_in(wkt1.c_str());
                if (!t1) return 0.0;
                std::string wkt2 = fmt::format("SRID=4326;POINT({},{})@{}", lon2, lat2, ts2);
                Temporal* t2 = tgeompoint_in(wkt2.c_str());
                if (!t2) { free(t1); return 0.0; }
                int r = edisjoint_tgeo_tgeo(t1, t2);
                free(t1); free(t2);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        lon1, lat1, ts1, lon2, lat2, ts2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEdisjointTgeoTgeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "EdisjointTgeoTgeoPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    return EdisjointTgeoTgeoPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]));
}

} // namespace NES
