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

#include <Functions/Meos/GeoAsGeojsonPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

namespace NES {

GeoAsGeojsonPhysicalFunction::GeoAsGeojsonPhysicalFunction(PhysicalFunction wkt, PhysicalFunction option, PhysicalFunction precision)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(wkt));
    paramFns.push_back(std::move(option));
    paramFns.push_back(std::move(precision));
}

VarVal GeoAsGeojsonPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto wkt = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto option = paramFns[1].execute(record, arena).cast<nautilus::val<uint64_t>>();
    auto precision = paramFns[2].execute(record, arena).cast<nautilus::val<uint64_t>>();
    constexpr uint32_t MAX_LEN = 16384;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* w, uint32_t wsz, uint64_t option, uint64_t precision, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                GSERIALIZED* gs = geom_in(s.c_str(), -1);
                if (!gs) return 0u;
                char* out = geo_as_geojson(gs, (int)option, (int)precision, nullptr);
                free(gs);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        wkt, option, precision, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGeoAsGeojsonPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "GeoAsGeojsonPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return GeoAsGeojsonPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
