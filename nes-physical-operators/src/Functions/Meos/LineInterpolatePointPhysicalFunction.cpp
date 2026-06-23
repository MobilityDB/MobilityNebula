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

#include <Functions/Meos/LineInterpolatePointPhysicalFunction.hpp>
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

LineInterpolatePointPhysicalFunction::LineInterpolatePointPhysicalFunction(PhysicalFunction wkt, PhysicalFunction frac, PhysicalFunction repeat)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(wkt));
    paramFns.push_back(std::move(frac));
    paramFns.push_back(std::move(repeat));
}

VarVal LineInterpolatePointPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto wkt = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto frac = paramFns[1].execute(record, arena).cast<double>();
    auto repeat = paramFns[2].execute(record, arena).cast<nautilus::val<uint64_t>>();
    constexpr uint32_t MAX_LEN = 8192;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* w, uint32_t wsz, double frac, uint64_t repeat, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                GSERIALIZED* gs = geom_in(s.c_str(), -1);
                if (!gs) return 0u;
                GSERIALIZED* result = line_interpolate_point(gs, frac, (bool)repeat);
                free(gs);
                if (!result) return 0u;
                char* out = geo_as_text(result, -1);
                free(result);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        wkt, frac, repeat, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterLineInterpolatePointPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "LineInterpolatePointPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return LineInterpolatePointPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
