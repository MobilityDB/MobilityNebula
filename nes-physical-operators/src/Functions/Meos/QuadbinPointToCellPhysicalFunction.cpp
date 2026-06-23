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

#include <Functions/Meos/QuadbinPointToCellPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_quadbin.h>
}
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <string.h>

namespace NES {

QuadbinPointToCellPhysicalFunction::QuadbinPointToCellPhysicalFunction(PhysicalFunction lon, PhysicalFunction lat, PhysicalFunction res)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(lon));
    paramFns.push_back(std::move(lat));
    paramFns.push_back(std::move(res));
}

VarVal QuadbinPointToCellPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto lon = paramFns[0].execute(record, arena).cast<double>();
    auto lat = paramFns[1].execute(record, arena).cast<double>();
    auto res = paramFns[2].execute(record, arena).cast<uint64_t>();
    constexpr uint32_t MAX_LEN = 32;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](double lon, double lat, uint64_t res, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Quadbin cell = quadbin_point_to_cell(lon, lat, (uint32_t)res);
                if (cell == 0) return 0u;
                char* s = quadbin_index_to_string(cell);
                if (!s) return 0u;
                uint32_t len = (uint32_t)strlen(s);
                if (len > bufMax) len = bufMax;
                memcpy(buf, s, len);
                free(s);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        lon, lat, res, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterQuadbinPointToCellPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "QuadbinPointToCellPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return QuadbinPointToCellPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
