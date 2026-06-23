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

#include <Functions/Meos/QuadbinTileToCellPhysicalFunction.hpp>
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

QuadbinTileToCellPhysicalFunction::QuadbinTileToCellPhysicalFunction(PhysicalFunction x, PhysicalFunction y, PhysicalFunction z)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(x));
    paramFns.push_back(std::move(y));
    paramFns.push_back(std::move(z));
}

VarVal QuadbinTileToCellPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto x = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto y = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto z = paramFns[2].execute(record, arena).cast<uint64_t>();
    constexpr uint32_t MAX_LEN = 32;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](uint64_t x, uint64_t y, uint64_t z, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Quadbin cell = quadbin_tile_to_cell((uint32_t)x, (uint32_t)y, (uint32_t)z);
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
        x, y, z, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterQuadbinTileToCellPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "QuadbinTileToCellPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return QuadbinTileToCellPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
