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

#include <Functions/Meos/Th3indexCellToParentPhysicalFunction.hpp>
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
#include <meos_h3.h>
}
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <string.h>

namespace NES {

Th3indexCellToParentPhysicalFunction::Th3indexCellToParentPhysicalFunction(PhysicalFunction cell, PhysicalFunction ts,
                                              PhysicalFunction resolution)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(cell));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(resolution));
}

VarVal Th3indexCellToParentPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell  = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts    = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto resolution = paramFns[2].execute(record, arena).cast<uint64_t>();

    constexpr uint32_t MAX_LEN = 32;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](uint64_t cell, uint64_t ts, uint64_t resolution, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Temporal* inst = th3indexinst_make((H3Index)cell, (TimestampTz)ts);
                if (!inst) return 0u;
                Temporal* res = th3index_cell_to_parent(inst, (int32_t)resolution);
                free(inst);
                if (!res) return 0u;
                H3Index result_cell = th3index_start_value(res);
                free(res);
                char* hex = h3index_out(result_cell);
                if (!hex) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(hex));
                if (len > bufMax) len = bufMax;
                memcpy(buf, hex, len);
                free(hex);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        cell, ts, resolution, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTh3indexCellToParentPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "Th3indexCellToParentPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return Th3indexCellToParentPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
