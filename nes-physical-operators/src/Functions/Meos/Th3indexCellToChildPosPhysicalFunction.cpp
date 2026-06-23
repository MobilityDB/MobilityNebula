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

#include <Functions/Meos/Th3indexCellToChildPosPhysicalFunction.hpp>
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

namespace NES {

Th3indexCellToChildPosPhysicalFunction::Th3indexCellToChildPosPhysicalFunction(PhysicalFunction cell, PhysicalFunction ts,
                                              PhysicalFunction parent_res)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(cell));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(parent_res));
}

VarVal Th3indexCellToChildPosPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell  = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts    = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto parent_res = paramFns[2].execute(record, arena).cast<uint64_t>();

    const auto result = nautilus::invoke(
        +[](uint64_t cell, uint64_t ts, uint64_t parent_res) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Temporal* inst = th3indexinst_make((H3Index)cell, (TimestampTz)ts);
                if (!inst) return 0.0;
                Temporal* res = th3index_cell_to_child_pos(inst, (int32_t)parent_res);
                free(inst);
                if (!res) return 0.0;
                double r = (double)tint_start_value(res);
                free(res);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        cell, ts, parent_res);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTh3indexCellToChildPosPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "Th3indexCellToChildPosPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return Th3indexCellToChildPosPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
