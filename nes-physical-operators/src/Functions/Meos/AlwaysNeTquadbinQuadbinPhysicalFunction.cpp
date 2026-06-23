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

#include <Functions/Meos/AlwaysNeTquadbinQuadbinPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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

namespace NES {

AlwaysNeTquadbinQuadbinPhysicalFunction::AlwaysNeTquadbinQuadbinPhysicalFunction(PhysicalFunction inst_cell, PhysicalFunction ts, PhysicalFunction cell) {
    paramFns.reserve(3);
    paramFns.push_back(std::move(inst_cell));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(cell));
}

VarVal AlwaysNeTquadbinQuadbinPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto inst_cell = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts        = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto cell      = paramFns[2].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](uint64_t ic, uint64_t ts, uint64_t cell) -> double {
            MEOS::Meos::ensureMeosInitialized();
            TInstant* inst = tquadbininst_make((Quadbin)ic, (TimestampTz)ts);
            if (!inst) return 0.0;
            int r = always_ne_tquadbin_quadbin((const Temporal*)inst, (Quadbin)cell);
            free(inst);
            return r > 0 ? 1.0 : 0.0;
        },
        inst_cell, ts, cell);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysNeTquadbinQuadbinPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==3,
                 "AlwaysNeTquadbinQuadbinPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return AlwaysNeTquadbinQuadbinPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
