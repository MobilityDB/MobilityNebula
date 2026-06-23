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

#include <Functions/Meos/AlwaysEqTquadbinTquadbinPhysicalFunction.hpp>
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

AlwaysEqTquadbinTquadbinPhysicalFunction::AlwaysEqTquadbinTquadbinPhysicalFunction(PhysicalFunction cell1, PhysicalFunction ts1, PhysicalFunction cell2, PhysicalFunction ts2) {
    paramFns.reserve(4);
    paramFns.push_back(std::move(cell1));
    paramFns.push_back(std::move(ts1));
    paramFns.push_back(std::move(cell2));
    paramFns.push_back(std::move(ts2));
}

VarVal AlwaysEqTquadbinTquadbinPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto cell1 = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts1   = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto cell2 = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto ts2   = paramFns[3].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](uint64_t c1, uint64_t t1, uint64_t c2, uint64_t t2) -> double {
            MEOS::Meos::ensureMeosInitialized();
            TInstant* i1 = tquadbininst_make((Quadbin)c1, (TimestampTz)t1);
            if (!i1) return 0.0;
            TInstant* i2 = tquadbininst_make((Quadbin)c2, (TimestampTz)t2);
            if (!i2) { free(i1); return 0.0; }
            int r = always_eq_tquadbin_tquadbin((const Temporal*)i1, (const Temporal*)i2);
            free(i1); free(i2);
            return r > 0 ? 1.0 : 0.0;
        },
        cell1, ts1, cell2, ts2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysEqTquadbinTquadbinPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==4,
                 "AlwaysEqTquadbinTquadbinPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return AlwaysEqTquadbinTquadbinPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
