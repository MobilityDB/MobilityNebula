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

#include <Functions/Meos/Th3indexGridDistancePhysicalFunction.hpp>
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

Th3indexGridDistancePhysicalFunction::Th3indexGridDistancePhysicalFunction(PhysicalFunction cell1, PhysicalFunction ts1,
                                              PhysicalFunction cell2, PhysicalFunction ts2)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(cell1));
    paramFns.push_back(std::move(ts1));
    paramFns.push_back(std::move(cell2));
    paramFns.push_back(std::move(ts2));
}

VarVal Th3indexGridDistancePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell1 = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts1   = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto cell2 = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto ts2   = paramFns[3].execute(record, arena).cast<uint64_t>();

    const auto result = nautilus::invoke(
        +[](uint64_t cell1, uint64_t ts1, uint64_t cell2, uint64_t ts2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Temporal* inst1 = th3indexinst_make((H3Index)cell1, (TimestampTz)ts1);
                if (!inst1) return 0.0;
                Temporal* inst2 = th3indexinst_make((H3Index)cell2, (TimestampTz)ts2);
                if (!inst2) { free(inst1); return 0.0; }
                Temporal* res = th3index_grid_distance(inst1, inst2);
                free(inst1);
                free(inst2);
                if (!res) return 0.0;
                double r = (double)tint_start_value(res);
                free(res);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        cell1, ts1, cell2, ts2);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTh3indexGridDistancePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "Th3indexGridDistancePhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return Th3indexGridDistancePhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
