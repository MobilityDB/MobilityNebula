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

#include <Functions/Meos/EverEqTnpointTnpointPhysicalFunction.hpp>
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
#include <stdlib.h>

extern "C" {
#include <meos.h>
#include <meos_npoint.h>
}

namespace NES {

EverEqTnpointTnpointPhysicalFunction::EverEqTnpointTnpointPhysicalFunction(
    PhysicalFunction rid1, PhysicalFunction pos1, PhysicalFunction ts1,
    PhysicalFunction rid2, PhysicalFunction pos2, PhysicalFunction ts2)
{
    paramFns.reserve(6);
    paramFns.push_back(std::move(rid1));
    paramFns.push_back(std::move(pos1));
    paramFns.push_back(std::move(ts1));
    paramFns.push_back(std::move(rid2));
    paramFns.push_back(std::move(pos2));
    paramFns.push_back(std::move(ts2));
}

VarVal EverEqTnpointTnpointPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto rid1 = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto pos1 = paramFns[1].execute(record, arena).cast<double>();
    auto ts1  = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto rid2 = paramFns[3].execute(record, arena).cast<uint64_t>();
    auto pos2 = paramFns[4].execute(record, arena).cast<double>();
    auto ts2  = paramFns[5].execute(record, arena).cast<uint64_t>();

    const auto result = nautilus::invoke(
        +[](uint64_t rid1, double pos1, uint64_t ts1,
            uint64_t rid2, double pos2, uint64_t ts2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Npoint* np1 = npoint_make((int64_t)rid1, pos1);
                if (!np1) return 0.0;
                Temporal* inst1 = (Temporal*)tnpointinst_make(np1, (TimestampTz)ts1);
                free(np1);
                if (!inst1) return 0.0;
                Npoint* np2 = npoint_make((int64_t)rid2, pos2);
                if (!np2) { free(inst1); return 0.0; }
                Temporal* inst2 = (Temporal*)tnpointinst_make(np2, (TimestampTz)ts2);
                free(np2);
                if (!inst2) { free(inst1); return 0.0; }
                int r = ever_eq_tnpoint_tnpoint(inst1, inst2);
                free(inst1); free(inst2);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        rid1, pos1, ts1, rid2, pos2, ts2);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverEqTnpointTnpointPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "EverEqTnpointTnpointPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    return EverEqTnpointTnpointPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]));
}

} // namespace NES
