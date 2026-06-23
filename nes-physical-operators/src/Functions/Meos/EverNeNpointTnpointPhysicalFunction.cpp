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

#include <Functions/Meos/EverNeNpointTnpointPhysicalFunction.hpp>
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
#include <string.h>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
#include <meos_npoint.h>
}

namespace NES {

EverNeNpointTnpointPhysicalFunction::EverNeNpointTnpointPhysicalFunction(PhysicalFunction rid_np, PhysicalFunction pos_np, PhysicalFunction rid_tp, PhysicalFunction pos_tp, PhysicalFunction ts)
{
    paramFns.reserve(5);
    paramFns.push_back(std::move(rid_np));
    paramFns.push_back(std::move(pos_np));
    paramFns.push_back(std::move(rid_tp));
    paramFns.push_back(std::move(pos_tp));
    paramFns.push_back(std::move(ts));
}

VarVal EverNeNpointTnpointPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto rid_np = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto pos_np = paramFns[1].execute(record, arena).cast<double>();
    auto rid_tp = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto pos_tp = paramFns[3].execute(record, arena).cast<double>();
    auto ts = paramFns[4].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](uint64_t rid_np, double pos_np, uint64_t rid_tp, double pos_tp, uint64_t ts) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Npoint* np_s = npoint_make((int64_t)rid_np, pos_np);
                if (!np_s) return 0.0;
                Npoint* np_t = npoint_make((int64_t)rid_tp, pos_tp);
                if (!np_t) { free(np_s); return 0.0; }
                Temporal* inst = (Temporal*)tnpointinst_make(np_t, (TimestampTz)ts);
                free(np_t);
                if (!inst) { free(np_s); return 0.0; }
                int r = ever_ne_npoint_tnpoint(np_s, inst);
                free(np_s); free(inst);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        rid_np, pos_np, rid_tp, pos_tp, ts);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverNeNpointTnpointPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "EverNeNpointTnpointPhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    return EverNeNpointTnpointPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]));
}

} // namespace NES
