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

#include <Functions/Meos/AlwaysNeTh3indexH3indexPhysicalFunction.hpp>
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

AlwaysNeTh3indexH3indexPhysicalFunction::AlwaysNeTh3indexH3indexPhysicalFunction(PhysicalFunction cell, PhysicalFunction ts,
                                              PhysicalFunction target)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(cell));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(target));
}

VarVal AlwaysNeTh3indexH3indexPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell   = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts     = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto target = paramFns[2].execute(record, arena).cast<uint64_t>();

    const auto result = nautilus::invoke(
        +[](uint64_t cell, uint64_t ts, uint64_t target) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Temporal* inst = th3indexinst_make((H3Index)cell, (TimestampTz)ts);
                if (!inst) return 0.0;
                int r = always_ne_th3index_h3index(inst, (H3Index)target);
                free(inst);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        cell, ts, target);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysNeTh3indexH3indexPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "AlwaysNeTh3indexH3indexPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return AlwaysNeTh3indexH3indexPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
