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

#include <Functions/Meos/Th3indexIsPentagonPhysicalFunction.hpp>
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

Th3indexIsPentagonPhysicalFunction::Th3indexIsPentagonPhysicalFunction(PhysicalFunction cell, PhysicalFunction ts)
{
    paramFns.reserve(2);
    paramFns.push_back(std::move(cell));
    paramFns.push_back(std::move(ts));
}

VarVal Th3indexIsPentagonPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto ts   = paramFns[1].execute(record, arena).cast<uint64_t>();

    const auto result = nautilus::invoke(
        +[](uint64_t cell, uint64_t ts) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Temporal* inst = th3indexinst_make((H3Index)cell, (TimestampTz)ts);
                if (!inst) return 0.0;
                Temporal* res = th3index_is_pentagon(inst);
                free(inst);
                if (!res) return 0.0;
                double r = tbool_start_value(res) ? 1.0 : 0.0;
                free(res);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        cell, ts);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTh3indexIsPentagonPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "Th3indexIsPentagonPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return Th3indexIsPentagonPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
