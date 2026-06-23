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

#include <Functions/Meos/QuadbinGtPhysicalFunction.hpp>
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

QuadbinGtPhysicalFunction::QuadbinGtPhysicalFunction(PhysicalFunction a, PhysicalFunction b)
{
    paramFns.reserve(2);
    paramFns.push_back(std::move(a));
    paramFns.push_back(std::move(b));
}

VarVal QuadbinGtPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto a = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto b = paramFns[1].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](uint64_t a, uint64_t b) -> double {
            MEOS::Meos::ensureMeosInitialized();
            bool r = quadbin_gt((Quadbin)a, (Quadbin)b);
            return r ? 1.0 : 0.0;
        },
        a, b);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterQuadbinGtPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==2,
                 "QuadbinGtPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return QuadbinGtPhysicalFunction(std::move(arguments.childFunctions[0]),
                                 std::move(arguments.childFunctions[1]));
}

} // namespace NES
