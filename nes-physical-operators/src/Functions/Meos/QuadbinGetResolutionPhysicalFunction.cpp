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

#include <Functions/Meos/QuadbinGetResolutionPhysicalFunction.hpp>
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
#include <meos_quadbin.h>
}
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <string.h>

namespace NES {

QuadbinGetResolutionPhysicalFunction::QuadbinGetResolutionPhysicalFunction(PhysicalFunction cell)
{
    paramFns.reserve(1);
    paramFns.push_back(std::move(cell));
}

VarVal QuadbinGetResolutionPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell = paramFns[0].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](uint64_t cell) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                return (double)quadbin_get_resolution((Quadbin)cell);
            } catch (const std::exception&) { return 0.0; }
        },
        cell);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterQuadbinGetResolutionPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1,
                 "QuadbinGetResolutionPhysicalFunction requires 1 children but got {}",
                 arguments.childFunctions.size());
    return QuadbinGetResolutionPhysicalFunction(
                                  std::move(arguments.childFunctions[0]));
}

} // namespace NES
