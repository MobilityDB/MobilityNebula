#define NES_PLUGIN_OPERATOR_TU
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

#include <Functions/Meos/Pose/PoseMakePoint3dPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <cstdlib>
#include <cstring>
#include <function.hpp>
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
#include <meos_pose.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterPoseMakePoint3dPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

PoseMakePoint3dPhysicalFunction::PoseMakePoint3dPhysicalFunction(PhysicalFunction litFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function,
                                                          PhysicalFunction arg2Function,
                                                          PhysicalFunction arg3Function)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(litFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
    parameterFunctions.push_back(std::move(arg2Function));
    parameterFunctions.push_back(std::move(arg3Function));
}

VarVal PoseMakePoint3dPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lit = parameterValues[0].cast<VariableSizedData>();
    auto arg0 = parameterValues[1].cast<nautilus::val<double>>();
    auto arg1 = parameterValues[2].cast<nautilus::val<double>>();
    auto arg2 = parameterValues[3].cast<nautilus::val<double>>();
    auto arg3 = parameterValues[4].cast<nautilus::val<double>>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](const char* litPtr, uint32_t litSize,
            double arg0,
            double arg1,
            double arg2,
            double arg3) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempS(litPtr, litSize);
                while (!tempS.empty() && (tempS.front()=='\'' || tempS.front()=='"')) tempS.erase(tempS.begin());
                while (!tempS.empty() && (tempS.back()=='\'' || tempS.back()=='"')) tempS.pop_back();
                GSERIALIZED* temp = geom_in(tempS.c_str(), -1);
                if (!temp) return (char*) nullptr;

                Pose* res = (Pose*) pose_make_point3d(temp, arg0, arg1, arg2, arg3);
                if (!res) return (char*) nullptr;
                char* outStr = pose_out(res, 15);
                free(res);
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        lit.getContent(), lit.getContentSize(), arg0, arg1, arg2, arg3);

    const auto outLen = nautilus::invoke(
        +[](const char* s) -> uint32_t { return s ? (uint32_t) strlen(s) : (uint32_t) 0; },
        outStr);

    auto variableSized = arena.allocateVariableSizedData(outLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, uint32_t len) -> void
        {
            if (s)
            {
                memcpy(dest, s, len);
                free((void*) s);
            }
        },
        variableSized.getContent(), outStr, outLen);

    return variableSized;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterPoseMakePoint3dPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "PoseMakePoint3dPhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    return PoseMakePoint3dPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4));
}

} // namespace NES
