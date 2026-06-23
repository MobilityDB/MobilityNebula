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

#include <Functions/Meos/ContainedIntSpanPhysicalFunction.hpp>
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
}
#include <string>

namespace NES {

ContainedIntSpanPhysicalFunction::ContainedIntSpanPhysicalFunction(PhysicalFunction val, PhysicalFunction sp) {
    paramFns.reserve(2);
    paramFns.push_back(std::move(val));
    paramFns.push_back(std::move(sp));
}

VarVal ContainedIntSpanPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto val = paramFns[0].execute(record, arena).cast<double>();
    auto sp  = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    const auto result = nautilus::invoke(
        +[](double val_d, const char* w, uint32_t wsz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                Span* sp = intspan_in(s.c_str());
                if (!sp) return 0.0;
                bool r = contained_int_span((int)val_d, sp);
                free(sp);
                return r ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        val, sp);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterContainedIntSpanPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==2,
                 "ContainedIntSpanPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return ContainedIntSpanPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
