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

#include <Functions/Meos/FloatspanMakePhysicalFunction.hpp>
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
#include <string.h>

namespace NES {

FloatspanMakePhysicalFunction::FloatspanMakePhysicalFunction(PhysicalFunction lower, PhysicalFunction upper, PhysicalFunction lower_inc, PhysicalFunction upper_inc) {
    paramFns.reserve(4);
    paramFns.push_back(std::move(lower));
    paramFns.push_back(std::move(upper));
    paramFns.push_back(std::move(lower_inc));
    paramFns.push_back(std::move(upper_inc));
}

VarVal FloatspanMakePhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto lower     = paramFns[0].execute(record, arena).cast<double>();
    auto upper     = paramFns[1].execute(record, arena).cast<double>();
    auto lower_inc = paramFns[2].execute(record, arena).cast<double>();
    auto upper_inc = paramFns[3].execute(record, arena).cast<double>();
    constexpr uint32_t MAX_LEN = 64;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));
    const auto actualLen = nautilus::invoke(
        +[](double lo, double hi, double lo_inc, double hi_inc,
            char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Span* sp = floatspan_make(lo, hi,
                                         lo_inc != 0.0, hi_inc != 0.0);
                if (!sp) return 0u;
                char* out = floatspan_out(sp, -1);
                free(sp);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        lower, upper, lower_inc, upper_inc,
        outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));
    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterFloatspanMakePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==4,
                 "FloatspanMakePhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return FloatspanMakePhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
