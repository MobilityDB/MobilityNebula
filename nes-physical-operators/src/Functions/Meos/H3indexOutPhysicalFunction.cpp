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

#include <Functions/Meos/H3indexOutPhysicalFunction.hpp>
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
#include <meos_h3.h>
}
#include <string.h>

namespace NES {

H3indexOutPhysicalFunction::H3indexOutPhysicalFunction(PhysicalFunction cell)
{
    paramFns.reserve(1);
    paramFns.push_back(std::move(cell));
}

VarVal H3indexOutPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto cell = paramFns[0].execute(record, arena).cast<uint64_t>();

    constexpr uint32_t MAX_LEN = 32;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](uint64_t cell, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* hex = h3index_out((H3Index)cell);
                if (!hex) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(hex));
                if (len > bufMax) len = bufMax;
                memcpy(buf, hex, len);
                free(hex);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        cell, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterH3indexOutPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1,
                 "H3indexOutPhysicalFunction requires 1 child but got {}",
                 arguments.childFunctions.size());
    return H3indexOutPhysicalFunction(std::move(arguments.childFunctions[0]));
}

} // namespace NES
