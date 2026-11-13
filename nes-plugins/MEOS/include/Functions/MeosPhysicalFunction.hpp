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

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// The C++ function computing one SQL signature of a MEOS function, generated from the MEOS catalog by
/// tools/codegen/codegen_nebula_sql.py. It takes one pointer per argument and one to the slot of a
/// number it answers: an argument of a variable-sized type points at the MeosRuntime::Argument naming
/// the size and the bytes of the value, any other at the slot its value is written in. It answers a
/// variable-sized value as a buffer malloc allocates, holding its length then its bytes, which the
/// caller frees, or null for no value.
template <size_t>
using MeosKernelArgument = int8_t*;

template <typename Indices>
struct MeosKernelOf;

template <size_t... Index>
struct MeosKernelOf<std::index_sequence<Index...>>
{
    using type = int8_t* (*)(MeosKernelArgument<Index>..., int8_t* out);
};

/// The kernel of a signature of the given arity
template <size_t Arity>
using MeosKernelOfArity = typename MeosKernelOf<std::make_index_sequence<Arity>>::type;

/// The largest arity of the catalog's SQL signatures
static constexpr size_t MEOS_KERNEL_MAX_ARITY = 13;

template <typename Arities>
struct MeosKernelVariantOf;

template <size_t... Arity>
struct MeosKernelVariantOf<std::index_sequence<Arity...>>
{
    using type = std::variant<MeosKernelOfArity<Arity>...>;
};

/// A kernel of any arity up to MEOS_KERNEL_MAX_ARITY, 0 for a call leaving every argument to its default
using MeosKernel = typename MeosKernelVariantOf<std::make_index_sequence<MEOS_KERNEL_MAX_ARITY + 1>>::type;

/// A MEOS function over one SQL signature: it evaluates its arguments, hands them to the kernel and reads
/// back what it answers. A variable-sized argument is passed where the record keeps it; a number is cast
/// to the type the signature states and written in a slot of the arena. A null argument answers null
/// without calling the kernel, as a MobilityDB function declared STRICT does.
class MeosPhysicalFunction final
{
public:
    MeosPhysicalFunction(
        MeosKernel kernel, std::vector<DataType::Type> argumentTypes, DataType::Type resultType, std::vector<PhysicalFunction> arguments);

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    MeosKernel kernel;
    std::vector<DataType::Type> argumentTypes;
    DataType::Type resultType;
    std::vector<PhysicalFunction> arguments;
};

static_assert(PhysicalFunctionConcept<MeosPhysicalFunction>);

}
