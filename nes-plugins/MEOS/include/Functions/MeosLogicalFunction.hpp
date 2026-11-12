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
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// One SQL signature of a MEOS function: the data types of its arguments and of its result, named as
/// the DataType registry names them (FLOAT64, TGEOMPOINT, ...), and the physical function computing it.
struct MeosOverload
{
    std::span<const std::string_view> arguments;
    std::string_view result;
    std::string_view physicalFunction;
};

/// A MEOS function as the catalog states it: its SQL name and its signatures, of static storage
struct MeosFunction
{
    std::string_view name;
    std::span<const MeosOverload> overloads;
};

/// Every MEOS function the catalog states, in the order of their SQL names, in the table
/// tools/codegen/codegen_nebula_sql.py writes from the catalog
std::span<const MeosFunction> meosFunctions();

/// The MEOS function of the SQL name, none for a name the catalog does not state
std::optional<MeosFunction> meosFunction(std::string_view name);

/// A MEOS function called by its SQL name, as the MEOS catalog states it (eIntersects, tgeompointFromText,
/// ...). The function is one name over several signatures, as in MobilityDB; the signature is chosen from
/// the data types of the arguments once they are inferred, and names the physical function lowering it.
/// A null argument makes the answer null, so the answer is nullable when an argument is.
class MeosLogicalFunction final
{
public:
    MeosLogicalFunction(std::string_view name, std::span<const MeosOverload> overloads, std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const MeosLogicalFunction& rhs) const;
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] MeosLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    /// The physical function of the chosen signature, else the SQL name
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] std::string_view getName() const { return name; }

private:
    /// The signature whose argument types the children's are, else the first one whose numeric arguments the
    /// children's numbers can be cast to; none while a child's type is not yet inferred
    [[nodiscard]] std::optional<size_t> chooseOverload() const;
    /// The type of the answer of the chosen signature, nullable when an argument is
    [[nodiscard]] DataType answerType() const;

    std::string_view name;
    std::span<const MeosOverload> overloads;
    std::vector<LogicalFunction> children;
    std::optional<size_t> chosen;
    DataType dataType;
};

namespace detail
{
struct ReflectedMeosLogicalFunction
{
    std::string name;
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<MeosLogicalFunction>
{
    MeosLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<MeosLogicalFunction>
{
    Reflected operator()(const MeosLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<MeosLogicalFunction>);

}

FMT_OSTREAM(NES::MeosLogicalFunction);
