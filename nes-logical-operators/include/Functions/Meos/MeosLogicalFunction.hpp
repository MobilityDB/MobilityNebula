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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableVariantDescriptor.pb.h>

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

/// A MEOS function called by its SQL name, as the MEOS catalog states it (eIntersects, tgeompointFromText,
/// ...). The function is one name over several signatures, as in MobilityDB; the signature is chosen from
/// the data types of the arguments once they are inferred, and names the physical function lowering it.
/// The names and their signatures are generated from the catalog by tools/codegen/codegen_nebula_sql.py.
class MeosLogicalFunction final : public LogicalFunctionConcept
{
public:
    MeosLogicalFunction(std::string_view name, std::span<const MeosOverload> overloads, std::vector<LogicalFunction> children);

    [[nodiscard]] SerializableFunction serialize() const override;
    [[nodiscard]] bool operator==(const LogicalFunctionConcept& rhs) const override;
    [[nodiscard]] DataType getDataType() const override;
    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;
    /// The physical function of the chosen signature, else the SQL name
    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

private:
    /// The signature whose argument types the children's are, else the first one whose numeric arguments the
    /// children's numbers can be cast to; none while a child's type is not yet inferred
    [[nodiscard]] std::optional<size_t> chooseOverload() const;

    std::string_view name;
    std::span<const MeosOverload> overloads;
    std::vector<LogicalFunction> children;
    std::optional<size_t> chosen;
    DataType dataType;
};

}

FMT_OSTREAM(NES::MeosLogicalFunction);
