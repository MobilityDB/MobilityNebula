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
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/ExtensionType.hpp>

/// The MEOS types and functions as the catalog states them, in the tables tools/codegen/codegen_nebula_sql.py
/// writes, which the plugin registers with the plugin loader: the types and the logical functions with the
/// logical operators, the kernels and the readers of the types' text with the physical operators.
namespace NES
{

/// A MEOS type: the extension type carrying it, which reads the bytes of the value a text states, and the
/// text of the value its bytes carry
struct MeosType
{
    const ExtensionType* type;
    std::string (*toText)(std::span<const std::byte>);
};

/// Every MEOS type, and `bytea`, the WKB a reader takes and a writer answers
std::span<const MeosType> meosTypes();

}
