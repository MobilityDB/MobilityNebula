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

namespace NES
{

/// A type a plugin adds over VARSIZED. Its values are byte strings whose form the plugin owns, as a
/// MEOS temporal point is the memory image of its value, and they are read from and written to
/// text through the plugin. A field of such a type is VARSIZED to every operator, so it is stored,
/// copied, hashed and shipped as any other variable-sized value; only the parser, the formatters and
/// the functions naming the type read it.
///
/// Each extension type is one object of static storage registered in the DataType registry under
/// its name, and a DataType refers to it by address, so two fields have the same extension type
/// exactly when they refer to the same object.
struct ExtensionType
{
    /// The name the type is declared by in a schema and registered under, as `TGEOMPOINT`.
    std::string_view name;
    /// The text of the value whose bytes are given.
    std::string (*toText)(std::span<const std::byte> bytes);
    /// The bytes of the value the text states. Throws if the text states no value of the type.
    std::vector<std::byte> (*fromText)(std::string_view text);
    /// The type of the elements of an array type, whose values are the number of their elements, then each element as
    /// a variable-sized value; null for a type of no array.
    const ExtensionType* element{nullptr};
};

}
