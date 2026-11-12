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

#include <string_view>

namespace NES
{

/// A type a plugin adds over VARSIZED. Its values are byte strings whose form the plugin owns, as a
/// MEOS temporal point is the memory image of its value. A field of such a type is VARSIZED to every
/// operator, so it is stored, copied, hashed and shipped as any other variable-sized value; it is read
/// from text by the ValueDeserializer and written to text by the ValueSerializer the plugin registers
/// under the name of the type, and only the functions naming the type read its bytes.
///
/// Each extension type is one object of static storage registered in the DataType registry under
/// its name, and a DataType refers to it by address, so two fields have the same extension type
/// exactly when they refer to the same object.
struct ExtensionType
{
    /// The name the type is declared by in a schema and registered under, as `TGEOMPOINT`.
    std::string_view name;
};

}
