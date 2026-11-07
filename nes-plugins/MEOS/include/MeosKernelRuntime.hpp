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
#include <cstdlib>
#include <cstring>
#include <string>
#include <MEOSWrapper.hpp>

/// What the generated MEOS kernels share: reading an argument a MeosPhysicalFunction hands over, and
/// answering in the form it reads back. A variable-sized argument points at its length, 4 bytes, then its
/// bytes; a number points at the slot it is written in.
namespace NES::MeosRuntime
{

/// MEOS initialized once for the process, as the plugin's wrapper initializes it
inline void initialized()
{
    static const bool done = []
    {
        MEOS::Meos::ensureMeosInitialized();
        return true;
    }();
    static_cast<void>(done);
}

/// The length of the variable-sized value at the reference
inline uint32_t lengthOf(const int8_t* reference)
{
    uint32_t length = 0;
    std::memcpy(&length, reference, sizeof(length));
    return length;
}

/// A MEOS value carried as its memory image, laid in memory malloc allocates, as MEOS allocates, which
/// aligns it for any type; an empty value is no value
class Value
{
public:
    explicit Value(const int8_t* reference) : length(lengthOf(reference))
    {
        if (length > 0)
        {
            laid = std::malloc(length);
            std::memcpy(laid, reference + sizeof(uint32_t), length);
        }
    }
    Value(const Value&) = delete;
    Value& operator=(const Value&) = delete;
    ~Value() { std::free(laid); }

    [[nodiscard]] bool empty() const { return laid == nullptr; }
    /// The value, as MEOS takes it: some functions take their argument as a pointer to non-const
    [[nodiscard]] void* get() const { return laid; }
    /// The bytes of the value, a WKB or a string for a value carried in its codec's form
    [[nodiscard]] const uint8_t* bytes() const { return static_cast<const uint8_t*>(laid); }
    [[nodiscard]] size_t size() const { return length; }

private:
    uint32_t length;
    void* laid = nullptr;
};

/// The string a variable-sized argument holds
inline std::string text(const int8_t* reference)
{
    std::string value(lengthOf(reference), '\0');
    std::memcpy(value.data(), reference + sizeof(uint32_t), value.size());
    return value;
}

/// The number written in a slot
template <typename T>
T number(const int8_t* slot)
{
    T value;
    std::memcpy(&value, slot, sizeof(T));
    return value;
}

/// The answer of no value
inline int8_t* none()
{
    return nullptr;
}

/// The answer of the size bytes at data, as a buffer malloc allocates: their length, then the bytes
inline int8_t* bytes(const void* data, const size_t size)
{
    auto* answer = static_cast<int8_t*>(std::malloc(sizeof(uint32_t) + size));
    const auto length = static_cast<uint32_t>(size);
    std::memcpy(answer, &length, sizeof(length));
    std::memcpy(answer + sizeof(uint32_t), data, size);
    return answer;
}

/// The answer of the memory image of a value MEOS allocated, which it frees
inline int8_t* image(void* value, const size_t size)
{
    if (value == nullptr)
    {
        return none();
    }
    auto* answer = bytes(value, size);
    std::free(value);
    return answer;
}

/// The answer of a string MEOS allocated, which it frees
inline int8_t* string(char* value)
{
    if (value == nullptr)
    {
        return none();
    }
    auto* answer = bytes(value, std::strlen(value));
    std::free(value);
    return answer;
}

/// The answer of a number, written in the slot the caller reads it from
template <typename T>
int8_t* number(int8_t* out, const T value)
{
    std::memcpy(out, &value, sizeof(T));
    return none();
}

}
