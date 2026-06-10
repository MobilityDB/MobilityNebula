// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// libc++ does not provide std::char_traits<unsigned char> (the standard only
// mandates the char/wchar_t/char8_t/char16_t/char32_t specializations, and
// libstdc++'s generic fallback is non-standard). Eclipse Paho's C++ wrapper
// stores binary payloads in std::basic_string<unsigned char>, so it fails to
// build under -stdlib=libc++. The standard permits a program to define a
// char_traits specialization for a non-standard character type; provide a
// minimal, conforming one so Paho builds with the NebulaStream toolchain.
#ifndef NES_PAHO_UCHAR_CHAR_TRAITS_H
#define NES_PAHO_UCHAR_CHAR_TRAITS_H

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ios>
#include <string>

namespace std {
template <>
struct char_traits<unsigned char>
{
    using char_type = unsigned char;
    using int_type = int;
    using off_type = std::streamoff;
    using pos_type = std::streampos;
    using state_type = std::mbstate_t;

    static void assign(char_type& a, const char_type& b) noexcept { a = b; }
    static constexpr bool eq(char_type a, char_type b) noexcept { return a == b; }
    static constexpr bool lt(char_type a, char_type b) noexcept { return a < b; }

    static int compare(const char_type* a, const char_type* b, size_t n)
    {
        return n == 0 ? 0 : std::memcmp(a, b, n);
    }
    static size_t length(const char_type* s)
    {
        size_t i = 0;
        while (s[i] != char_type(0)) { ++i; }
        return i;
    }
    static const char_type* find(const char_type* s, size_t n, const char_type& c)
    {
        for (size_t i = 0; i < n; ++i) { if (s[i] == c) { return s + i; } }
        return nullptr;
    }
    static char_type* move(char_type* d, const char_type* s, size_t n)
    {
        return n == 0 ? d : static_cast<char_type*>(std::memmove(d, s, n));
    }
    static char_type* copy(char_type* d, const char_type* s, size_t n)
    {
        return n == 0 ? d : static_cast<char_type*>(std::memcpy(d, s, n));
    }
    static char_type* assign(char_type* d, size_t n, char_type c)
    {
        if (n != 0) { std::memset(d, c, n); }
        return d;
    }

    static constexpr int_type not_eof(int_type c) noexcept { return c == eof() ? 0 : c; }
    static constexpr char_type to_char_type(int_type c) noexcept { return static_cast<char_type>(c); }
    static constexpr int_type to_int_type(char_type c) noexcept { return static_cast<int_type>(c); }
    static constexpr bool eq_int_type(int_type a, int_type b) noexcept { return a == b; }
    static constexpr int_type eof() noexcept { return static_cast<int_type>(EOF); }
};
} // namespace std

#endif // NES_PAHO_UCHAR_CHAR_TRAITS_H
