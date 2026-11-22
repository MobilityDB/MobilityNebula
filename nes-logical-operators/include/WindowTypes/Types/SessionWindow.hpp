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
#include <functional>
#include <ostream>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::Windowing
{

/// A SessionWindow groups the records of each key into sessions: a session holds the records of a key whose
/// event times follow one another by at most the gap, and its window is [first event time, last event time + gap).
/// The engine cuts time into slices one gap long, so the size and the slide of the window are both the gap.
class SessionWindow
{
public:
    explicit SessionWindow(TimeMeasure gap);

    [[nodiscard]] TimeMeasure getGap() const;
    [[nodiscard]] TimeMeasure getSize() const;

    bool operator==(const SessionWindow& otherWindowType) const;
    friend std::ostream& operator<<(std::ostream& os, const SessionWindow& sessionWindow);

private:
    TimeMeasure gap;
};

}

namespace NES
{
template <>
struct Reflector<Windowing::SessionWindow>
{
    Reflected operator()(const Windowing::SessionWindow& sessionWindow, const ReflectionContext& context) const;
};

template <>
struct Unreflector<Windowing::SessionWindow>
{
    Windowing::SessionWindow operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::Windowing::SessionWindow>
{
    std::size_t operator()(const NES::Windowing::SessionWindow& window) const noexcept;
};

FMT_OSTREAM(NES::Windowing::SessionWindow);
