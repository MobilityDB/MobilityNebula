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

#include <string>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>

namespace NES::Windowing
{

/// A SessionWindow groups the records of each key into sessions: a session holds the records of a key whose
/// event times follow one another by less than the gap, and it closes once the key is silent for the gap.
/// The window of a session starts at its first event time and ends at its last event time plus the gap.
/// The engine cuts time into slices one gap long, so getSize() and getSlide() both answer the gap.
class SessionWindow final : public TimeBasedWindowType
{
public:
    SessionWindow(TimeCharacteristic timeCharacteristic, TimeMeasure gap);

    [[nodiscard]] TimeMeasure getGap() const;
    TimeMeasure getSize() override;
    TimeMeasure getSlide() override;

    [[nodiscard]] std::string toString() const override;

    bool operator==(const WindowType& otherWindowType) const override;

private:
    const TimeMeasure gap;
};

}
