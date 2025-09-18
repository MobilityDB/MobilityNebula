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

#include <SinksParsing/JSONFormat.hpp>

#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <iostream>
#include <memory>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

namespace NES::Sinks {

namespace {
std::string encodeBase64(std::string_view input) {
  if (input.empty()) {
    return {};
  }
  static constexpr char kAlphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string encoded;
  encoded.reserve(((input.size() + 2) / 3) * 4);

  size_t idx = 0;
  const auto size = input.size();
  while (idx + 2 < size) {
    const auto block =
        static_cast<uint32_t>(static_cast<unsigned char>(input[idx])) << 16 |
        static_cast<uint32_t>(static_cast<unsigned char>(input[idx + 1])) << 8 |
        static_cast<uint32_t>(static_cast<unsigned char>(input[idx + 2]));

    encoded.push_back(kAlphabet[(block >> 18) & 0x3F]);
    encoded.push_back(kAlphabet[(block >> 12) & 0x3F]);
    encoded.push_back(kAlphabet[(block >> 6) & 0x3F]);
    encoded.push_back(kAlphabet[block & 0x3F]);
    idx += 3;
  }

  const auto tail = size - idx;
  if (tail == 1) {
    const auto block =
        static_cast<uint32_t>(static_cast<unsigned char>(input[idx])) << 16;
    encoded.push_back(kAlphabet[(block >> 18) & 0x3F]);
    encoded.push_back(kAlphabet[(block >> 12) & 0x3F]);
    encoded.push_back('=');
    encoded.push_back('=');
  } else if (tail == 2) {
    const auto block =
        static_cast<uint32_t>(static_cast<unsigned char>(input[idx])) << 16 |
        static_cast<uint32_t>(static_cast<unsigned char>(input[idx + 1])) << 8;
    encoded.push_back(kAlphabet[(block >> 18) & 0x3F]);
    encoded.push_back(kAlphabet[(block >> 12) & 0x3F]);
    encoded.push_back(kAlphabet[(block >> 6) & 0x3F]);
    encoded.push_back('=');
  }

  return encoded;
}
} // namespace

JSONFormat::JSONFormat(Schema pSchema) : Format(std::move(pSchema)) {
  PRECONDITION(schema.getNumberOfFields() != 0,
               "Formatter expected a non-empty schema");
  size_t offset = 0;
  for (const auto &field : schema.getFields()) {
    const auto physicalType = field.dataType;
    formattingContext.offsets.push_back(offset);
    offset += physicalType.getSizeInBytes();
    formattingContext.physicalTypes.emplace_back(physicalType);
    formattingContext.names.emplace_back(field.name);
  }
  formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string
JSONFormat::getFormattedBuffer(const Memory::TupleBuffer &inputBuffer) const {
  return tupleBufferToFormattedJSONString(inputBuffer, formattingContext);
}

std::string JSONFormat::tupleBufferToFormattedJSONString(
    Memory::TupleBuffer tbuffer, const FormattingContext &formattingContext) {
  std::stringstream ss;
  auto numberOfTuples = tbuffer.getNumberOfTuples();
  auto buffer = std::span(tbuffer.getBuffer<char>(),
                          numberOfTuples * formattingContext.schemaSizeInBytes);
  for (size_t i = 0; i < numberOfTuples; i++) {
    auto tuple = buffer.subspan(i * formattingContext.schemaSizeInBytes,
                                formattingContext.schemaSizeInBytes);
    auto fields =
        std::views::iota(static_cast<size_t>(0),
                         formattingContext.offsets.size()) |
        std::views::transform([&](const auto &index) {
          auto type = formattingContext.physicalTypes[index];
          auto offset = formattingContext.offsets[index];
          if (type.type == DataType::Type::VARSIZED) {
            auto childIdx = *std::bit_cast<const uint32_t *>(&tuple[offset]);
            const auto raw =
                Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
            // Trajectories and other VARSIZED payloads are binary; expose them
            // as base64 in JSON.
            const auto encoded = encodeBase64(raw);
            return fmt::format(R"("{}":"{}")",
                               formattingContext.names.at(index), encoded);
          }
          return fmt::format("\"{}\":{}", formattingContext.names.at(index),
                             type.formattedBytesToString(&tuple[offset]));
        });

    ss << fmt::format("{{{}}}\n", fmt::join(fields, ","));
  }
  return ss.str();
}

std::ostream &operator<<(std::ostream &out, const JSONFormat &format) {
  return out << fmt::format("JSONFormat(Schema: {})", format.schema);
}

} // namespace NES::Sinks
