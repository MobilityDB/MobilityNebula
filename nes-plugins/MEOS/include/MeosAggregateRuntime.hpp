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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <MeosKernelRuntime.hpp>

/// An aggregate of the MEOS catalog over its MEOS state, as tools/codegen/codegen_nebula_sql.py
/// generates each entry of the catalog's `aggregates` section, and as MobilityDuck's
/// src/include/mobilityduck/meos_aggregate.hpp runs the same entries in DuckDB. The catalog states,
/// per CREATE AGGREGATE of MobilityDB, the public MEOS function of each role: the transition folds the
/// arguments of one row into the state and the final answers the result. Nothing else is stated per
/// aggregate, so this one template runs every one of them from an operations struct the generator writes
/// per catalog entry:
///
///   Row                    the decoded arguments of one row
///   GATHERS                true when the aggregate states no combine, its rows folded in time order
///   HAS_FINAL, KEEPS       whether it states a final, and whether that final takes its state const,
///                          keeping it
///   Read(row, a0, block)   decode the argument a MeosAggregationPhysicalFunction hands over and the query
///                          literals of its block (#literal) into row; false when the argument holds no value
///   Transition(state, row) the state with the row folded in
///   Free(row)              release what Read decoded
///   Final(state)           the result over the state
///   Write(result, out)     the answer of the result, released, in the form a MEOS kernel answers
///   Order(row)             the start timestamp of the first argument of a row
///   Release(state)         a state released without its result: the free of its type the catalog states,
///                          else as its final releases it
///
/// A final consumes its state (set_union_finalfn, temporal_tagg_finalfn), except a final taking its state
/// const, which keeps it (temporal_append_finalfn answers a compact copy): that state is released after its
/// final.
///
/// NebulaStream keeps the state of each aggregation of a window and key in a slot of the hash map of a slice,
/// and joins the state of a slice into the state of every window the slice lies in: a sliding window joins a
/// slice into several windows, so a join reads its source and leaves it whole. It releases a slice by
/// releasing the buffers of its hash map and calls no function of the aggregation on it, so the state lives
/// in buffers the hash map holds as its children, as the median keeps its paged vector, and nothing a kernel
/// allocates outlives the kernel. The state is the rows of the window: the slot holds the indexes of the
/// children keeping the bytes of their arguments, a join copies the rows of its source, and the finish decodes
/// them and folds them by the transition. An aggregate stating no combine is the constructor of a temporal
/// value fed one row at a time (appendInstantAgg builds a sequence from instants), each transition accepting a
/// value only after those already folded in time, so its rows are folded ordered by the start timestamp of
/// their first argument: the answer MobilityDB gives over the same rows ordered by time.
namespace NES::MeosRuntime
{

/// The slot of the answer of a result: a number of 8 bytes, then the null flag Finish sets when the aggregate
/// folded no value
inline constexpr size_t AnswerNullAt = sizeof(int64_t);
inline constexpr size_t AnswerSize = AnswerNullAt + sizeof(int64_t);

/// The index of no child buffer
inline constexpr uint32_t NoBuffer = std::numeric_limits<uint32_t>::max();

/// The bytes a child buffer keeps start after a header of 8 bytes, aligned for any MEOS type
inline constexpr size_t Header = sizeof(uint64_t);

/// The smallest child buffer the rows are kept in
inline constexpr size_t SmallestBuffer = 256;

/// The size rounded up to the alignment of a header
inline size_t aligned(const size_t size)
{
    return (size + Header - 1) / Header * Header;
}

inline uint32_t indexAt(const int8_t* slot, const size_t at)
{
    uint32_t index = 0;
    std::memcpy(&index, slot + at, sizeof(index));
    return index;
}

inline void setIndex(int8_t* slot, const size_t at, const uint32_t index)
{
    std::memcpy(slot + at, &index, sizeof(index));
}

/// The memory of the child buffer of the parent at the index, which the parent keeps as long as it lives
inline int8_t* childMemory(const TupleBuffer* parent, const uint32_t index)
{
    auto child = parent->loadChildBuffer(ChildBufferIndex{index});
    return child.getAvailableMemoryArea<int8_t>().data();
}

inline size_t childSize(const TupleBuffer* parent, const uint32_t index)
{
    return parent->loadChildBuffer(ChildBufferIndex{index}).getBufferSize();
}

/// A new child buffer of the parent of at least the size, its index
inline uint32_t newChild(TupleBuffer* parent, AbstractBufferProvider* provider, const size_t size)
{
    auto buffer = provider->getUnpooledBuffer(std::max(size, SmallestBuffer));
    if (not buffer)
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer available for the state of a MEOS aggregate");
    }
    return parent->storeChildBuffer(*buffer).getRawValue();
}

/// The length of the bytes the memory keeps after its header
inline size_t lengthAt(const int8_t* memory)
{
    uint64_t length = 0;
    std::memcpy(&length, memory, sizeof(length));
    return length;
}

/// The rows a slot gathers are kept in a chain of child buffers, the slot holding the index of the first and the
/// last: each buffer holds the index of the next and the bytes it uses, then the rows, each its length and its
/// bytes
inline constexpr size_t FirstAt = 0;
inline constexpr size_t LastAt = sizeof(uint32_t);
inline constexpr size_t UsedAt = sizeof(uint64_t);
inline constexpr size_t RowsAt = 2 * sizeof(uint64_t);
inline constexpr size_t RowsBuffer = 4096;

inline void appendRow(int8_t* slot, TupleBuffer* parent, AbstractBufferProvider* provider, const int8_t* data, const size_t size)
{
    const size_t needed = Header + aligned(size);
    auto last = indexAt(slot, LastAt);
    uint64_t used = 0;
    if (last != NoBuffer)
    {
        used = lengthAt(childMemory(parent, last) + UsedAt);
    }
    if (last == NoBuffer or childSize(parent, last) < used + needed)
    {
        const auto next = newChild(parent, provider, std::max(RowsBuffer, RowsAt + needed));
        setIndex(childMemory(parent, next), 0, NoBuffer);
        used = RowsAt;
        if (last == NoBuffer)
        {
            setIndex(slot, FirstAt, next);
        }
        else
        {
            setIndex(childMemory(parent, last), 0, next);
        }
        setIndex(slot, LastAt, next);
        last = next;
    }
    auto* memory = childMemory(parent, last);
    const uint64_t length = size;
    std::memcpy(memory + used, &length, sizeof(length));
    std::memcpy(memory + used + Header, data, size);
    used += needed;
    std::memcpy(memory + UsedAt, &used, sizeof(used));
}

/// Each row the slot gathers handed to the function, as its bytes and their length
template <typename F>
void forEachRow(const int8_t* slot, const TupleBuffer* parent, F&& function)
{
    auto index = indexAt(slot, FirstAt);
    while (index != NoBuffer)
    {
        auto* memory = childMemory(parent, index);
        const auto used = lengthAt(memory + UsedAt);
        for (size_t at = RowsAt; at < used;)
        {
            const auto length = lengthAt(memory + at);
            function(memory + at + Header, length);
            at += Header + aligned(length);
        }
        index = indexAt(memory, 0);
    }
}

template <class OPS>
struct MeosAggregate
{
    using Row = typename OPS::Row;

    /// The indexes of the first and the last child buffer of the rows
    static constexpr size_t StateSize = 2 * sizeof(uint32_t);

    static void Initialize(int8_t* slot)
    {
        setIndex(slot, FirstAt, NoBuffer);
        setIndex(slot, LastAt, NoBuffer);
    }

    /// The argument of the row gathered: the bytes of a variable-sized one, the slot of a number
    static void Update(int8_t* slot, TupleBuffer* parent, AbstractBufferProvider* provider, int8_t* argument, const bool varsized)
    {
        if (varsized)
        {
            appendRow(slot, parent, provider, contentOf(argument), lengthOf(argument));
        }
        else
        {
            appendRow(slot, parent, provider, argument, sizeof(int64_t));
        }
    }

    /// The rows of the source slot copied into the target's; the source is left whole, a slice joining every window
    /// it lies in
    static void
    Combine(int8_t* target, TupleBuffer* targetParent, int8_t* source, TupleBuffer* sourceParent, AbstractBufferProvider* provider)
    {
        forEachRow(
            source, sourceParent, [&](const int8_t* data, const size_t size) { appendRow(target, targetParent, provider, data, size); });
    }

    /// The state the rows the slot gathers fold into, laid in memory MEOS allocates; none when no row holds a value
    static void* Folded(const int8_t* slot, const TupleBuffer* parent, int8_t* block, const bool varsized)
    {
        std::vector<Row> rows;
        forEachRow(
            slot,
            parent,
            [&](const int8_t* data, const size_t size)
            {
                /// A variable-sized argument is handed over as the Argument naming its bytes, a number as its slot
                int8_t named[sizeof(Argument)];
                int8_t number[sizeof(int64_t)];
                if (varsized)
                {
                    const Argument argument{.size = size, .content = data};
                    std::memcpy(named, &argument, sizeof(argument));
                }
                else
                {
                    std::memcpy(number, data, sizeof(number));
                }
                Row row{};
                if (OPS::Read(row, varsized ? named : number, block))
                {
                    rows.push_back(row);
                }
            });
        if constexpr (OPS::GATHERS)
        {
            std::stable_sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) { return OPS::Order(a) < OPS::Order(b); });
        }
        void* state = nullptr;
        for (auto& row : rows)
        {
            state = OPS::Transition(state, row);
            OPS::Free(row);
        }
        return state;
    }

    /// The answer of the result over the rows the slot gathers, which it keeps: a variable-sized value as a kernel
    /// answers it, a number in `out`, and in either case the null flag at AnswerNullAt of `out`, set when the
    /// aggregate folded no value, as an aggregate over no row answers null in MobilityDB
    static int8_t* Finish(int8_t* slot, TupleBuffer* parent, int8_t* block, const bool varsized, int8_t* out)
    {
        initialized();
        void* state = Folded(slot, parent, block, varsized);
        void* r = state == nullptr ? nullptr : OPS::HAS_FINAL ? OPS::Final(state) : state;
        if (state != nullptr and OPS::KEEPS and r != state)
        {
            OPS::Release(state);
        }
        std::memset(out, 0, AnswerSize);
        if (r == nullptr)
        {
            out[AnswerNullAt] = 1;
            return none();
        }
        return OPS::Write(r, out);
    }
};

/// The query literal k of the block a MeosAggregationPhysicalFunction hands over, in the form a kernel takes
/// an argument of its type
inline int8_t* literal(int8_t* block, const size_t k)
{
    uint32_t offset = 0;
    std::memcpy(&offset, block + k * sizeof(uint32_t), sizeof(offset));
    return block + offset;
}

/// The memory image of the variable-sized argument at the reference, laid in memory malloc allocates, as
/// MEOS allocates, which aligns it for any type; none for an empty value
inline void* laid(const int8_t* reference)
{
    const auto length = lengthOf(reference);
    if (length == 0)
    {
        return nullptr;
    }
    void* value = std::malloc(length);
    std::memcpy(value, contentOf(reference), length);
    return value;
}

}
