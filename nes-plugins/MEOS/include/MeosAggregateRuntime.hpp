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
#include <vector>
#include <MeosKernelRuntime.hpp>

/// An aggregate of the MEOS catalog over its MEOS state, as tools/codegen/codegen_nebula_sql.py
/// generates each entry of the catalog's `aggregates` section, and as MobilityDuck's
/// src/include/mobilityduck/meos_aggregate.hpp runs the same entries in DuckDB. The catalog states,
/// per CREATE AGGREGATE of MobilityDB, the public MEOS function of each role: the transition folds the
/// arguments of one row into the state, the combine joins two partial states, the final answers the
/// result. Nothing else is stated per aggregate, so this one template runs every one of them from an
/// operations struct the generator writes per catalog entry:
///
///   Row                    the decoded arguments of one row
///   GATHERS                true when the aggregate states no combine
///   HAS_FINAL, KEEPS       whether it states a final, and whether that final takes its state const,
///                          keeping it
///   Read(row, a0, block)   decode the argument a MeosAggregationPhysicalFunction hands over and the query
///                          literals of its block (#literal) into row; false when the argument holds no value
///   Transition(state, row) the state with the row folded in
///   Free(row)              release what Read decoded
///   Combine(a, b)          the two partial states joined
///   Final(state)           the result over the state
///   Write(result, out)     the answer of the result, released, in the form a MEOS kernel answers
///   Order(row)             the start timestamp of the first argument of a row
///
/// A combine answers in the place of its first state, returning it or freeing it for a larger one
/// (set_union_combinefn), except that a skip-list combine answers its second state when the first is
/// empty (temporal_tagg_combinefn): the state it does not answer is released. A final consumes its state
/// (set_union_finalfn, temporal_tagg_finalfn), so a state is released by releasing what its final
/// answers, except a final taking its state const, which keeps it (temporal_append_finalfn answers a
/// compact copy): that state is released after its final.
///
/// An aggregate stating no combine is the constructor of a temporal value fed one row at a time:
/// appendInstantAgg builds a sequence from instants and appendSequenceAgg a sequence set from sequences,
/// each transition accepting a value only after those already folded in time. NebulaStream hands the rows
/// of a window in no order and joins the partial states of its slices and worker threads, so such an
/// aggregate gathers its rows, a partial state joining another by taking its rows, and folds them when it
/// finishes, ordered by the start timestamp of their first argument: the answer MobilityDB gives over the
/// same rows ordered by time.
///
/// The state lives in the memory NebulaStream keeps for each aggregation of a window and key, a slot of
/// StateSize bytes it hands over with no alignment, so the state is copied in and out of it.
namespace NES::MeosRuntime
{

template <class OPS>
struct MeosAggregate
{
    using Row = typename OPS::Row;

    struct State
    {
        void* state;
        std::vector<Row>* rows;
    };

    static constexpr size_t StateSize = sizeof(State);

    static State load(const int8_t* slot)
    {
        State s;
        std::memcpy(&s, slot, sizeof(State));
        return s;
    }

    static void store(int8_t* slot, const State& s) { std::memcpy(slot, &s, sizeof(State)); }

    static void Initialize(int8_t* slot) { store(slot, State{nullptr, nullptr}); }

    /// The state with the row folded in; the row is released either way
    static void Fold(State& s, Row& row)
    {
        s.state = OPS::Transition(s.state, row);
        OPS::Free(row);
    }

    static void Release(void* state)
    {
        if (state == nullptr)
        {
            return;
        }
        if (not OPS::HAS_FINAL or OPS::KEEPS)
        {
            std::free(state);
        }
        else
        {
            std::free(OPS::Final(state));
        }
    }

    static void ReleaseRows(State& s)
    {
        if (s.rows == nullptr)
        {
            return;
        }
        for (auto& row : *s.rows)
        {
            OPS::Free(row);
        }
        delete s.rows;
        s.rows = nullptr;
    }

    /// The gathered rows folded into the state, in the order of their start timestamps
    static void FoldGathered(State& s)
    {
        if (s.rows == nullptr)
        {
            return;
        }
        auto& rows = *s.rows;
        std::stable_sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) { return OPS::Order(a) < OPS::Order(b); });
        for (auto& row : rows)
        {
            Fold(s, row);
        }
        delete s.rows;
        s.rows = nullptr;
    }

    static void Update(int8_t* slot, int8_t* a0, int8_t* block)
    {
        initialized();
        Row row{};
        if (not OPS::Read(row, a0, block))
        {
            return;
        }
        auto s = load(slot);
        if (OPS::GATHERS)
        {
            if (s.rows == nullptr)
            {
                s.rows = new std::vector<Row>();
            }
            s.rows->push_back(row);
        }
        else
        {
            Fold(s, row);
        }
        store(slot, s);
    }

    /// The source state joined into the target, the source left empty
    static void Combine(int8_t* target, int8_t* source)
    {
        initialized();
        auto a = load(target);
        auto b = load(source);
        if (OPS::GATHERS)
        {
            if (b.rows != nullptr)
            {
                if (a.rows == nullptr)
                {
                    a.rows = b.rows;
                }
                else
                {
                    a.rows->insert(a.rows->end(), b.rows->begin(), b.rows->end());
                    delete b.rows;
                }
                b.rows = nullptr;
            }
        }
        else
        {
            void* x = a.state;
            void* y = b.state;
            b.state = nullptr;
            if (x == nullptr)
            {
                a.state = y;
            }
            else if (y != nullptr)
            {
                void* r = OPS::Combine(x, y);
                Release(r == y ? x : y);
                a.state = r;
            }
        }
        store(target, a);
        store(source, b);
    }

    /// The answer of the result over a state, which is left empty; a number is answered in `out`
    static int8_t* Finish(int8_t* slot, int8_t* out)
    {
        initialized();
        auto s = load(slot);
        if (OPS::GATHERS)
        {
            FoldGathered(s);
        }
        void* state = s.state;
        s.state = nullptr;
        store(slot, s);
        void* r = state == nullptr ? nullptr : OPS::HAS_FINAL ? OPS::Final(state) : state;
        if (state != nullptr and OPS::KEEPS and r != state)
        {
            std::free(state);
        }
        if (r == nullptr)
        {
            std::memset(out, 0, sizeof(int64_t));
            return none();
        }
        return OPS::Write(r, out);
    }

    /// A state left by an error or never finished released
    static void Destroy(int8_t* slot)
    {
        auto s = load(slot);
        Release(s.state);
        s.state = nullptr;
        ReleaseRows(s);
        store(slot, s);
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
    std::memcpy(value, reference + sizeof(uint32_t), length);
    return value;
}

}
