#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The MobilityNebula SQL surface, generated from the MEOS catalog as MobilityFlink's is.

MobilityFlink and MobilitySpark take their SQL surface from one generator, JMEOS
`tools/codegen_jvm.py` (engines `flink-sql` and `spark-sql`), vendored unchanged in
`tools/codegen/jmeos/`. This generator reads the catalog through that generator's own model
(`SqlModel`), so a MEOS SQL type has in NebulaStream the C type, the codec and the carried
form it has in Flink and Spark.

A MEOS value crosses NebulaStream as the bytes of a VARSIZED field whose data type names its
MEOS type, an extension type (`nes-data-types/include/DataTypes/ExtensionType.hpp`). The
bytes are the value's memory image where the catalog states one for its C type
(typeEncodings[C].image), else the form its codec writes. Its text, which a source reads and a
sink writes, is the form the codec writes: the upper-case hex of the WKB for a byte codec, the
codec's string otherwise, the text MobilityFlink's value classes render.

Each type is registered under its catalog SQL name in upper case, as NebulaStream registers
every data type (INT8, VARSIZED), and a schema names it regardless of case. The header each
function is declared in is the catalog's `file` field.

Emitted:
  nes-plugins/MEOS/DataTypes/MeosDataTypes.cpp   one extension type per MEOS SQL type
  nes-plugins/MEOS/DataTypes/CMakeLists.txt      their registration in the DataType registry
  nes-plugins/MEOS/Functions/                    one logical function per SQL name, one kernel per SQL signature
  nes-plugins/MEOS/Aggregations/                 one logical aggregation per aggregate name, one operations
                                                 struct per overload, run by MeosAggregateRuntime.hpp as
                                                 MobilityDuck's meos_aggregate.hpp runs the same catalog entries

Usage:
  codegen_nebula_sql.py --catalog meos-idl.json --output-root <MobilityNebula root>
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

LICENSE = """/*
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
"""

CMAKE_LICENSE = """# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""


def load_jvm_generator():
    """JMEOS's generator, vendored unchanged; it loads its Spark sibling from its own directory."""
    path = HERE / 'jmeos' / 'codegen_jvm.py'
    spec = importlib.util.spec_from_file_location('codegen_jvm', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_catalog(path):
    """The MEOS catalog, as build_descriptor.load_catalog reads it."""
    with open(path) as fh:  # BINDING-HEADER-PARSE-OK: reads meos-idl.json, the catalog itself; no header is read
        return json.load(fh)


def sql_model(cj, cat):
    """JMEOS's SqlModel over the catalog. Where Flink binds the symbols of the JMEOS jar, NebulaStream
    calls libmeos itself, so the functions it may call are the public ones of the catalog."""
    callable_fns = {f['name']: {'ret': '', 'arg_types': []} for f in cat['functions'] if f.get('api') != 'internal'}
    return cj.SqlModel(cat, callable_fns)


def c_spelling(canonical):
    """A catalog `canonical` C type as C++ writes it in a declaration."""
    return canonical.replace(' *', '*').strip()


def literal(aux):
    """The C++ literal of the value the catalog states for a codec's trailing input, null where it
    states none (geo_as_hexewkb's endian, which MEOS reads as the machine's byte order)."""
    v = aux.get('default', 0)
    if v is None:
        return 'nullptr'
    return json.dumps(v) if isinstance(v, str) else str(v)


def ident(sql):
    return ''.join(p.capitalize() for p in sql.split('_'))


class TypeLayer:
    """The extension type of each MEOS SQL type the model gives a codec."""

    def __init__(self, cj, cat):
        self.m = sql_model(cj, cat)
        self.by_name = {f['name']: f for f in cat['functions']}
        self.variant = cj.WKB_VARIANT
        self.headers = set()

    def fn(self, name):
        """The catalog record of a function the types call; its `file` is included for its declaration."""
        f = self.by_name[name]
        self.headers.add(f['file'])
        return f

    def encoder_call(self, enc, value, aux):
        """The call of the codec's encoder on `value`: its stated trailing inputs, the WKB variant a
        writer taking one beside the value reads, and the length it answers in `size`."""
        named = {a['name']: a for a in aux}
        args = [value]
        for p in self.fn(enc)['params'][1:]:
            if c_spelling(p['canonical']) == 'size_t*':
                args.append('&size')
            elif p['name'] in named:
                args.append(literal(named[p['name']]))
            elif p['name'] == 'variant':
                args.append(str(self.variant))
            else:
                raise SystemExit(f'{enc}: the catalog states no value for parameter {p["name"]}')
        return f'{enc}({", ".join(args)})'

    def decoder_call(self, kind, dec, aux):
        """The call of the codec's decoder on the WKB `wkb` or the terminated text `terminated`."""
        if kind == 'bytes':
            return f'{dec}(wkb.data(), wkb.size())'
        return f'{dec}(terminated.c_str(){"".join(", " + literal(a) for a in aux)})'

    def source(self, sql):
        """The C++ of the extension type of `sql`: its text writer, its text reader and its object."""
        kind, dec, enc, in_aux, out_aux = self.m.codec[sql]
        image = self.m.image.get(sql)
        ctype = c_spelling(self.fn(dec)['returnType']['canonical'])
        pointer = ctype.endswith('*')
        name = ident(sql)
        size_fn = image and image.get('size')
        carried = (f'memory image ({image["copy"]} / {size_fn or str(image["sizeBytes"]) + " bytes"})' if image
                   else f'{kind} form ({dec} / {enc})')
        text = 'the hex of its WKB' if kind == 'bytes' else f'its {kind} string'
        out = [f'/* {sql}: C type {self.m.sql_cbase[sql]}, carried as its {carried}, read and written as {text} */']

        # The text of the value the bytes carry; no bytes are no value, the answer of a function MEOS
        # answers none for, written as the empty text
        out += [f'std::string {name}ToText(std::span<const std::byte> bytes)', '{',
                '    if (bytes.empty())', '    {', '        return {};', '    }']
        if image:
            copy = self.fn(image['copy'])['name']
            base = ctype[:-1]
            out += ['    MEOS::Meos::ensureMeosInitialized();',
                    f'    auto* value = static_cast<{ctype}>(fromImage(bytes, [](const void* laid) -> void* '
                    f'{{ return {copy}(static_cast<const {base}*>(laid)); }}));',
                    '    size_t size = 0;',
                    f'    auto* written = {self.encoder_call(enc, "value", out_aux)};',
                    '    std::free(value);',
                    f'    if (written == nullptr) {{ cannotWrite("{sql}"); }}',
                    ('    auto text = hex(std::as_bytes(std::span{written, size}));' if kind == 'bytes'
                     else '    std::string text{written};'),
                    '    std::free(written);',
                    '    return text;']
        elif kind == 'bytes':
            # the bytes are the codec's form, the WKB itself
            out.append('    return hex(bytes);')
        else:
            # the bytes are the codec's string
            out.append('    return textOf(bytes);')
        out += ['}', '']

        # The bytes of the value the text states
        out += [f'std::vector<std::byte> {name}FromText(std::string_view text)', '{',
                '    MEOS::Meos::ensureMeosInitialized();']
        out.append('    const auto wkb = unhex(text);' if kind == 'bytes' else '    const std::string terminated{text};')
        out.append(f'    auto value = {self.decoder_call(kind, dec, in_aux)};')
        if pointer:
            out.append(f'    if (value == nullptr) {{ cannotRead("{sql}", text); }}')
        if image:
            size = f'static_cast<size_t>({self.fn(size_fn)["name"]}(value))' if size_fn else f'size_t{{{image["sizeBytes"]}}}'
            out += [f'    auto carried = image(value, {size});', '    std::free(value);', '    return carried;']
        else:
            # read to check it, then carried in the codec's form
            out.append('    std::free(value);' if pointer else '    static_cast<void>(value);')
            out.append('    return bytesOf(std::span{wkb});' if kind == 'bytes'
                       else '    return bytesOf(std::span{terminated});')
        out += ['}', '',
                f'constexpr ExtensionType {name}Type{{.name = "{sql.upper()}", .toText = {name}ToText, .fromText = {name}FromText}};',
                '']
        return '\n'.join(out)

    def bytea_source(self):
        """The C++ of the extension type of `bytea`, the WKB a reader takes and a writer answers, as JMEOS's
        #_arg and #_ret carry a bytea as a byte[]: its bytes carried as they are, read and written as their
        upper-case hex, as #source writes the bytes of a byte codec"""
        return '\n'.join([
            '/* bytea: the bytes of a WKB, carried as they are, read and written as their hex */',
            'std::string ByteaToText(std::span<const std::byte> bytes)', '{',
            '    if (bytes.empty())', '    {', '        return {};', '    }',
            '    return hex(bytes);', '}', '',
            'std::vector<std::byte> ByteaFromText(std::string_view text)', '{',
            '    const auto wkb = unhex(text);', '    return bytesOf(std::span{wkb});', '}', '',
            'constexpr ExtensionType ByteaType{.name = "BYTEA", .toText = ByteaToText, .fromText = ByteaFromText};', ''])

    def array_source(self, sql):
        """The C++ of the extension type of an array of `sql`, as MobilityFlink's ARRAY of the value class
        carries it: the number of elements, then each element's memory image after its length, written as
        the text of its elements in braces. An array passes between functions and is read from no text."""
        name = ident(sql)
        return '\n'.join([
            f'/* {sql}[]: an array of {sql}, carried as its number of elements, then each element\'s memory image after its length */',
            f'std::string {name}ArrayToText(std::span<const std::byte> bytes)', '{',
            '    if (bytes.empty())', '    {', '        return {};', '    }',
            '    uint32_t count = 0;', '    std::memcpy(&count, bytes.data(), sizeof(count));',
            '    size_t at = sizeof(count);', '    std::string text{"{"};',
            '    for (uint32_t i = 0; i < count; ++i)', '    {',
            '        uint32_t size = 0;', '        std::memcpy(&size, bytes.data() + at, sizeof(size));', '        at += sizeof(size);',
            '        if (i > 0)', '        {', '            text += ", ";', '        }',
            f'        text += {name}ToText(bytes.subspan(at, size));', '        at += size;', '    }',
            '    text += "}";', '    return text;', '}', '',
            f'std::vector<std::byte> {name}ArrayFromText(std::string_view text)', '{',
            f'    cannotRead("{sql}[]", text);', '}', '',
            f'constexpr ExtensionType {name}ArrayType{{.name = "{sql.upper()}_ARRAY", .toText = {name}ArrayToText, '
            f'.fromText = {name}ArrayFromText, .element = &{name}Type}};', ''])

    def registrar(self, sqls, arrays=()):
        out = ['}', '', 'namespace NES', '{', '']
        for sql in sqls:
            out += [f'DataTypeRegistryReturnType DataTypeGeneratedRegistrar::Register{sql.upper()}DataType(DataTypeRegistryArguments)',
                    '{',
                    f'    return DataType{{.type = DataType::Type::VARSIZED, .extension = &{ident(sql)}Type}};',
                    '}', '']
        for sql in arrays:
            out += [f'DataTypeRegistryReturnType DataTypeGeneratedRegistrar::Register{sql.upper()}_ARRAYDataType(DataTypeRegistryArguments)',
                    '{',
                    f'    return DataType{{.type = DataType::Type::VARSIZED, .extension = &{ident(sql)}ArrayType}};',
                    '}', '']
        out.append('}')
        return out


PREAMBLE = r'''
/* AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.
 * One extension type per MEOS SQL type, carrying the form MobilityFlink's value class carries. */

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <span>
#include <string>
#include <string_view>
#include <vector>

extern "C" {
@HEADERS@
}

#include <DataTypes/DataType.hpp>
#include <DataTypes/ExtensionType.hpp>
#include <MEOSWrapper.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace
{
using NES::ExtensionType;

[[noreturn]] void cannotWrite(std::string_view type)
{
    throw NES::FormattingError("MEOS cannot write a value of {}", type);
}

[[noreturn]] void cannotRead(std::string_view type, std::string_view text)
{
    throw NES::CannotFormatSourceData("MEOS reads no value of {} from '{}'", type, text);
}

/// The upper-case hex of the bytes, the text MobilityFlink renders a WKB in.
std::string hex(std::span<const std::byte> bytes)
{
    static constexpr std::string_view digits = "0123456789ABCDEF";
    std::string text;
    text.reserve(2 * bytes.size());
    for (const auto byte : bytes)
    {
        text.push_back(digits[std::to_integer<uint8_t>(byte) >> 4U]);
        text.push_back(digits[std::to_integer<uint8_t>(byte) & 0xFU]);
    }
    return text;
}

/// The bytes the hex states, as the MEOS WKB readers take them.
std::vector<uint8_t> unhex(std::string_view text)
{
    const auto digit = [text](const char c) -> uint8_t
    {
        if (c >= '0' and c <= '9')
        {
            return c - '0';
        }
        if (c >= 'A' and c <= 'F')
        {
            return c - 'A' + 10;
        }
        if (c >= 'a' and c <= 'f')
        {
            return c - 'a' + 10;
        }
        throw NES::CannotFormatSourceData("'{}' is not hexadecimal", text);
    };
    if (text.size() % 2 != 0)
    {
        throw NES::CannotFormatSourceData("'{}' has an odd number of hexadecimal digits", text);
    }
    std::vector<uint8_t> bytes(text.size() / 2);
    for (size_t i = 0; i < bytes.size(); ++i)
    {
        bytes[i] = static_cast<uint8_t>((digit(text[2 * i]) << 4U) | digit(text[2 * i + 1]));
    }
    return bytes;
}

/// A copy of the bytes the span holds.
template <typename T>
std::vector<std::byte> bytesOf(std::span<const T> held)
{
    const auto bytes = std::as_bytes(held);
    return {bytes.begin(), bytes.end()};
}

/// The string the bytes hold.
std::string textOf(std::span<const std::byte> bytes)
{
    std::string text(bytes.size(), '\0');
    std::memcpy(text.data(), bytes.data(), bytes.size());
    return text;
}

/// The value whose memory image the bytes hold, as the catalog's copy function copies it from the image
/// laid in memory malloc allocates, as MEOS allocates, which aligns it for any type.
void* fromImage(std::span<const std::byte> bytes, void* (*copy)(const void*))
{
    void* laid = std::malloc(bytes.size());
    std::memcpy(laid, bytes.data(), bytes.size());
    void* value = copy(laid);
    std::free(laid);
    return value;
}

/// The memory image of the value, the size bytes it is laid in.
std::vector<std::byte> image(const void* value, const size_t size)
{
    std::vector<std::byte> bytes(size);
    std::memcpy(bytes.data(), value, size);
    return bytes;
}

'''


# ───────────────────────── the functions ─────────────────────────
#
# Every SQL signature the catalog states for a public MEOS function becomes one overload of the
# NebulaStream function of its SQL name (MeosLogicalFunction), computed by one kernel: a C++ function
# reading the arguments a MeosPhysicalFunction hands it, calling MEOS and answering in the form it reads
# back (MeosKernelRuntime.hpp). The name is the signature's altSqlName where the catalog states one,
# else its sqlName, else the function's sqlfn, as SqlModel.signatures reads it for Flink and Spark.
# Every composition the catalog states (its top-level `compositions`: a SQL function MobilityDB writes as
# casts of its arguments and a call of another function, `expandSpace(tgeompoint, float)` being
# `expandSpace($1::stbox, $2)`) becomes one overload as well, its kernel calling the casts and then the
# call, as codegen_jvm._composition_overload builds its eval for Flink and Spark.

# The NebulaStream data type of each SQL scalar type, and the C type of the slot it is written in
SQL_SCALAR = {
    'boolean': ('BOOLEAN', 'bool'), 'smallint': ('INT16', 'int16_t'), 'integer': ('INT32', 'int32_t'),
    'bigint': ('INT64', 'int64_t'), 'float': ('FLOAT64', 'double'), 'double precision': ('FLOAT64', 'double'),
}
# The C types a number is passed as, and those a number answers in
C_NUMBER = {'bool', 'int', 'int16_t', 'int32_t', 'uint32_t', 'int64_t', 'uint64_t', 'double', 'uint8_t'}
KERNELS_PER_FILE = 250
# The largest arity MeosPhysicalFunction calls a kernel with (MEOS_KERNEL_MAX_ARITY)
MAX_ARITY = 13


def no_value(none):
    return f'    {{\n        return {none};\n    }}'


class FunctionLayer:
    """The overloads and kernels of every SQL signature the types of the TypeLayer carry."""

    def __init__(self, types, cat, taken, cj):
        self.cj = cj
        self.t = types
        self.m = types.m
        self.cat = cat
        # the names NebulaStream's own functions take, which a MEOS function cannot take as well
        self.taken = {n.upper() for n in taken}
        self.enums = {e['name'] for e in cat.get('enums', [])}
        self.members = {v['name'] for e in cat.get('enums', []) for v in (e.get('values') or []) if isinstance(v, dict)}
        self.overloads = {}   # SQL name -> [(NebulaStream argument types, result type, key)]
        self.kernels = []     # (key, C++ source, physical argument types, physical result type)
        self.left = {}        # reason -> count
        self.arrays = set()   # the SQL types an array of which some signature carries

    def leave(self, why):
        self.left[why] = self.left.get(why, 0) + 1

    def nes_type(self, sql):
        """The NebulaStream data type a SQL type is carried in, and the physical type of its value; an array of
        values carried as their memory image is a type of its own (TypeLayer.array_source)"""
        if sql.endswith('[]') and sql[:-2] in self.m.image:
            self.arrays.add(sql[:-2])
            return f'{sql[:-2].upper()}_ARRAY', 'VARSIZED'
        if sql in self.m.codec:
            return sql.upper(), 'VARSIZED'
        if sql == 'text':
            return 'VARSIZED', 'VARSIZED'
        if sql == 'bytea':
            return 'BYTEA', 'VARSIZED'
        if sql in SQL_SCALAR:
            return SQL_SCALAR[sql][0], SQL_SCALAR[sql][0]
        if sql == 'timestamptz':
            # NebulaStream's timestamp, milliseconds from the Unix epoch (MeosRuntime::timestamptz)
            return 'UINT64', 'UINT64'
        return None

    def bound_literal(self, value, ctype):
        """The C literal passing the value a wrapper binds to a parameter of C type `ctype`, as
        codegen_jvm._bound_literal reads it: a macro or enum member stands for its catalog value"""
        if value in self.members and ctype in self.enums:
            return value
        v = self.m.consts.get(value, value)
        if isinstance(v, bool):
            v = 'true' if v else 'false'
        if v in ('true', 'false'):
            return v if ctype == 'bool' else None
        if v == 'NULL':
            return 'nullptr' if ctype.endswith('*') else None
        try:
            x = float(v)
        except (TypeError, ValueError):
            return None
        if ctype == 'double':
            return repr(x)
        if x != int(x) or ctype not in C_NUMBER:
            return None
        return f'static_cast<{ctype}>({int(x)})'

    def argument(self, k, sql, p, pre, post):
        """The C expression passing argument k, of SQL type `sql`, to parameter p, with the statements
        reading it (`pre`) and freeing what they allocate (`post`); None where no rule passes it"""
        ctype = c_spelling(p['canonical'])
        base = ctype.replace('const ', '').replace('struct ', '').rstrip('*').strip()
        a = f'a{k}'
        if sql in self.m.codec:
            kind, dec, enc, in_aux, out_aux = self.m.codec[sql]
            pre.append(f'    const MeosRuntime::Value v{k}{{{a}}};')
            pre.append(f'    if (v{k}.empty())\n' + no_value('@NONE@'))
            if sql in self.m.image and ctype.endswith('*'):
                return f'static_cast<{base}*>(v{k}.get())'
            if kind == 'bytes' and not ctype.endswith('*'):
                self.t.fn(dec)
                return f'{dec}(v{k}.bytes(), v{k}.size())'
            if kind != 'bytes' and ctype.endswith('*'):
                # carried as the codec's string, decoded for the call
                self.t.fn(dec)
                pre.append(f'    auto* d{k} = {dec}(MeosRuntime::text({a}).c_str());')
                pre.append(f'    if (d{k} == nullptr)\n' + no_value('@NONE@'))
                post.append(f'    std::free(d{k});')
                return f'd{k}'
            return None
        if sql == 'text':
            pre.append(f'    const std::string s{k} = MeosRuntime::text({a});')
            if ctype == 'const char*':
                return f's{k}.c_str()'
            if ctype in ('text*', 'const text*'):
                self.t.fn('cstring_to_text')
                pre.append(f'    text* t{k} = cstring_to_text(s{k}.c_str());')
                post.append(f'    std::free(t{k});')
                return f't{k}'
            if ctype in self.m.enum_parser:
                parser = self.t.fn(self.m.enum_parser[ctype])['name']
                return f'{parser}(s{k}.c_str())'
            return None
        if sql == 'bytea' and base == 'uint8_t' and ctype.endswith('*'):
            # the WKB a reader takes, whose length SqlModel.visible folds into it (#_byte_arrays): the bytes
            # and their length fill the buffer and the length parameter
            pre.append(f'    const MeosRuntime::Value v{k}{{{a}}};')
            pre.append(f'    if (v{k}.empty())\n' + no_value('@NONE@'))
            return f'v{k}.bytes(), v{k}.size()'
        if sql in SQL_SCALAR and ctype in C_NUMBER:
            return f'static_cast<{ctype}>(MeosRuntime::number<{SQL_SCALAR[sql][1]}>({a}))'
        if sql == 'timestamptz' and ctype == 'TimestampTz':
            return f'MeosRuntime::timestamptz(MeosRuntime::number<uint64_t>({a}))'
        return None

    def array_argument(self, k, sql, p, pre):
        """The C array of the values an array argument k carries, as #_array_arg passes MEOS the array of
        the values a Flink ARRAY decodes to; MEOS reads its length off the parameter beside it"""
        ctype = c_spelling(p['canonical'])
        if sql[:-2] not in self.m.image or not ctype.endswith('**'):
            return None
        pre.append(f'    MeosRuntime::Array v{k}{{a{k}}};')
        pre.append(f'    if (v{k}.empty())\n' + no_value('@NONE@'))
        # through void*, since C++ refuses to cast void** to a const-qualified T**
        return f'static_cast<{ctype}>(static_cast<void*>(v{k}.get()))'

    def answer(self, sql, f, length=None):
        """The statements answering the result `r` of SQL type `sql`, and the answer of no value; None
        where no rule answers it. An array answer (shape.arrayReturn) holds `length` values."""
        rc = c_spelling(f['returnType']['canonical'])
        none = 'MeosRuntime::none()'
        if sql.endswith('[]'):
            image = self.m.image.get(sql[:-2])
            if not image or not rc.endswith('**') or length is None:
                return None
            if image.get('size'):
                sf = self.t.fn(image['size'])
                pt = c_spelling(sf['params'][0]['canonical'])
                size = f'[](const void* e) {{ return static_cast<size_t>({sf["name"]}(static_cast<{pt}>(e))); }}'
            else:
                size = f'[](const void*) {{ return size_t{{{image["sizeBytes"]}}}; }}'
            return [f'    return MeosRuntime::array(reinterpret_cast<void**>(r), static_cast<int>({length}), {size});'], none
        if sql in self.m.codec:
            kind, dec, enc, in_aux, out_aux = self.m.codec[sql]
            image = self.m.image.get(sql)
            if image and rc.endswith('*'):
                if image.get('size'):
                    # the size function takes the C type of the value, of which the result may be a subtype
                    # (a TSequence is a Temporal), as C converts it
                    sf = self.t.fn(image['size'])
                    pt = c_spelling(sf['params'][0]['canonical'])
                    size = f'static_cast<size_t>({sf["name"]}(static_cast<{pt}>(static_cast<const void*>(r))))'
                else:
                    size = f'size_t{{{image["sizeBytes"]}}}'
                return ['    if (r == nullptr)\n' + no_value(none), f'    return MeosRuntime::image(r, {size});'], none
            if kind == 'bytes' and not rc.endswith('*'):
                call = self.t.encoder_call(enc, 'r', out_aux)
                return ['    size_t size = 0;', f'    auto* w = {call};',
                        '    auto* answer = MeosRuntime::bytes(w, size);', '    std::free(w);',
                        '    return answer;'], none
            if kind != 'bytes' and rc.endswith('*'):
                call = self.t.encoder_call(enc, 'r', out_aux)
                return ['    if (r == nullptr)\n' + no_value(none), '    size_t size = 0;', f'    char* w = {call};',
                        '    std::free(r);', '    return MeosRuntime::string(w);'], none
            return None
        if sql == 'bytea' and rc == 'uint8_t*' and length is not None:
            # the WKB a writer answers, its length in its size out-parameter, as JMEOS's #_ret reads it
            return ['    if (r == nullptr)\n' + no_value(none), f'    auto* answer = MeosRuntime::bytes(r, {length});',
                    '    std::free(r);', '    return answer;'], none
        if sql == 'text':
            if rc == 'char*':
                return ['    return MeosRuntime::string(r);'], none
            if rc == 'const char*':
                # a name MEOS keeps, which the caller does not free
                return ['    if (r == nullptr)\n' + no_value(none), '    return MeosRuntime::bytes(r, std::strlen(r));'], none
            if rc == 'text*':
                self.t.fn('text_out')
                return ['    if (r == nullptr)\n' + no_value(none), '    size_t size = 0;',
                        '    char* w = text_out(r, &size);', '    std::free(r);', '    return MeosRuntime::string(w);'], none
            return None
        if sql == 'timestamptz' and rc == 'TimestampTz':
            return ['    return MeosRuntime::number<uint64_t>(out, MeosRuntime::milliseconds(r));'], \
                'MeosRuntime::number<uint64_t>(out, uint64_t{})'
        if sql in SQL_SCALAR and rc in C_NUMBER:
            slot = SQL_SCALAR[sql][1]
            # a predicate MEOS answers as an int answers true for 1 alone, its error being -1
            value = 'r == 1' if (slot == 'bool' and rc == 'int') else f'static_cast<{slot}>(r)'
            return [f'    return MeosRuntime::number<{slot}>(out, {value});'], f'MeosRuntime::number<{slot}>(out, {slot}{{}})'
        return None

    def default_argument(self, k, sql, p, literal, pre, post):
        """The C expression passing the default `literal` to parameter p for an argument of SQL type `sql` a
        shorter call leaves out; the literal is the one JMEOS's #_default_literal writes"""
        ctype = c_spelling(p['canonical'])
        if sql == 'text':
            if ctype == 'const char*':
                return literal
            if ctype in ('text*', 'const text*'):
                self.t.fn('cstring_to_text')
                pre.append(f'    text* t{k} = cstring_to_text({literal});')
                post.append(f'    std::free(t{k});')
                return f't{k}'
            if ctype in self.m.enum_parser:
                return f'{self.t.fn(self.m.enum_parser[ctype])["name"]}({literal})'
            return None
        if sql in SQL_SCALAR and ctype in C_NUMBER:
            return f'static_cast<{ctype}>({literal.rstrip("L")})'
        return None

    def signature(self, f, idx, name, args, ret, bound, sig, defaults):
        """The overloads and kernels of one SQL signature of f: the call stating every argument and each
        shorter call its trailing defaults allow, as JMEOS's #_default_variants derives them, its arguments
        paired with the C parameters as #_inputs pairs them; or the reason the signature is left out"""
        # a function a type's CREATE TYPE binds, which the dialect never names (the signature's typeSupport,
        # rule 3 of MobilityDB's doc/contributing/io/00-INDEX.md), as JMEOS's codegen_spark_udfs.type_support
        # leaves it out: its type's portable reader and writer are other signatures
        if sig.get('typeSupport'):
            return 'type support'
        vis, outs = self.m.visible(f)
        shape = f.get('shape') or {}
        # an out parameter answering the length of the result is passed a length the call ignores, but for
        # the length of an array answer (shape.arrayReturn), which the answer reads
        returned = ((shape.get('arrayReturn') or {}).get('lengthFrom') or {}).get('name') \
            if ret and ret.endswith('[]') else None
        if [p for p in outs if c_spelling(p['canonical']) != 'size_t*' and p['name'] != returned]:
            return 'out parameters'
        # the length of an input array (shape.inputArrays) is read off the array, as #_inputs reads it
        # but the WKB a reader takes, which SqlModel.visible folds with its length into one value (#_byte_arrays)
        folded = self.cj._byte_arrays(f)
        arrays = {a['param']: a for a in shape.get('inputArrays') or [] if a['param'] not in folded}
        counts = {}
        for a in arrays.values():
            lf = a.get('lengthFrom') or {}
            if lf.get('kind') != 'param' or lf.get('name') not in {p['name'] for p in vis}:
                return 'array arguments'
            counts[lf['name']] = a['param']
        types = [self.nes_type(a) for a in args]
        rtype = self.nes_type(ret) if ret else None
        if None in types or rtype is None:
            missing = next(t for t, n in zip(list(args) + [ret], types + [rtype]) if n is None)
            return f'type {missing}'
        if len(args) > MAX_ARITY:
            return 'arity'
        if name.upper() in self.taken:
            return f'name {name} is a NebulaStream function'
        bound = {k: v for k, v in (bound or {}).items() if k in {p['name'] for p in vis}}
        walk = [i for i, p in enumerate(vis) if p['name'] not in bound and p['name'] not in counts]
        order = sig.get('sqlArgParams') or (f.get('shape') or {}).get('sqlArgParams')
        if order:
            at = {p['name']: i for i, p in enumerate(vis)}
            if set(order) <= set(at) and sorted(at[n] for n in order) == walk:
                walk = [at[n] for n in order]
        if len(walk) != len(args):
            return 'arity mismatch'
        why = None
        for variant in self.cj._default_variants(args, defaults, len(args)):
            left = len(variant) if variant else 0
            why = self.emit(f, idx, name, args, types, rtype, ret, vis, outs, bound, walk, variant or [], left,
                            arrays, counts) or why
        return why

    def emit(self, f, idx, name, args, types, rtype, ret, vis, outs, bound, walk, variant, left, arrays, counts):
        """The overload and kernel of the call of signature `idx` of f leaving its last `left` arguments to
        the defaults `variant`, as JMEOS's #_sql_overloads emits one eval per call; or the reason it is
        left out"""
        stated = len(args) - left
        nes_args = tuple(t for t, _ in types[:stated])
        if any(o[0] == nes_args for o in self.overloads.get(name, [])):
            return 'same argument types as an earlier signature'
        call = [None] * len(vis)
        for i, p in enumerate(vis):
            if p['name'] in bound:
                call[i] = self.bound_literal(bound[p['name']], c_spelling(p['canonical']))
                if call[i] is None:
                    return 'bound literal'
        pre, post = [], []
        passed = {}
        for k, (i, sql) in enumerate(zip(walk, args)):
            if sql.endswith('[]') != (vis[i]['name'] in arrays):
                return f'array argument {sql} as {c_spelling(vis[i]["canonical"])}'
            if vis[i]['name'] in arrays:
                e = self.array_argument(k, sql, vis[i], pre) if k < stated else None
            elif k < stated:
                e = self.argument(k, sql, vis[i], pre, post)
            else:
                e = self.default_argument(k, sql, vis[i], variant[k - stated], pre, post)
            if e is None:
                return f'argument {sql} as {c_spelling(vis[i]["canonical"])}'
            call[i] = e
            passed[vis[i]['name']] = k
        for i, p in enumerate(vis):
            if p['name'] in counts:
                if counts[p['name']] not in passed:
                    return f'array {counts[p["name"]]} not passed'
                call[i] = f'v{passed[counts[p["name"]]]}.count()'
        for j, p in enumerate(outs):
            pre.append(f'    {c_spelling(p["canonical"])[:-1]} length{j} = 0;')
            call.append(f'&length{j}')
        got = self.answer(ret, f, 'length0' if outs else None)
        if got is None:
            return f'result {ret} as {c_spelling(f["returnType"]["canonical"])}'
        answer, none = got
        self.t.fn(f['name'])
        key = f'Meos_{f["name"]}_{idx}' + (f'_{stated}' if left else '')
        params = ', '.join(f'int8_t* a{k}' for k in range(stated))
        body = [f'int8_t* {key}({params + ", " if params else ""}int8_t* out)', '{', '    MeosRuntime::initialized();']
        if 'MeosRuntime::number<' not in ''.join(answer) + none:
            body.append('    static_cast<void>(out);')
        body += [line.replace('@NONE@', none) for line in pre]
        body.append(f'    auto r = {f["name"]}({", ".join(c for c in call if c is not None)});')
        body += post
        body += answer
        body.append('}')
        self.kernels.append((key, '\n'.join(body), [pt for _, pt in types[:stated]], rtype[1]))
        self.overloads.setdefault(name, []).append((nes_args, rtype[0], key))
        return None

    def composition(self, n, comp):
        """The overloads and kernels of composition n of the catalog (its top-level `compositions`), as
        codegen_jvm._composition_overload builds its eval: each argument operand read for the first function
        it reaches and passed through the MEOS functions of its `casts` in order, a `value` operand passed
        as the literal it binds, the function `call` called on them and its answer read as a signature's
        answer; a composition without `call` answers the last cast of its one operand. A composition
        answering rows, or restoring its answer to the type of an argument, is left out, its reason
        counted; or the reason the composition is left out"""
        if comp.get('retSet') or comp['ret'].endswith('[]'):
            return 'composition answering rows'
        if comp.get('restore'):
            return 'composition restoring its answer'
        operands = comp['operands']
        if 'call' not in comp:
            chain = list((operands[0] if len(operands) == 1 else {}).get('casts') or ())
            last = self.m.by_name.get(chain[-1]) if chain else None
            if last is None:
                return 'composition without a call'
            comp = dict(comp, call=chain[-1], operands=[dict(operands[0], casts=chain[:-1],
                                                              param=self.m.visible(last)[0][0]['name'])])
            operands = comp['operands']
        f = self.m.by_name.get(comp['call'])
        if f is None or f.get('api') == 'internal':
            return f'composition calling {comp["call"]}'
        args, name = comp['args'], comp['sqlName']
        types = [self.nes_type(a) for a in args]
        rtype = self.nes_type(comp['ret'])
        if None in types or rtype is None:
            missing = next(t for t, x in zip(list(args) + [comp['ret']], types + [rtype]) if x is None)
            return f'type {missing}'
        if len(args) > MAX_ARITY:
            return 'arity'
        if name.upper() in self.taken:
            return f'name {name} is a NebulaStream function'
        vis, outs = self.m.visible(f)
        if [p for p in outs if c_spelling(p['canonical']) != 'size_t*']:
            return 'out parameters'
        if (f.get('shape') or {}).get('inputArrays'):
            return 'array arguments'
        by_param = {o.get('param'): o for o in operands}
        why = None
        for variant in self.cj._default_variants(args, comp.get('argDefaults') or [], len(args)):
            left = len(variant) if variant else 0
            why = self.emit_composition(n, comp, f, name, args, types, rtype, vis, outs, by_param,
                                        variant or [], left) or why
        return why

    def emit_composition(self, n, comp, f, name, args, types, rtype, vis, outs, by_param, variant, left):
        """The overload and kernel of the call of composition n leaving its last `left` arguments to the
        defaults `variant`; or the reason it is left out. The answer of each cast is freed after the call,
        and before any later step answers no value."""
        stated = len(args) - left
        nes_args = tuple(t for t, _ in types[:stated])
        if any(o[0] == nes_args for o in self.overloads.get(name, [])):
            return 'same argument types as an earlier signature'
        pre, post, call, casted = [], [], [], []
        for p in vis:
            o = by_param.get(p['name'])
            e = None
            if o is not None and 'value' in o:
                e = self.bound_literal(str(o['value']).strip("'"), c_spelling(p['canonical']))
            elif o is not None and 'arg' in o and o['arg'] < len(args):
                k, chain = o['arg'], [self.m.by_name.get(c) for c in o.get('casts') or ()]
                if None in chain or any(len(self.m.visible(c)[0]) != 1 or self.m.visible(c)[1] for c in chain):
                    return f'composition cast of {p["name"]}'
                # the argument is read for the first function it reaches
                q = self.m.visible(chain[0])[0][0] if chain else p
                start = len(pre)
                e = (self.argument(k, args[k], q, pre, post) if k < stated
                     else self.default_argument(k, args[k], q, variant[k - stated], pre, post))
                for j, c in enumerate(chain if e is not None else ()):
                    self.t.fn(c['name'])
                    t = f'c{k}_{j}'
                    pre.append(f'    auto* {t} = {c["name"]}({e});')
                    casted.append(t)
                    pre.append(f'    if ({t} == nullptr)\n' + no_value('@NONE@'))
                    post.insert(0, f'    std::free({t});')
                    e = t
                # a step reading this argument answers no value after freeing the earlier casts
                held = casted[:len(casted) - len(chain)] if e is not None else casted
                for i in range(start, len(pre)):
                    if held:
                        frees = ''.join(f'        std::free({c});\n' for c in reversed(held))
                        pre[i] = pre[i].replace('        return @NONE@;', frees + '        return @NONE@;')
            if e is None:
                return f'composition operand {p["name"]}'
            call.append(e)
        for j, p in enumerate(outs):
            pre.append(f'    {c_spelling(p["canonical"])[:-1]} length{j} = 0;')
            call.append(f'&length{j}')
        got = self.answer(comp['ret'], f, 'length0' if outs else None)
        if got is None:
            return f'result {comp["ret"]} as {c_spelling(f["returnType"]["canonical"])}'
        answer, none = got
        self.t.fn(f['name'])
        key = f'Meos_composition{n}' + (f'_{stated}' if left else '')
        params = ', '.join(f'int8_t* a{k}' for k in range(stated))
        body = [f'int8_t* {key}({params + ", " if params else ""}int8_t* out)', '{', '    MeosRuntime::initialized();']
        if 'MeosRuntime::number<' not in ''.join(answer) + none:
            body.append('    static_cast<void>(out);')
        body += [line.replace('@NONE@', none) for line in pre]
        body.append(f'    auto r = {f["name"]}({", ".join(call)});')
        body += post
        body += answer
        body.append('}')
        self.kernels.append((key, '\n'.join(body), [pt for _, pt in types[:stated]], rtype[1]))
        self.overloads.setdefault(name, []).append((nes_args, rtype[0], key))
        return None

    def build(self):
        """Every SQL signature of the catalog's public functions, as JMEOS's #_sql_overloads walks them, then
        every composition the catalog states, as it walks them after; an entry the catalog flags vendored
        left out as every binding leaves it out (the PostgreSQL and H3 code MobilityDB carries)"""
        keep = lambda f: (f.get('sqlfn') and not f.get('sqlfnBackingOnly') and f.get('api') != 'internal'  # noqa: E731
                          and not f.get('vendored'))
        for f in self.cat['functions']:
            if not keep(f):
                continue
            for idx, ((name, args, ret, defaults, bound), sig) in enumerate(
                    zip(self.m.signatures(f), f.get('sqlSignatures') or [])):
                why = self.signature(f, idx, name, args, ret, bound, sig, defaults)
                if why:
                    self.leave(why)
        for n, comp in enumerate(self.cat.get('compositions') or []):
            why = self.composition(n, comp)
            if why:
                self.leave(why)

    def logical_source(self):
        out = [LICENSE, '/* AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
               ' * Every MEOS function by its SQL name, each with the signatures the catalog states for it. */', '',
               '#include <array>', '#include <string_view>', '#include <utility>',
               '#include <Functions/Meos/MeosLogicalFunction.hpp>', '#include <LogicalFunctionRegistry.hpp>', '',
               'namespace NES', '{', '', 'namespace', '{']
        for n, (name, overloads) in enumerate(sorted(self.overloads.items())):
            for o, (nes_args, _, _) in enumerate(overloads):
                quoted = ', '.join(f'"{a}"' for a in nes_args)
                out.append(f'constexpr std::array<std::string_view, {len(nes_args)}> arguments{n}_{o}{{{quoted}}};')
            rows = ', '.join(f'MeosOverload{{arguments{n}_{o}, "{r}", "{k}"}}' for o, (_, r, k) in enumerate(overloads))
            out.append(f'constexpr std::array<MeosOverload, {len(overloads)}> overloads{n}{{{rows}}};')
        out += ['}', '']
        for n, (name, _) in enumerate(sorted(self.overloads.items())):
            out += [f'LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::Register{name}LogicalFunction('
                    'LogicalFunctionRegistryArguments arguments)',
                    '{', f'    return MeosLogicalFunction("{name}", overloads{n}, std::move(arguments.children));', '}', '']
        out.append('}')
        return '\n'.join(out) + '\n'

    def kernel_sources(self, headers):
        files = []
        for start in range(0, len(self.kernels), KERNELS_PER_FILE):
            chunk = self.kernels[start:start + KERNELS_PER_FILE]
            out = [LICENSE, '/* AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
                   ' * The kernels of the MEOS functions, one per SQL signature, and their physical functions. */', '',
                   '#include <cstdint>', '#include <cstdlib>', '#include <cstring>', '#include <string>', '#include <utility>',
                   '', 'extern "C" {', headers, '}', '',
                   '#include <DataTypes/DataType.hpp>', '#include <Functions/Meos/MeosPhysicalFunction.hpp>',
                   '#include <MeosKernelRuntime.hpp>', '#include <PhysicalFunctionRegistry.hpp>', '',
                   'namespace NES', '{', '', 'namespace', '{']
            for _, src, _, _ in chunk:
                out += [src, '']
            out += ['}', '']
            for key, _, ptypes, rtype in chunk:
                types = ', '.join(f'DataType::Type::{t}' for t in ptypes)
                out += [f'PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::Register{key}PhysicalFunction('
                        'PhysicalFunctionRegistryArguments arguments)',
                        '{', f'    return MeosPhysicalFunction(MeosKernel{{&{key}}}, {{{types}}}, DataType::Type::{rtype}, '
                        'std::move(arguments.childFunctions));', '}', '']
            out.append('}')
            files.append('\n'.join(out) + '\n')
        return files


class AggregateLayer:
    """The aggregates of the catalog's `aggregates` section over the types of the TypeLayer, as MobilityDuck's
    tools/codegen_duck_udfs.py #_agg_ops and #gen_aggregates write them: one operations struct per catalog
    entry, run by the MeosAggregate template of nes-plugins/MEOS/include/MeosAggregateRuntime.hpp, and one
    aggregate per name over its overloads. An argument is decoded as a function's is (FunctionLayer) and the
    result answered as a function's result is."""

    # The MEOS function answering the start timestamp of a temporal value, the order an aggregate with no
    # combine folds its rows in, as MobilityDuck's AGG_ORDER_KEY
    ORDER_KEY = 'temporal_start_timestamptz'

    def __init__(self, functions):
        self.f = functions
        self.t = functions.t
        self.m = functions.m
        self.by_name = {f['name']: f for f in functions.cat['functions']}
        self.overloads = {}   # name -> [(NebulaStream argument type, result type, key)]
        self.ops = []         # (key, C++ source, physical argument type)
        self.left = {}

    def leave(self, why):
        self.left[why] = self.left.get(why, 0) + 1

    def input(self, p, sql, k):
        """How SQL argument k, of type `sql`, reaches the C parameter p, as #_agg_input: (field, decode, call,
        free, literal type) with the row at row. The first argument is the one a MeosAggregationPhysicalFunction
        hands over, at a0; each other one is a query literal of the block (MeosRuntime::literal), of the
        literal type, the NebulaStream type it is written in. None where no rule passes it"""
        ctype = c_spelling(p['canonical'])
        base = ctype.replace('const ', '').replace('struct ', '').rstrip('*').strip()
        src = 'a0' if k == 0 else f'MeosRuntime::literal(block, {k - 1})'
        at = f'row.a{k}'
        # a value the call reads none of skips the row, releasing what the row's earlier arguments hold
        skip = f'\n        if ({at} == nullptr)\n        {{\n@RELEASE@            return false;\n        }}'
        if sql in self.m.codec:
            kind, dec = self.m.codec[sql][:2]
            if not ctype.endswith('*'):
                return None
            if sql in self.m.image:
                decode = f'{at} = MeosRuntime::laid({src});'
            elif kind != 'bytes':
                # carried as the codec's string, decoded for the call
                self.t.fn(dec)
                decode = f'{at} = {dec}(MeosRuntime::text({src}).c_str());'
            else:
                return None
            return (f'void* a{k};', decode + skip, f'static_cast<{base}*>({at})', f'std::free({at});', 'VARSIZED')
        if sql == 'text':
            if ctype in ('text*', 'const text*'):
                self.t.fn('cstring_to_text')
                return (f'text* a{k};', f'{at} = cstring_to_text(MeosRuntime::text({src}).c_str());', at,
                        f'std::free({at});', 'VARSIZED')
            if ctype == 'const char*':
                return (f'char* a{k};', f'{at} = strdup(MeosRuntime::text({src}).c_str());', at, f'std::free({at});',
                        'VARSIZED')
            if ctype in self.m.enum_parser:
                parser = self.t.fn(self.m.enum_parser[ctype])['name']
                return (f'{ctype} a{k};', f'{at} = {parser}(MeosRuntime::text({src}).c_str());', at, '', 'VARSIZED')
            return None
        if sql in SQL_SCALAR and ctype in C_NUMBER:
            return (f'{ctype} a{k};', f'{at} = static_cast<{ctype}>(MeosRuntime::number<{SQL_SCALAR[sql][1]}>({src}));',
                    at, '', SQL_SCALAR[sql][0])
        if sql == 'timestamptz' and ctype == 'TimestampTz':
            # NebulaStream's timestamp, milliseconds from the Unix epoch, as FunctionLayer.argument reads it
            return (f'TimestampTz a{k};', f'{at} = MeosRuntime::timestamptz(MeosRuntime::number<uint64_t>({src}));',
                    at, '', 'UINT64')
        # a literal of a C type the catalog reads from text (typeEncodings[C].in: an interval), read by it
        enc = (self.f.cat.get('typeEncodings') or {}).get(base) or {}
        if k > 0 and ctype.endswith('*') and 'text' in (enc.get('decoders') or {}):
            reader = self.t.fn(enc['decoders']['text'])['name']
            aux = ''.join(f', {literal(x)}' for x in (enc.get('decoderAux') or {}).get('text') or [])
            return (f'{base}* a{k};', f'{at} = {reader}(MeosRuntime::text({src}).c_str(){aux});' + skip, at,
                    f'std::free({at});', 'VARSIZED')
        return None

    def entry(self, a):
        """The operations struct of catalog aggregate `a`, its overload added to its name, or the reason it is
        left out, as #_agg_ops"""
        roles = {r: (a.get(r) or {}).get('meos') for r in ('transition', 'combine', 'final')}
        args, ret, stype = a.get('args') or [], a['ret'], a['stype']
        name = a.get('altSqlName') or a['sqlName']
        if not args:
            return 'no argument'
        # the first argument is the aggregated field, of a NebulaStream type; the others are query literals
        first = self.f.nes_type(args[0])
        rtype = self.f.nes_type(ret)
        if first is None or rtype is None:
            return f'type {args[0] if first is None else ret}'
        if any(r and r not in self.by_name for r in roles.values()):
            return 'role not in the catalog'
        tf = self.by_name[roles['transition']]
        gathers = not roles['combine']
        if gathers and self.ORDER_KEY not in self.by_name:
            return f'no combine and no {self.ORDER_KEY}'
        params = tf.get('params') or []
        vis = params[1:]
        sig = next((s for s in tf.get('sqlSignatures') or [] if s.get('args') == [stype] + args), {})
        bound = {**((tf.get('shape') or {}).get('boundArgs') or {}), **(sig.get('boundArgs') or {})}
        bound = {n: v for n, v in bound.items() if n in {p['name'] for p in vis}}
        walk = [i for i, p in enumerate(vis) if p['name'] not in bound]
        order = sig.get('sqlArgParams') or (tf.get('shape') or {}).get('sqlArgParams')
        if order:
            at = {p['name']: i for i, p in enumerate(vis)}
            if set(order) <= set(at) and sorted(at[n] for n in order) == walk:
                walk = [at[n] for n in order]
        if len(walk) != len(args):
            return 'arity'
        call = [None] * len(vis)
        for i, p in enumerate(vis):
            if p['name'] in bound:
                call[i] = self.f.bound_literal(bound[p['name']], c_spelling(p['canonical']))
                if call[i] is None:
                    return f'bound {bound[p["name"]]}'
        fields, decodes, frees, literal_types = [], [], [], []
        for k, (i, sql) in enumerate(zip(walk, args)):
            got = self.input(vis[i], sql, k)
            if got is None:
                return f'argument {sql}/{c_spelling(vis[i]["canonical"])}'
            field, decode, call[i], free, ltype = got
            fields.append(field)
            decodes.append(decode.replace('@RELEASE@', ''.join(f'            {f}\n' for f in frees)))
            if free:
                frees.append(free)
            if k > 0:
                literal_types.append(ltype)
        if gathers and fields[0] != 'void* a0;':
            return 'no combine over a value of no temporal type'
        ff = self.by_name.get(roles['final']) if roles['final'] else None
        keeps = bool(ff and ff.get('params') and ff['params'][0]['canonical'].startswith('const '))
        res = ff or tf
        rc = c_spelling(res['returnType']['canonical'])
        if not rc.endswith('*'):
            return f'result {rc}'
        answer = self.f.answer(ret, res)
        if answer is None:
            return f'result {ret} as {rc}'
        lines, _ = answer
        cast = lambda fn, i: f'static_cast<{c_spelling(fn["params"][i]["canonical"])}>'  # noqa: E731
        key = f'MeosAggregate_{name}_{len(self.overloads.get(name, []))}'
        for fn in (tf, self.by_name.get(roles['combine']), ff):
            if fn:
                self.t.fn(fn['name'])
        L = [f'struct {key}Ops', '{', '    struct Row', '    {', *(f'        {f}' for f in fields), '    };',
             f'    static constexpr bool GATHERS = {"true" if gathers else "false"};',
             f'    static constexpr bool HAS_FINAL = {"true" if ff else "false"};',
             f'    static constexpr bool KEEPS = {"true" if keeps else "false"};',
             '    static bool Read(Row& row, int8_t* a0, int8_t* block)', '    {',
             *([] if len(args) > 1 else ['        static_cast<void>(block);']),
             *(f'        {d}' for d in decodes), '        return true;', '    }',
             '    static void* Transition(void* s, Row& row)', '    {',
             f'        return {tf["name"]}({cast(tf, 0)}(s){"".join(", " + c for c in call)});', '    }',
             '    static void Free(Row& row)', '    {',
             *((f'        {f}' for f in frees) if frees else ['        static_cast<void>(row);']), '    }']
        if roles['combine']:
            cf = self.by_name[roles['combine']]
            L += ['    static void* Combine(void* a, void* b)', '    {',
                  f'        return {cf["name"]}({cast(cf, 0)}(a), {cast(cf, 1)}(b));', '    }']
        else:
            L += ['    static void* Combine(void*, void*)', '    {', '        return nullptr;', '    }']
        if ff:
            L += ['    static void* Final(void* s)', '    {', f'        return {ff["name"]}({cast(ff, 0)}(s));', '    }']
        else:
            L += ['    static void* Final(void* s)', '    {', '        return s;', '    }']
        body = [line.replace('\n', '\n    ') for line in lines]
        L += ['    static int8_t* Write(void* v, int8_t* out)', '    {',
              f'        auto* r = static_cast<{rc.replace("const ", "")}>(v);']
        if 'MeosRuntime::number<' not in ''.join(lines):
            L.append('        static_cast<void>(out);')
        L += [f'    {line}' for line in body]
        L += ['    }']
        if gathers:
            self.t.fn(self.ORDER_KEY)
            L += ['    static int64_t Order(const Row& row)', '    {',
                  f'        return {self.ORDER_KEY}(static_cast<const Temporal*>(row.a0));', '    }']
        else:
            L += ['    static int64_t Order(const Row&)', '    {', '        return 0;', '    }']
        L += ['};', '']
        self.ops.append((key, '\n'.join(L), first[1], tuple(literal_types)))
        self.overloads.setdefault(name, []).append((first[0], tuple(literal_types), rtype[0], key))
        return None

    def build(self, functions):
        """Every entry of the catalog's `aggregates` section, one overload per name and argument types, as
        #gen_aggregates walks them; an aggregate sharing its name with a function stops the generation, the
        parser taking a call of such a name over a field for the aggregate"""
        seen = set()
        for a in self.f.cat.get('aggregates') or []:
            name = a.get('altSqlName') or a['sqlName']
            key = (name.lower(), tuple(a.get('args') or []))
            if key in seen:
                continue
            why = self.entry(a)
            if why:
                self.leave(why)
                continue
            seen.add(key)
        clash = sorted(n for n in self.overloads if n.upper() in {f.upper() for f in functions} | self.f.taken)
        if clash:
            raise SystemExit(f'an aggregate and a function share a name: {", ".join(clash)}')

    def logical_source(self):
        out = [LICENSE, '/* AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
               ' * Every MEOS aggregate by its name, each with the overloads the catalog states for it. */', '',
               '#include <array>', '#include <memory>', '#include <string_view>', '#include <utility>',
               '#include <Functions/FieldAccessLogicalFunction.hpp>',
               '#include <Operators/Windows/Aggregations/Meos/MeosAggregationLogicalFunction.hpp>',
               '#include <AggregationLogicalFunctionRegistry.hpp>', '#include <ErrorHandling.hpp>', '',
               'namespace NES', '{', '', 'namespace', '{']
        for n, (name, overloads) in enumerate(sorted(self.overloads.items())):
            for o, (_, lits, _, _) in enumerate(overloads):
                quoted = ', '.join(f'"{t}"' for t in lits)
                out.append(f'constexpr std::array<std::string_view, {len(lits)}> literals{n}_{o}{{{quoted}}};')
            rows = ', '.join(f'MeosAggregateOverload{{"{a}", literals{n}_{o}, "{r}", "{k}"}}'
                             for o, (a, _, r, k) in enumerate(overloads))
            out.append(f'constexpr std::array<MeosAggregateOverload, {len(overloads)}> overloads{n}{{{rows}}};')
        out += ['}', '']
        for n, (name, _) in enumerate(sorted(self.overloads.items())):
            out += [f'AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::'
                    f'Register{name}AggregationLogicalFunction(AggregationLogicalFunctionRegistryArguments arguments)',
                    '{', '    if (arguments.fields.size() != 2)', '    {',
                    f'        throw CannotDeserialize("{name} takes an on and an as field, but got {{}}", arguments.fields.size());',
                    '    }',
                    f'    return std::make_shared<MeosAggregationLogicalFunction>("{name}", overloads{n}, arguments.fields[0], '
                    'arguments.fields[1], std::move(arguments.literals));', '}', '']
        out.append('}')
        return '\n'.join(out) + '\n'

    def kernel_source(self, headers):
        out = [LICENSE, '/* AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
               ' * The operations of the MEOS aggregates, one per overload, and their physical aggregations. */', '',
               '#include <array>', '#include <cstdint>', '#include <cstdlib>', '#include <cstring>', '#include <memory>',
               '#include <string>', '#include <utility>', '', 'extern "C" {', headers, '}', '',
               '#include <Aggregation/Function/Meos/MeosAggregationPhysicalFunction.hpp>', '#include <DataTypes/DataType.hpp>',
               '#include <MeosAggregateRuntime.hpp>', '#include <MeosKernelRuntime.hpp>',
               '#include <AggregationPhysicalFunctionRegistry.hpp>', '', 'namespace NES', '{', '', 'namespace', '{']
        for key, src, _, lits in self.ops:
            types = ', '.join(f'DataType::Type::{t}' for t in lits)
            out += [src, f'constexpr std::array<DataType::Type, {len(lits)}> {key}Literals{{{types}}};', f'using {key} = MeosRuntime::MeosAggregate<{key}Ops>;',
                    f'constexpr MeosAggregateKernels {key}Kernels{{&{key}::Initialize, &{key}::Update, &{key}::Combine, '
                    f'&{key}::Finish, &{key}::Destroy, {key}::StateSize}};', '']
        out += ['}', '']
        for key, _, ptype, _ in self.ops:
            out += [f'AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::'
                    f'Register{key}AggregationPhysicalFunction(AggregationPhysicalFunctionRegistryArguments arguments)',
                    '{', '    return std::make_shared<MeosAggregationPhysicalFunction>(std::move(arguments.inputType), '
                    'std::move(arguments.resultType), std::move(arguments.inputFunction), std::move(arguments.resultFieldIdentifier), '
                    f'DataType::Type::{ptype}, {key}Literals, arguments.literals, {key}Kernels);', '}', '']
        out.append('}')
        return '\n'.join(out) + '\n'


def nebula_function_names(root):
    """The names NebulaStream's own logical functions are registered under, read from the add_plugin
    calls of the repository's CMakeLists"""
    import re
    names = set()
    own = Path(root) / 'nes-plugins' / 'MEOS' / 'Functions'
    for cm in Path(root).glob('nes-*/**/CMakeLists.txt'):
        if cm.parent == own:
            continue
        text = cm.read_text()  # BINDING-HEADER-PARSE-OK: a CMakeLists' add_plugin names, never a header
        names.update(mm.group(1) for mm in re.finditer(r'add_plugin\((\w+)\s+LogicalFunction', text))
    return names


def write_if_changed(path, text):
    """Write the file only where its content changes, so that a build recompiles only what a run changed"""
    if not path.exists() or path.read_text() != text:
        path.write_text(text)


def run(args):
    """The types, then the functions over them, as JMEOS's #run_flink_sql writes the value classes, then
    one function per SQL name with its overloads."""
    cat = load_catalog(args.catalog)
    cj = load_jvm_generator()
    layer = TypeLayer(cj, cat)
    sqls = sorted(layer.m.codec)
    # the MEOS types and `bytea`, the WKB a reader takes and a writer answers (TypeLayer.bytea_source)
    bodies = [layer.source(sql) for sql in sqls] + [layer.bytea_source()]
    type_headers = '\n'.join(f'#include <{h}>' for h in sorted(layer.headers))

    # The functions, whose kernels include the headers of the functions they call; the types written after
    # them carry an array of each type some signature passes or answers an array of
    layer.headers = set()
    functions = FunctionLayer(layer, cat, nebula_function_names(args.output_root), cj)
    functions.build()
    arrays = sorted(functions.arrays)
    src = (LICENSE + PREAMBLE.replace('@HEADERS@', type_headers) + '\n'.join(bodies) + '\n'
           + '\n'.join(layer.array_source(sql) for sql in arrays) + '\n'
           + '\n'.join(layer.registrar(sqls + ['bytea'], arrays)) + '\n')
    out = Path(args.output_root) / 'nes-plugins' / 'MEOS' / 'DataTypes'
    out.mkdir(parents=True, exist_ok=True)
    write_if_changed(out / 'MeosDataTypes.cpp', src)
    cmake = [CMAKE_LICENSE,
             '# AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
             '# Each MEOS SQL type is a data type of its own, registered under its name in upper case, and so is',
             '# an array of it that a function passes or answers, under its name followed by _ARRAY.', '']
    cmake += [f'add_plugin({sql.upper()} DataType nes-data-types MeosDataTypes.cpp)' for sql in sqls + ['bytea']]
    cmake += [f'add_plugin({sql.upper()}_ARRAY DataType nes-data-types MeosDataTypes.cpp)' for sql in arrays]
    cmake += ['', "# The types call libmeos and initialize MEOS through the plugin's wrapper.",
              'target_link_libraries(nes-data-types PUBLIC nes-meos)', '']
    write_if_changed(out / 'CMakeLists.txt', '\n'.join(cmake))
    print(f'codegen_nebula_sql: {len(sqls)} MEOS data types '
          f'({sum(1 for s in sqls if s in layer.m.image)} carried as their memory image) and {len(arrays)} arrays '
          f'into {out}')
    out = Path(args.output_root) / 'nes-plugins' / 'MEOS' / 'Functions'
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob('MeosKernels*.cpp'):
        old.unlink()
    write_if_changed(out / 'MeosLogicalFunctions.cpp', functions.logical_source())
    headers = '\n'.join(f'#include <{h}>' for h in sorted(layer.headers))
    kernel_files = functions.kernel_sources(headers)
    for i, src in enumerate(kernel_files):
        write_if_changed(out / f'MeosKernels{i}.cpp', src)
    cmake = [CMAKE_LICENSE,
             '# AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
             '# Each MEOS function is a logical function under its SQL name, and each of its SQL signatures a',
             '# physical function computed by its kernel.', '']
    cmake += [f'add_plugin({name} LogicalFunction nes-logical-operators MeosLogicalFunctions.cpp)'
              for name in sorted(functions.overloads)]
    cmake.append('')
    for i in range(len(kernel_files)):
        chunk = functions.kernels[i * KERNELS_PER_FILE:(i + 1) * KERNELS_PER_FILE]
        cmake += [f'add_plugin({key} PhysicalFunction nes-physical-operators MeosKernels{i}.cpp)' for key, *_ in chunk]
    write_if_changed(out / 'CMakeLists.txt', '\n'.join(cmake) + '\n')
    print(f'codegen_nebula_sql: {len(functions.overloads)} MEOS functions over {len(functions.kernels)} SQL signatures '
          f'into {out}')
    for why, n in sorted(functions.left.items(), key=lambda x: -x[1]):
        print(f'  left out {n:5d}  {why}')

    # The aggregates, whose operations include the headers of the roles they call
    layer.headers = set()
    aggregates = AggregateLayer(functions)
    aggregates.build(functions.overloads)
    out = Path(args.output_root) / 'nes-plugins' / 'MEOS' / 'Aggregations'
    out.mkdir(parents=True, exist_ok=True)
    write_if_changed(out / 'MeosAggregations.cpp', aggregates.logical_source())
    headers = '\n'.join(f'#include <{h}>' for h in sorted(layer.headers))
    write_if_changed(out / 'MeosAggregationKernels.cpp', aggregates.kernel_source(headers))
    cmake = [CMAKE_LICENSE,
             '# AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
             '# Each MEOS aggregate is a logical aggregation under its name, and each of its overloads a physical',
             '# aggregation run by the MeosAggregate template over its operations.', '']
    cmake += [f'add_plugin({name} AggregationLogicalFunction nes-logical-operators MeosAggregations.cpp)'
              for name in sorted(aggregates.overloads)]
    cmake.append('')
    cmake += [f'add_plugin({key} AggregationPhysicalFunction nes-physical-operators MeosAggregationKernels.cpp)'
              for key, *_ in aggregates.ops]
    write_if_changed(out / 'CMakeLists.txt', '\n'.join(cmake) + '\n')
    print(f'codegen_nebula_sql: {len(aggregates.overloads)} MEOS aggregates over {len(aggregates.ops)} overloads '
          f'into {out}')
    for why, n in sorted(aggregates.left.items(), key=lambda x: -x[1]):
        print(f'  left out {n:5d}  {why}')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--catalog', required=True, help='meos-idl.json of the MEOS the build links')
    ap.add_argument('--output-root', required=True, help='the MobilityNebula checkout')
    run(ap.parse_args())


if __name__ == '__main__':
    sys.exit(main())
