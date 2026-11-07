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

    def registrar(self, sqls):
        out = ['}', '', 'namespace NES', '{', '']
        for sql in sqls:
            out += [f'DataTypeRegistryReturnType DataTypeGeneratedRegistrar::Register{sql.upper()}DataType(DataTypeRegistryArguments)',
                    '{',
                    f'    return DataType{{.type = DataType::Type::VARSIZED, .extension = &{ident(sql)}Type}};',
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

    def leave(self, why):
        self.left[why] = self.left.get(why, 0) + 1

    def nes_type(self, sql):
        """The NebulaStream data type a SQL type is carried in, and the physical type of its value"""
        if sql in self.m.codec:
            return sql.upper(), 'VARSIZED'
        if sql == 'text':
            return 'VARSIZED', 'VARSIZED'
        if sql in SQL_SCALAR:
            return SQL_SCALAR[sql][0], SQL_SCALAR[sql][0]
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
        if sql in SQL_SCALAR and ctype in C_NUMBER:
            return f'static_cast<{ctype}>(MeosRuntime::number<{SQL_SCALAR[sql][1]}>({a}))'
        return None

    def answer(self, sql, f):
        """The statements answering the result `r` of SQL type `sql`, and the answer of no value; None
        where no rule answers it"""
        rc = c_spelling(f['returnType']['canonical'])
        none = 'MeosRuntime::none()'
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
        vis, outs = self.m.visible(f)
        # an out parameter answering the length of the result is passed a length the call ignores
        if [p for p in outs if c_spelling(p['canonical']) != 'size_t*']:
            return 'out parameters'
        if (f.get('shape') or {}).get('inputArrays'):
            return 'array arguments'
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
        walk = [i for i, p in enumerate(vis) if p['name'] not in bound]
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
            why = self.emit(f, idx, name, args, types, rtype, ret, vis, outs, bound, walk, variant or [], left) or why
        return why

    def emit(self, f, idx, name, args, types, rtype, ret, vis, outs, bound, walk, variant, left):
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
        for k, (i, sql) in enumerate(zip(walk, args)):
            if k < stated:
                e = self.argument(k, sql, vis[i], pre, post)
            else:
                e = self.default_argument(k, sql, vis[i], variant[k - stated], pre, post)
            if e is None:
                return f'argument {sql} as {c_spelling(vis[i]["canonical"])}'
            call[i] = e
        for j, p in enumerate(outs):
            pre.append(f'    size_t length{j} = 0;')
            call.append(f'&length{j}')
        got = self.answer(ret, f)
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

    def build(self):
        """Every SQL signature of the catalog's public functions, as JMEOS's #_sql_overloads walks them; an
        entry the catalog flags vendored left out as every binding leaves it out (the PostgreSQL and H3
        code MobilityDB carries)"""
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
    bodies = [layer.source(sql) for sql in sqls]
    headers = '\n'.join(f'#include <{h}>' for h in sorted(layer.headers))
    src = (LICENSE + PREAMBLE.replace('@HEADERS@', headers) + '\n'.join(bodies) + '\n'
           + '\n'.join(layer.registrar(sqls)) + '\n')
    out = Path(args.output_root) / 'nes-plugins' / 'MEOS' / 'DataTypes'
    out.mkdir(parents=True, exist_ok=True)
    write_if_changed(out / 'MeosDataTypes.cpp', src)
    cmake = [CMAKE_LICENSE,
             '# AUTO-GENERATED by tools/codegen/codegen_nebula_sql.py from the MEOS catalog -- do not edit by hand.',
             '# Each MEOS SQL type is a data type of its own, registered under its name in upper case.', '']
    cmake += [f'add_plugin({sql.upper()} DataType nes-data-types MeosDataTypes.cpp)' for sql in sqls]
    cmake += ['', "# The types call libmeos and initialize MEOS through the plugin's wrapper.",
              'target_link_libraries(nes-data-types PUBLIC nes-meos)', '']
    write_if_changed(out / 'CMakeLists.txt', '\n'.join(cmake))
    print(f'codegen_nebula_sql: {len(sqls)} MEOS data types '
          f'({sum(1 for s in sqls if s in layer.m.image)} carried as their memory image) into {out}')

    # The functions, whose kernels include the headers of the functions they call
    layer.headers = set()
    functions = FunctionLayer(layer, cat, nebula_function_names(args.output_root), cj)
    functions.build()
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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--catalog', required=True, help='meos-idl.json of the MEOS the build links')
    ap.add_argument('--output-root', required=True, help='the MobilityNebula checkout')
    run(ap.parse_args())


if __name__ == '__main__':
    sys.exit(main())
