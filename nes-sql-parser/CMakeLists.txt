# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(src)
get_source(nes-sql-parser SQL_PARSER_SOURCE_FILES)

find_package(antlr4-generator CONFIG REQUIRED)

set(ANTLR_JAR_FILE "antlr-${ANTLR_VERSION}-complete.jar")

if (DEFINED ENV{NES_PREBUILT_VCPKG_ROOT})
  # if in prebuilt docker, expect jar _with correct version_ in /opt
  if (NOT EXISTS "/opt/${ANTLR_JAR_FILE}")
    message(FATAL_ERROR "could not find expected ANTLR jar file /opt/${ANTLR_JAR_FILE}. "
                        "Maybe there is a version mismatch?")
  endif ()
  set(ANTLR4_JAR_LOCATION "/opt/${ANTLR_JAR_FILE}")
else ()
  include(FetchContent)
  # The value of ANTLR_JAR_FILE (containing the ANTLR version) is set by antlr4-generator.
  # On mismatch: update antlr version in dockerfile, then update hashes here and in docker.
  FetchContent_Declare(
    antlr4_jar
    URL      https://www.antlr.org/download/${ANTLR_JAR_FILE}
    URL_HASH SHA256=eae2dfa119a64327444672aff63e9ec35a20180dc5b8090b7a6ab85125df4d76
    DOWNLOAD_NO_EXTRACT TRUE
  )
  FetchContent_MakeAvailable(antlr4_jar)
  set(ANTLR4_JAR_LOCATION "${antlr4_jar_SOURCE_DIR}/${ANTLR_JAR_FILE}")
endif ()

# MEOS family grammar gating: when a type family is excluded (-DCBUFFER=0 etc.)
# filter its lexer keyword tokens + functionName alternatives OUT of the grammar
# so the excluded type's keywords are physically absent from the generated
# lexer/parser (they then lex as a plain IDENTIFIER, which the #if-guarded dispatch
# rejects as an unknown function) — matching MobilityDB/MEOS's -DCBUFFER=0. The
# default all-families-ON build uses the master grammar directly (no filtering, no
# staleness in the codegen workflow); the filter only engages when a family is OFF,
# which is a configure-time option toggle (so this configure-time filter re-runs).
if(CBUFFER AND NPOINT AND POSE AND RGEO)
    set(_NES_GRAMMAR ${CMAKE_CURRENT_SOURCE_DIR}/AntlrSQL.g4)
else()
    foreach(_f CBUFFER NPOINT POSE RGEO)
        if(${_f})
            set(_${_f}_N 1)
        else()
            set(_${_f}_N 0)
        endif()
    endforeach()
    set(_NES_GRAMMAR ${CMAKE_CURRENT_BINARY_DIR}/AntlrSQL.g4)
    execute_process(
        COMMAND python3 ${CMAKE_SOURCE_DIR}/tools/codegen/filter_grammar.py
                --in ${CMAKE_CURRENT_SOURCE_DIR}/AntlrSQL.g4 --out ${_NES_GRAMMAR}
                --cbuffer ${_CBUFFER_N} --npoint ${_NPOINT_N} --pose ${_POSE_N} --rgeo ${_RGEO_N}
        RESULT_VARIABLE _g4_rc)
    if(NOT _g4_rc EQUAL 0)
        message(FATAL_ERROR "filter_grammar.py failed (rc=${_g4_rc})")
    endif()
endif()

# generate BOTH parser and lexer, and additionaly listener
antlr4_generate(NES_SQL_PARSER ${_NES_GRAMMAR} BOTH -listener)

# The VCPKG port is called 'antlr4', finding the package requires to look for 'antlr4-runtime' and
# adding antlr library as a dependency to another library, requires to link against 'antlr-static'.
find_package(antlr4-runtime CONFIG REQUIRED)
add_library(nes-sql-parser ${SQL_PARSER_SOURCE_FILES} ${ANTLR4_SRC_FILES_NES_SQL_PARSER})

# MEOS family compile-defines (CBUFFER/NPOINT/POSE/RGEO=1|0): the codegen wraps
# each family op's #include + dispatch case in #if <FAMILY>, so a family OFF drops
# the token (the parser has no case for it and rejects the function at plan time),
# matching MobilityDB/MEOS's -DCBUFFER=0 etc. Scoped to the dispatch TU so toggling
# a family recompiles only this file and the rest of the ccache stays warm.
set_source_files_properties(
    ${CMAKE_CURRENT_SOURCE_DIR}/src/AntlrSQLQueryPlanCreator.cpp
    PROPERTIES COMPILE_DEFINITIONS "${MEOS_FAMILY_COMPILE_DEFS}")

target_include_directories(nes-sql-parser PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>
        ${ANTLR4_INCLUDE_DIR_NES_SQL_PARSER} ${ANTLR4_INCLUDE_DIR}
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/private>)
target_link_libraries(nes-sql-parser PRIVATE nes-input-formatters PUBLIC antlr4_static nes-common nes-sinks nes-sources nes-logical-operators)

add_tests_if_enabled(tests)

