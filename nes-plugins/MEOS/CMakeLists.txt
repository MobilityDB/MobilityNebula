# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The MEOS plugin: the MEOS types and functions of the MEOS catalog, which tools/codegen/codegen_nebula_sql.py
# writes into DataTypes/ and Functions/, over libmeos, which nes-meos initializes.

find_package(meos REQUIRED)
find_package(reflectcpp CONFIG REQUIRED)

add_library(nes-meos STATIC MEOSWrapper.cpp)
target_include_directories(nes-meos PUBLIC include PUBLIC .)
target_link_libraries(nes-meos PUBLIC ${meos} nes-common nes-data-types)
# libmeos's headers are a dependency's, read as system headers by every target linking the plugin
target_include_directories(nes-meos SYSTEM PUBLIC ${meos_INCLUDE_DIR})
# The libraries libmeos links
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROJ REQUIRED proj)
find_library(JSON_C_LIB json-c REQUIRED)
target_link_libraries(nes-meos PUBLIC ${PROJ_LIBRARIES} ${JSON_C_LIB})
# meos_h3.h includes <h3api.h>, which H3 installs in an h3 subdirectory, found as MobilityDB finds it
find_path(H3_INCLUDE_DIR NAMES h3api.h PATH_SUFFIXES h3)
if (H3_INCLUDE_DIR)
    target_include_directories(nes-meos SYSTEM PUBLIC ${H3_INCLUDE_DIR})
endif ()

# The types, the logical functions and their registration, linked into the logical operators; the kernels,
# the physical functions and the readers of the types' text, linked into the physical operators. Each is
# linked whole, as the glue of a registry is, so that the initializers describing the entries to the plugin
# loader are kept, and sees its owner compile-only, as the glue does, so that the cycle between the two
# static libraries does not link it twice (cmake/RuntimeRegistrationUtil.cmake).
add_library(nes-meos-logical STATIC MeosLogicalRegistration.cpp Functions/MeosLogicalFunction.cpp)
target_link_libraries(nes-meos-logical PUBLIC nes-meos PRIVATE $<COMPILE_ONLY:nes-logical-operators> reflectcpp::reflectcpp)
link_plugin_library(nes-logical-operators "$<LINK_LIBRARY:WHOLE_ARCHIVE,nes-meos-logical>")

add_library(nes-meos-physical STATIC MeosPhysicalRegistration.cpp Functions/MeosPhysicalFunction.cpp)
target_link_libraries(nes-meos-physical PUBLIC nes-meos
        PRIVATE $<COMPILE_ONLY:nes-physical-operators> $<COMPILE_ONLY:nes-input-formatters>)
link_plugin_library(nes-physical-operators "$<LINK_LIBRARY:WHOLE_ARCHIVE,nes-meos-physical>")

add_subdirectory(DataTypes)
add_subdirectory(Functions)
