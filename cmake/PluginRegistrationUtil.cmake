# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Includes all CMake utility functions to generate plugin registrars, which register plugins at plugin registries.

# creates a library that exposes only the registries and not the registrars
# assumes that registries are located in '${CMAKE_CURRENT_SOURCE_DIR}/registry/include' (see nes-sources for an example)
function(create_plugin_registry_library plugin_registry_library plugin_registry_component)
    add_library(${plugin_registry_library} registry)
    target_link_libraries(${plugin_registry_library} PUBLIC ${plugin_registry_component})
    target_include_directories(${plugin_registry_library}
            PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/registry/include>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/registry/templates> # link against generated registrar headers
            $<INSTALL_INTERFACE:include/nebulastream/>
    )
endfunction()

# enables/disables an optional plugin; if enabled, the path to the plugin becomes part of the build
function(activate_optional_plugin plugin_path plugin_option)
    if (${plugin_option})
        message(STATUS "Activating optional plugin: ${plugin_path} (and all of its dependencies).")
        add_subdirectory(${plugin_path})
    else ()
        message(STATUS "Skipping optional plugin: ${plugin_path}.")
    endif ()
endfunction()

# create a new library for the plugin and link the component that the plugin registry belongs to against it
# adds the name of plugin to the list of plugin names for the plugin registry
# adds the name of the library of the plugin to the list of libraries for the plugin registry
function(add_plugin_as_library plugin_name plugin_registry plugin_registry_component plugin_library)
    set(sources ${ARGN})
    add_library(${plugin_library} STATIC ${sources})
    target_link_libraries(${plugin_library} PRIVATE ${plugin_registry_component})

    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_libraries" "${plugin_library}")
    foreach (source ${sources})
        get_filename_component(plugin_source "${source}" ABSOLUTE)
        record_plugin_source(${plugin_registry} ${plugin_name} "${plugin_source}" ${plugin_library})
    endforeach ()
endfunction()

# records that the source defines the register function of the plugin, compiled in the target, so that the source is
# handed the declaration of that function alone (generate_plugin_registrar)
function(record_plugin_source plugin_registry plugin_name plugin_source plugin_target)
    string(MD5 source_key "${plugin_source}")
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_sources" "${plugin_source}")
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugins_of_${source_key}" "${plugin_name}")
    set_property(GLOBAL PROPERTY "${plugin_registry}_target_of_${source_key}" "${plugin_target}")
endfunction()

# adds the source files of the plugin to the source files of the component that the plugin registry belongs to
# adds the name of plugin to the list of plugin names for the plugin registry
macro(add_plugin plugin_name plugin_registry plugin_registry_component)
    set(sources ${ARGN})
    if (TARGET ${plugin_registry_component})
        foreach (source ${sources})
            set_property(TARGET ${plugin_registry_component} APPEND PROPERTY SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/${source})
        endforeach ()
    else ()
        add_source_files(${plugin_registry_component}
                ${sources}
        )
    endif ()
    set_property(GLOBAL APPEND PROPERTY "${plugin_registry}_plugin_names" "${plugin_name}")
    foreach (source ${sources})
        get_filename_component(plugin_source "${source}" ABSOLUTE)
        record_plugin_source(${plugin_registry} ${plugin_name} "${plugin_source}" ${plugin_registry_component})
    endforeach ()
endmacro()

# iterates over all plugins, collect all plugins with given name, inject plugins into registrar
function(generate_plugin_registrar current_dir current_binary_dir plugin_registry plugin_registry_component)
    set(registrar_header_template_path ${current_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.inc.in)
    set(registrar_header_generated_path ${current_binary_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.inc)

    # get the names of plugins and all plugin libraries for the plugin registry
    get_property(plugin_registry_plugin_names_final GLOBAL PROPERTY ${plugin_registry}_plugin_names)
    get_property(plugin_registry_plugin_libraries_final GLOBAL PROPERTY ${plugin_registry}_plugin_libraries)

    # first, read the Configuration(RETURN_TYPE, ARGUMENTS) from the '.in' file
    file(READ ${registrar_header_template_path} registrar_header_file_data)

    # second, remove the configuration and write the modified version of the registrar header template to a temporary file
    # we generate the final '.inc' file from that temporary file
    set(temp_registrar_header_template_file "${current_binary_dir}/temp_registrar_header_template.inc.in")
    file(WRITE ${temp_registrar_header_template_file} "${registrar_header_file_data}")

    # generate the list of declarations of the register functions that the plugins implement to register themselves
    # generate the list of concrete register calls that are called in the 'registerAll' function call of the Registrar to populate the registry
    set(REGISTER_FUNCTION_DECLARATIONS "")
    set(REGISTER_ALL_FUNCTION_CALLS "")
    foreach (reg_func IN LISTS plugin_registry_plugin_names_final)
        list(APPEND REGISTER_FUNCTION_DECLARATIONS "${plugin_registry}RegistryReturnType Register${reg_func}${plugin_registry}(${plugin_registry}RegistryArguments)")
        list(APPEND REGISTER_ALL_FUNCTION_CALLS "registry.addEntry(\"${reg_func}\", Register${reg_func}${plugin_registry})")
    endforeach ()

    # link all plugin libraries against the component that the plugin registry belongs to, this makes the implementation
    # details accessible to the library of the component that the plugin registry belongs to
    foreach (plugin_library IN LISTS plugin_registry_plugin_libraries_final)
        target_link_libraries(${plugin_registry_component} PUBLIC ${plugin_library})
    endforeach ()

    # add ';'s to the end of the generated function [declarations|calls], and add further formatting (new line and tab)
    string(REPLACE ";" ";\n" REGISTER_FUNCTION_DECLARATIONS "${REGISTER_FUNCTION_DECLARATIONS};")
    string(REPLACE ";" ";\n\t" REGISTER_ALL_FUNCTION_CALLS "${REGISTER_ALL_FUNCTION_CALLS};")

    # remove the '.in' from the end of the file and write the result to the parent directory of the the template

    # the definitions, with every declaration and every register call, in a file only the definitions unit includes
    set(registrar_definitions_path ${current_binary_dir}/registry/templates/${plugin_registry}GeneratedRegistrarDefinitions.inc)
    configure_file(
            ${temp_registrar_header_template_file}
            ${registrar_definitions_path}
            @ONLY
    )
    # the header the registry includes, which holds no plugin and so stays the same file whatever plugins there are
    set(REGISTER_FUNCTION_DECLARATIONS "")
    set(REGISTER_ALL_FUNCTION_CALLS "")
    configure_file(
            ${temp_registrar_header_template_file}
            ${registrar_header_generated_path}
            @ONLY
    )
    file(REMOVE ${temp_registrar_header_template_file})

    # The registrar is compiled once: the registry's header declares registerAll alone, and one generated translation
    # unit defines it with the declarations of every register function. A plugin source is handed the declarations of
    # the register functions it defines, so that adding or changing a plugin recompiles that source and that unit
    # rather than every source including the registry. Each file is written only where its content changes.
    file(GLOB registry_headers "${current_dir}/registry/include/*.hpp")
    set(registry_header "")
    foreach (header IN LISTS registry_headers)
        file(STRINGS "${header}" includes_registrar REGEX "<${plugin_registry}GeneratedRegistrar\\.inc>")
        if (includes_registrar)
            get_filename_component(registry_header "${header}" NAME)
        endif ()
    endforeach ()
    if (registry_header STREQUAL "")
        message(FATAL_ERROR "No header in ${current_dir}/registry/include includes ${plugin_registry}GeneratedRegistrar.inc")
    endif ()
    string(REGEX MATCH "namespace ([A-Za-z_:]+)[ \t\r\n]*{[^{}]*@REGISTER_FUNCTION_DECLARATIONS@" declarations_block "${registrar_header_file_data}")
    set(declarations_namespace "${CMAKE_MATCH_1}")
    if (declarations_namespace STREQUAL "")
        message(FATAL_ERROR "${registrar_header_template_path} declares its register functions in no namespace")
    endif ()

    # the guard the template checks that the registry's header defines around its inclusion
    string(REGEX MATCH "#ifndef (INCLUDED_FROM_[A-Za-z_]+)" registrar_guard_line "${registrar_header_file_data}")
    set(registrar_guard "${CMAKE_MATCH_1}")
    if (registrar_guard STREQUAL "")
        message(FATAL_ERROR "${registrar_header_template_path} states no INCLUDED_FROM_ guard")
    endif ()
    set(definitions_source "${current_binary_dir}/registry/templates/${plugin_registry}GeneratedRegistrar.cpp")
    file(CONFIGURE OUTPUT "${definitions_source}"
            CONTENT "#include <${registry_header}>\n#define NES_REGISTRAR_DEFINITIONS\n#define ${registrar_guard}\n#include \"${registrar_definitions_path}\"\n")
    target_sources(${plugin_registry_component} PRIVATE "${definitions_source}")

    get_property(plugin_sources GLOBAL PROPERTY ${plugin_registry}_plugin_sources)
    list(REMOVE_DUPLICATES plugin_sources)
    foreach (plugin_source IN LISTS plugin_sources)
        string(MD5 source_key "${plugin_source}")
        get_property(source_plugins GLOBAL PROPERTY ${plugin_registry}_plugins_of_${source_key})
        get_property(source_target GLOBAL PROPERTY ${plugin_registry}_target_of_${source_key})
        set(source_declarations "")
        foreach (plugin_name IN LISTS source_plugins)
            string(APPEND source_declarations
                    "${plugin_registry}RegistryReturnType Register${plugin_name}${plugin_registry}(${plugin_registry}RegistryArguments);\n")
        endforeach ()
        set(declarations_header "${current_binary_dir}/registry/templates/${plugin_registry}Declarations/${source_key}.hpp")
        file(CONFIGURE OUTPUT "${declarations_header}"
                CONTENT "#pragma once\n#include <${registry_header}>\nnamespace ${declarations_namespace}\n{\n${source_declarations}}\n")
        set_property(SOURCE "${plugin_source}" TARGET_DIRECTORY ${source_target}
                APPEND PROPERTY COMPILE_OPTIONS "-include${declarations_header}")
    endforeach ()
endfunction()

function(generate_plugin_registrars plugin_registry_component)
    foreach (plugin_registry ${ARGN})
        cmake_language(EVAL CODE "
                cmake_language(DEFER DIRECTORY [[${PROJECT_SOURCE_DIR}]] CALL generate_plugin_registrar [[${CMAKE_CURRENT_SOURCE_DIR}]] [[${CMAKE_CURRENT_BINARY_DIR}]] [[${plugin_registry}]] [[${plugin_registry_component}]])
        ")
    endforeach ()
endfunction()

# Provide the names of all registries that the component creates as ARGS
# Registries are typically located in the 'registry' directory of the component, e.g., 'nes-sources/registry'
function(create_registries_for_component)
    get_filename_component(COMPONENT_NAME "${CMAKE_CURRENT_LIST_DIR}" NAME)
    set(registries_library ${COMPONENT_NAME}-registry)
    create_plugin_registry_library(${registries_library} ${COMPONENT_NAME})
    target_link_libraries(${COMPONENT_NAME} PRIVATE ${registries_library})
    generate_plugin_registrars(${COMPONENT_NAME} ${ARGN})
endfunction()
