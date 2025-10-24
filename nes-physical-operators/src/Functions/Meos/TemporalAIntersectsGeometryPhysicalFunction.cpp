#include <utility>
#include <vector>
#include <string>
#include <cstring>
#include <Functions/Meos/TemporalAIntersectsGeometryPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <MEOSWrapper.hpp>
#include <fmt/format.h>
#include <iostream>
#include <val.hpp>
#include <function.hpp>

namespace NES {

// Constructor with 4 parameters for temporal-static intersection
TemporalAIntersectsGeometryPhysicalFunction::TemporalAIntersectsGeometryPhysicalFunction(PhysicalFunction lon1Function, PhysicalFunction lat1Function, PhysicalFunction timestamp1Function, PhysicalFunction staticGeometryFunction)
    : isTemporal6Param(false)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(lon1Function));
    parameterFunctions.push_back(std::move(lat1Function));
    parameterFunctions.push_back(std::move(timestamp1Function));
    parameterFunctions.push_back(std::move(staticGeometryFunction));
}

// Constructor with 6 parameters for temporal-temporal intersection
TemporalAIntersectsGeometryPhysicalFunction::TemporalAIntersectsGeometryPhysicalFunction(PhysicalFunction lon1Function, PhysicalFunction lat1Function, PhysicalFunction timestamp1Function, PhysicalFunction lon2Function, PhysicalFunction lat2Function, PhysicalFunction timestamp2Function)
    : isTemporal6Param(true)
{
    parameterFunctions.reserve(6);
    parameterFunctions.push_back(std::move(lon1Function));
    parameterFunctions.push_back(std::move(lat1Function));
    parameterFunctions.push_back(std::move(timestamp1Function));
    parameterFunctions.push_back(std::move(lon2Function));
    parameterFunctions.push_back(std::move(lat2Function));
    parameterFunctions.push_back(std::move(timestamp2Function));
}

VarVal TemporalAIntersectsGeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::cout << "TemporalAIntersectsGeometryPhysicalFunction::execute called with " << parameterFunctions.size() << " arguments" << std::endl;
    
    // Execute all parameter functions to get their values
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& paramFunc : parameterFunctions) {
        parameterValues.push_back(paramFunc.execute(record, arena));
    }
    
    if (isTemporal6Param) {
        // 6-parameter case: temporal-temporal intersection
        return executeTemporal6Param(parameterValues);
    } else {
        // 4-parameter case: temporal-static intersection
        return executeTemporal4Param(parameterValues);
    }
}

VarVal TemporalAIntersectsGeometryPhysicalFunction::executeTemporal6Param(const std::vector<VarVal>& params) const
{
    // Extract coordinate values: lon1, lat1, timestamp1, lon2, lat2, timestamp2
    auto lon1 = params[0].cast<nautilus::val<double>>();
    auto lat1 = params[1].cast<nautilus::val<double>>();
    auto timestamp1 = params[2].cast<nautilus::val<uint64_t>>();
    auto lon2 = params[3].cast<nautilus::val<double>>();
    auto lat2 = params[4].cast<nautilus::val<double>>();
    auto timestamp2 = params[5].cast<nautilus::val<uint64_t>>();
    
    std::cout << "6-param temporal-temporal aintersection with coordinate values" << std::endl;
    
    // Use nautilus::invoke to call external MEOS function with coordinate parameters
    const auto result = nautilus::invoke(
        +[](double lon1_val, double lat1_val, uint64_t ts1_val, double lon2_val, double lat2_val, uint64_t ts2_val) -> int {
            try {
                // Use the existing global MEOS initialization mechanism
                MEOS::Meos::ensureMeosInitialized();
                auto inRange = [](double lo, double la){ return lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0; };
                if (!inRange(lon1_val, lat1_val) || !inRange(lon2_val, lat2_val)) {
                    std::cout << "TemporalAIntersects: coordinates out of range" << std::endl;
                    return 0;
                }
                
                // Convert UINT64 timestamps to MEOS timestamp strings
                std::string timestamp1_str = MEOS::Meos::convertEpochToTimestamp(ts1_val);
                std::string timestamp2_str = MEOS::Meos::convertEpochToTimestamp(ts2_val);
                
                // Build temporal geometry WKT strings from coordinates and timestamps
                std::string left_geometry_wkt = fmt::format("SRID=4326;Point({} {})@{}", lon1_val, lat1_val, timestamp1_str);
                std::string right_geometry_wkt = fmt::format("SRID=4326;Point({} {})@{}", lon2_val, lat2_val, timestamp2_str);
                
                std::cout << "Built temporal geometries:" << std::endl;
                std::cout << "Left: " << left_geometry_wkt << std::endl;
                std::cout << "Right: " << right_geometry_wkt << std::endl;
                
                // Both geometries are temporal points, use temporal-temporal aintersection
                std::cout << "Using temporal-temporal aintersection (aintersects_tgeo_tgeo)" << std::endl;
                MEOS::Meos::TemporalGeometry left_temporal(left_geometry_wkt);
                if (!left_temporal.getGeometry()) {
                    std::cout << "TemporalAIntersects: left temporal geometry is null" << std::endl;
                    return 0;
                }
                MEOS::Meos::TemporalGeometry right_temporal(right_geometry_wkt);
                if (!right_temporal.getGeometry()) {
                    std::cout << "TemporalAIntersects: right temporal geometry is null" << std::endl;
                    return 0;
                }
                int intersection_result = left_temporal.aintersects(right_temporal);
                std::cout << "aintersects_tgeo_tgeo result: " << intersection_result << std::endl;
                
                return intersection_result;
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry aintersection: " << e.what() << std::endl;
                return -1;  // Error case
            } catch (...) {
                std::cout << "Unknown error in temporal geometry aintersection" << std::endl;
                return -1;  // Error case
            }
        },
        lon1, lat1, timestamp1, lon2, lat2, timestamp2
    );
    
    return VarVal(result);
}

VarVal TemporalAIntersectsGeometryPhysicalFunction::executeTemporal4Param(const std::vector<VarVal>& params) const
{
    // Extract values: lon1, lat1, timestamp1, static_geometry_wkt
    auto lon1 = params[0].cast<nautilus::val<double>>();
    auto lat1 = params[1].cast<nautilus::val<double>>();
    auto timestamp1 = params[2].cast<nautilus::val<uint64_t>>();
    auto static_geometry_varsized = params[3].cast<VariableSizedData>();
    
    std::cout << "4-param temporal-static aintersection with coordinate values" << std::endl;
    
    // Use nautilus::invoke to call external MEOS function with coordinate and geometry parameters
    const auto result = nautilus::invoke(
        +[](double lon1_val, double lat1_val, uint64_t ts1_val, const char* static_geom_ptr, uint32_t static_geom_size) -> int {
            try {
                // Use the existing global MEOS initialization mechanism
                MEOS::Meos::ensureMeosInitialized();
                if (!(lon1_val >= -180.0 && lon1_val <= 180.0 && lat1_val >= -90.0 && lat1_val <= 90.0)) {
                    std::cout << "TemporalAIntersects: coordinates out of range" << std::endl;
                    return 0;
                }
                
                // Convert UINT64 timestamp to MEOS timestamp string
                std::string timestamp1_str = MEOS::Meos::convertEpochToTimestamp(ts1_val);
                
                // Build temporal geometry WKT string from coordinates and timestamp
                std::string left_geometry_wkt = fmt::format("SRID=4326;Point({} {})@{}", lon1_val, lat1_val, timestamp1_str);
                
                // Extract static geometry WKT from VariableSizedData
                std::string right_geometry_wkt(static_geom_ptr, static_geom_size);
                
                // Strip quotes if present (CSV parsing includes quotes in the string values)
                while (!right_geometry_wkt.empty() && (right_geometry_wkt.front() == '\'' || right_geometry_wkt.front() == '"')) {
                    right_geometry_wkt = right_geometry_wkt.substr(1);
                }
                while (!right_geometry_wkt.empty() && (right_geometry_wkt.back() == '\'' || right_geometry_wkt.back() == '"')) {
                    right_geometry_wkt = right_geometry_wkt.substr(0, right_geometry_wkt.size() - 1);
                }
                
                std::cout << "Built geometries:" << std::endl;
                std::cout << "Left (temporal): " << left_geometry_wkt << std::endl;
                std::cout << "Right (static): " << right_geometry_wkt << std::endl;
                
                // Validate input strings are not empty
                if (left_geometry_wkt.empty() || right_geometry_wkt.empty()) {
                    std::cout << "Empty geometry WKT string(s)" << std::endl;
                    return -1;
                }
                
                // Use temporal-static aintersection
                std::cout << "Using temporal-static aintersection (aintersects_tgeo_geo)" << std::endl;
                MEOS::Meos::TemporalGeometry left_temporal(left_geometry_wkt);
                if (!left_temporal.getGeometry()) {
                    std::cout << "TemporalAIntersects: MEOS temporal geometry is null" << std::endl;
                    return 0;
                }
                MEOS::Meos::StaticGeometry right_static(right_geometry_wkt);
                if (!right_static.getGeometry()) {
                    std::cout << "TemporalAIntersects: MEOS static geometry is null" << std::endl;
                    return 0;
                }
                int intersection_result = left_temporal.aintersectsStatic(right_static);
                std::cout << "aintersects_tgeo_geo result: " << intersection_result << std::endl;
                
                return intersection_result;
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry aintersection: " << e.what() << std::endl;
                return -1;  // Error case
            } catch (...) {
                std::cout << "Unknown error in temporal geometry aintersection" << std::endl;
                return -1;  // Error case
            }
        },
        lon1, lat1, timestamp1, static_geometry_varsized.getContent(), static_geometry_varsized.getContentSize()
    );
    
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTemporalAIntersectsGeometryPhysicalFunction(PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    if (physicalFunctionRegistryArguments.childFunctions.size() == 4) {
        return TemporalAIntersectsGeometryPhysicalFunction(
            physicalFunctionRegistryArguments.childFunctions[0],
            physicalFunctionRegistryArguments.childFunctions[1],
            physicalFunctionRegistryArguments.childFunctions[2],
            physicalFunctionRegistryArguments.childFunctions[3]
        );
    } else if (physicalFunctionRegistryArguments.childFunctions.size() == 6) {
        return TemporalAIntersectsGeometryPhysicalFunction(
            physicalFunctionRegistryArguments.childFunctions[0],
            physicalFunctionRegistryArguments.childFunctions[1],
            physicalFunctionRegistryArguments.childFunctions[2],
            physicalFunctionRegistryArguments.childFunctions[3],
            physicalFunctionRegistryArguments.childFunctions[4],
            physicalFunctionRegistryArguments.childFunctions[5]
        );
    } else {
        PRECONDITION(false, "TemporalAIntersectsGeometryPhysicalFunction requires 4 or 6 child functions, but got {}", physicalFunctionRegistryArguments.childFunctions.size());
    }
}

}
