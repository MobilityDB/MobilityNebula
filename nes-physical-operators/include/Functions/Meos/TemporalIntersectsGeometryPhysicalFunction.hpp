
#pragma once

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES {

class TemporalIntersectsGeometryPhysicalFunction : public PhysicalFunctionConcept {
public:
    /// Constructor with 4 parameters for temporal-static intersection: lon1, lat1, timestamp1, static_geometry_wkt
    TemporalIntersectsGeometryPhysicalFunction(PhysicalFunction lon1Function, PhysicalFunction lat1Function, PhysicalFunction timestamp1Function, PhysicalFunction staticGeometryFunction);
    
    /// Constructor with 6 parameters for temporal-temporal intersection: lon1, lat1, timestamp1, lon2, lat2, timestamp2
    TemporalIntersectsGeometryPhysicalFunction(PhysicalFunction lon1Function, PhysicalFunction lat1Function, PhysicalFunction timestamp1Function, PhysicalFunction lon2Function, PhysicalFunction lat2Function, PhysicalFunction timestamp2Function);

    /// Execute the function with the given record and arena
    VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::vector<PhysicalFunction> parameterFunctions;  // Stores 4 or 6 parameter functions
    bool isTemporal6Param;  // true for 6-param temporal-temporal, false for 4-param temporal-static
    
    // Helper methods for different parameter cases
    VarVal executeTemporal6Param(const std::vector<VarVal>& params) const;
    VarVal executeTemporal4Param(const std::vector<VarVal>& params) const;
};

}