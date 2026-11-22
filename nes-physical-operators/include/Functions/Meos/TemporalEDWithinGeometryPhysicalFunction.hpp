#pragma once

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES {

class TemporalEDWithinGeometryPhysicalFunction : public PhysicalFunctionConcept {
public:
    TemporalEDWithinGeometryPhysicalFunction(PhysicalFunction lonFunction,
                                             PhysicalFunction latFunction,
                                             PhysicalFunction timestampFunction,
                                             PhysicalFunction geometryFunction,
                                             PhysicalFunction distanceFunction);

    VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::vector<PhysicalFunction> parameterFunctions;
};

}
