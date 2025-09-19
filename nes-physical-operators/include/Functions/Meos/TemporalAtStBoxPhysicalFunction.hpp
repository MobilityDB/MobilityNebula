#pragma once

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES {

class TemporalAtStBoxPhysicalFunction : public PhysicalFunctionConcept {
public:
    TemporalAtStBoxPhysicalFunction(PhysicalFunction lonFunction,
                                    PhysicalFunction latFunction,
                                    PhysicalFunction timestampFunction,
                                    PhysicalFunction stboxFunction);

    TemporalAtStBoxPhysicalFunction(PhysicalFunction lonFunction,
                                    PhysicalFunction latFunction,
                                    PhysicalFunction timestampFunction,
                                    PhysicalFunction stboxFunction,
                                    PhysicalFunction borderInclusiveFunction);

    VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::vector<PhysicalFunction> parameterFunctions;
    bool hasBorderParam;
};

}
