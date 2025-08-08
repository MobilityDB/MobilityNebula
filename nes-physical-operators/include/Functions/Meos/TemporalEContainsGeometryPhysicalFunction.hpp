#pragma once
#include <Functions/PhysicalFunction.hpp>
#include <vector>

namespace NES {

class TemporalEContainsGeometryPhysicalFunction : public PhysicalFunctionConcept {
public:
    /* temporal–static (tgeo,geo) or static–temporal (geo,tgeo) */
    /* Order determined by data types of the parameters */
    TemporalEContainsGeometryPhysicalFunction(PhysicalFunction param1,
                                             PhysicalFunction param2,
                                             PhysicalFunction param3,
                                             PhysicalFunction param4);

    /* temporal–temporal (tgeo,tgeo) */
    TemporalEContainsGeometryPhysicalFunction(PhysicalFunction lon1,
                                             PhysicalFunction lat1,
                                             PhysicalFunction ts1,
                                             PhysicalFunction lon2,
                                             PhysicalFunction lat2,
                                             PhysicalFunction ts2);

    VarVal execute(const Record&, ArenaRef&) const override;

private:
    std::vector<PhysicalFunction> paramFns;

    VarVal execTemporalStatic (const std::vector<VarVal>&) const;
    VarVal execStaticTemporal (const std::vector<VarVal>&) const;
    VarVal execTemporalTemporal(const std::vector<VarVal>&) const;
};

} // namespace NES
