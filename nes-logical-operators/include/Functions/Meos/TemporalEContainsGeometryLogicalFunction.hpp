#pragma once
#include <Functions/LogicalFunction.hpp>
#include <string_view>
#include <vector>

namespace NES {

class TemporalEContainsGeometryLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "TemporalEContainsGeometry";

    TemporalEContainsGeometryLogicalFunction(LogicalFunction param1,
                                             LogicalFunction param2,
                                             LogicalFunction param3,
                                             LogicalFunction param4);

    /// temporal–temporal (tgeo, tgeo) → econtains_tgeo_tgeo
    TemporalEContainsGeometryLogicalFunction(LogicalFunction lon1,
                                             LogicalFunction lat1,
                                             LogicalFunction ts1,
                                             LogicalFunction lon2,
                                             LogicalFunction lat2,
                                             LogicalFunction ts2);

    /** --- inherited API --- **/
    DataType                     getDataType()                     const override;
    LogicalFunction              withDataType(const DataType&)     const override;
    std::vector<LogicalFunction> getChildren()                     const override;
    LogicalFunction              withChildren(const std::vector<LogicalFunction>&) const override;
    std::string_view             getType()                         const override;
    bool                         operator==(const LogicalFunctionConcept&) const override;
    std::string                  explain(ExplainVerbosity)         const override;
    LogicalFunction              withInferredDataType(const Schema&) const override;
    SerializableFunction         serialize()                       const override;

private:
    DataType                 dataType;
    std::vector<LogicalFunction> parameters;
};

} // namespace NES
