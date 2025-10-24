#pragma once

#include <Functions/LogicalFunction.hpp>
#include <string_view>
#include <utility>
#include <vector>

namespace NES {

class TemporalEDWithinGeometryLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "TemporalEDWithinGeometry";

    TemporalEDWithinGeometryLogicalFunction(LogicalFunction lon,
                                            LogicalFunction lat,
                                            LogicalFunction timestamp,
                                            LogicalFunction geometry,
                                            LogicalFunction distance);

    DataType getDataType() const override;
    LogicalFunction withDataType(const DataType& dataType) const override;
    std::vector<LogicalFunction> getChildren() const override;
    LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;
    std::string_view getType() const override;
    bool operator==(const LogicalFunctionConcept& rhs) const override;
    std::string explain(ExplainVerbosity verbosity) const override;
    LogicalFunction withInferredDataType(const Schema& schema) const override;
    SerializableFunction serialize() const override;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
};

} // namespace NES
