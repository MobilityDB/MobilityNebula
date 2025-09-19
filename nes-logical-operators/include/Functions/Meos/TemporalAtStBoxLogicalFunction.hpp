#pragma once

#include <Functions/LogicalFunction.hpp>
#include <string_view>
#include <utility>
#include <vector>

namespace NES {

class TemporalAtStBoxLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "TGEO_AT_STBOX";

    TemporalAtStBoxLogicalFunction(LogicalFunction lon,
                                   LogicalFunction lat,
                                   LogicalFunction timestamp,
                                   LogicalFunction stbox);

    TemporalAtStBoxLogicalFunction(LogicalFunction lon,
                                   LogicalFunction lat,
                                   LogicalFunction timestamp,
                                   LogicalFunction stbox,
                                   LogicalFunction borderInclusive);

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
    bool hasBorderParam;
};

} // namespace NES
