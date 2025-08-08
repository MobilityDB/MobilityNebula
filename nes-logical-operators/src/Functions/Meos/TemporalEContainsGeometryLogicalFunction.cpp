#include <Functions/Meos/TemporalEContainsGeometryLogicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <fmt/format.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES {

/* ─────────── constructors ─────────── */

TemporalEContainsGeometryLogicalFunction::
TemporalEContainsGeometryLogicalFunction(LogicalFunction param1,
                                         LogicalFunction param2,
                                         LogicalFunction param3,
                                         LogicalFunction param4)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32)) {
    parameters = {std::move(param1), std::move(param2), std::move(param3), std::move(param4)};
}

TemporalEContainsGeometryLogicalFunction::
TemporalEContainsGeometryLogicalFunction(LogicalFunction lon1,
                                         LogicalFunction lat1,
                                         LogicalFunction ts1,
                                         LogicalFunction lon2,
                                         LogicalFunction lat2,
                                         LogicalFunction ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32)) {
    parameters  = {std::move(lon1), std::move(lat1), std::move(ts1),
                   std::move(lon2), std::move(lat2), std::move(ts2)};
}

/* ─────────── boiler-plate overrides ─────────── */

DataType TemporalEContainsGeometryLogicalFunction::getDataType() const { return dataType; }

LogicalFunction
TemporalEContainsGeometryLogicalFunction::withDataType(const DataType& dt) const {
    auto c = *this; c.dataType = dt; return c;
}

std::vector<LogicalFunction>
TemporalEContainsGeometryLogicalFunction::getChildren() const { return parameters; }

LogicalFunction
TemporalEContainsGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& ch) const {
    PRECONDITION(ch.size()==4 || ch.size()==6,
                 "TemporalEContainsGeometry expects 4 or 6 params, got {}", ch.size());
    auto c=*this; c.parameters=ch;
    return c;
}

std::string_view TemporalEContainsGeometryLogicalFunction::getType() const { return NAME; }

bool TemporalEContainsGeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    const auto* o = dynamic_cast<const TemporalEContainsGeometryLogicalFunction*>(&rhs);
    return o && parameters==o->parameters;
}

std::string
TemporalEContainsGeometryLogicalFunction::explain(ExplainVerbosity v) const {
    std::string a;
    for(size_t i=0;i<parameters.size();++i){
        if(i) a += ", ";
        a += parameters[i].explain(v);
    }
    return fmt::format("TEMPORAL_ECONTAINS_GEOMETRY({})", a);
}

LogicalFunction
TemporalEContainsGeometryLogicalFunction::withInferredDataType(const Schema& s) const {
    std::vector<LogicalFunction> ch;
    ch.reserve(parameters.size());
    for(auto& p: parameters) ch.push_back(p.withInferredDataType(s));

    // light-weight checks
    auto isNum  = [](const DataType& dt){ return dt.isNumeric(); };
    auto isTime = [](const DataType& dt){ return dt.isType(DataType::Type::UINT64); };
    auto isStr  = [](const DataType& dt){ return dt.isType(DataType::Type::VARSIZED); };

    // Validate based on parameter count and types
    if(ch.size() == 6) {
        // 6-param: temporal-temporal (lon1, lat1, ts1, lon2, lat2, ts2)
        INVARIANT(isNum(ch[0].getDataType()) && isNum(ch[1].getDataType()) && isTime(ch[2].getDataType())
               && isNum(ch[3].getDataType()) && isNum(ch[4].getDataType()) && isTime(ch[5].getDataType()),
               "Invalid types for temporal-temporal contains");
    } else if(ch.size() == 4) {
        if(isStr(ch[0].getDataType())) {
            // 4-param: static-temporal (static_geom, lon, lat, ts)
            INVARIANT(isStr(ch[0].getDataType()) && isNum(ch[1].getDataType()) 
                   && isNum(ch[2].getDataType()) && isTime(ch[3].getDataType()),
                   "Invalid types for static-temporal contains");
        } else {
            // 4-param: temporal-static (lon, lat, ts, static_geom)
            INVARIANT(isNum(ch[0].getDataType()) && isNum(ch[1].getDataType()) 
                   && isTime(ch[2].getDataType()) && isStr(ch[3].getDataType()),
                   "Invalid types for temporal-static contains");
        }
    } else {
        PRECONDITION(false, "TemporalEContainsGeometry expects 4 or 6 parameters, got {}", ch.size());
    }
    return withChildren(ch);
}

SerializableFunction
TemporalEContainsGeometryLogicalFunction::serialize() const {
    SerializableFunction sf;
    sf.set_function_type(NAME);
    for(auto& p: parameters) *sf.add_children() = p.serialize();
    DataTypeSerializationUtil::serializeDataType(dataType, sf.mutable_data_type());
    return sf;
}

/* ─────────── registry helper ─────────── */
LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTemporalEContainsGeometryLogicalFunction(
        LogicalFunctionRegistryArguments arguments){
    if(arguments.children.size()==6)
        return TemporalEContainsGeometryLogicalFunction(arguments.children[0],arguments.children[1],arguments.children[2],
                                                        arguments.children[3],arguments.children[4],arguments.children[5]);
    PRECONDITION(arguments.children.size()==4,
                 "TemporalEContainsGeometry expects 4 or 6 params, got {}", arguments.children.size());

    // decide 4-param layout by inspecting first child’s type
    // decide 4-param layout by inspecting first child's type
    return TemporalEContainsGeometryLogicalFunction(arguments.children[0],arguments.children[1],
                                                    arguments.children[2],arguments.children[3]);
}

} // namespace NES
