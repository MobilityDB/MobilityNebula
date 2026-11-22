#include <Functions/Meos/TemporalEDWithinGeometryLogicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

TemporalEDWithinGeometryLogicalFunction::TemporalEDWithinGeometryLogicalFunction(LogicalFunction lon,
                                                                                 LogicalFunction lat,
                                                                                 LogicalFunction timestamp,
                                                                                 LogicalFunction geometry,
                                                                                 LogicalFunction distance)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
{
    parameters.reserve(5);
    parameters.push_back(std::move(lon));
    parameters.push_back(std::move(lat));
    parameters.push_back(std::move(timestamp));
    parameters.push_back(std::move(geometry));
    parameters.push_back(std::move(distance));
}

DataType TemporalEDWithinGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TemporalEDWithinGeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalEDWithinGeometryLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction TemporalEDWithinGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5, "TemporalEDWithinGeometryLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view TemporalEDWithinGeometryLogicalFunction::getType() const
{
    return NAME;
}

bool TemporalEDWithinGeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TemporalEDWithinGeometryLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string TemporalEDWithinGeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string args;
    for (size_t index = 0; index < parameters.size(); ++index)
    {
        if (index > 0)
        {
            args += ", ";
        }
        args += parameters[index].explain(verbosity);
    }
    return fmt::format("{}({})", NAME, args);
}

LogicalFunction TemporalEDWithinGeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }

    INVARIANT(newChildren[0].getDataType().isNumeric(), "Longitude must be numeric, but was: {}", newChildren[0].getDataType());
    INVARIANT(newChildren[1].getDataType().isNumeric(), "Latitude must be numeric, but was: {}", newChildren[1].getDataType());
    INVARIANT(newChildren[2].getDataType().isType(DataType::Type::UINT64), "Timestamp must be UINT64, but was: {}", newChildren[2].getDataType());
    INVARIANT(newChildren[3].getDataType().isType(DataType::Type::VARSIZED), "Geometry literal must be VARSIZED, but was: {}", newChildren[3].getDataType());
    INVARIANT(newChildren[4].getDataType().isNumeric(), "Distance must be numeric, but was: {}", newChildren[4].getDataType());

    return withChildren(newChildren);
}

SerializableFunction TemporalEDWithinGeometryLogicalFunction::serialize() const
{
    SerializableFunction serialized;
    serialized.set_function_type(NAME);
    for (const auto& child : parameters)
    {
        serialized.add_children()->CopyFrom(child.serialize());
    }
    DataTypeSerializationUtil::serializeDataType(getDataType(), serialized.mutable_data_type());
    return serialized;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTemporalEDWithinGeometryLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 5,
                 "TemporalEDWithinGeometryLogicalFunction requires 5 children, but got {}",
                 arguments.children.size());
    return TemporalEDWithinGeometryLogicalFunction(arguments.children[0],
                                                   arguments.children[1],
                                                   arguments.children[2],
                                                   arguments.children[3],
                                                   arguments.children[4]);
}

} // namespace NES
