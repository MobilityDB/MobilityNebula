#define NES_PLUGIN_OPERATOR_TU
#include <Functions/Meos/EdwithinTgeoGeoLogicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <SerializableVariantDescriptor.pb.h>

/* Decoupled from the regenerated plugin registrar (see LogicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::LogicalFunctionGeneratedRegistrar { LogicalFunctionRegistryReturnType RegisterEdwithinTgeoGeoLogicalFunction(LogicalFunctionRegistryArguments); }

namespace NES
{

EdwithinTgeoGeoLogicalFunction::EdwithinTgeoGeoLogicalFunction(LogicalFunction lon,
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

DataType EdwithinTgeoGeoLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction EdwithinTgeoGeoLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> EdwithinTgeoGeoLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction EdwithinTgeoGeoLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5, "EdwithinTgeoGeoLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view EdwithinTgeoGeoLogicalFunction::getType() const
{
    return NAME;
}

bool EdwithinTgeoGeoLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EdwithinTgeoGeoLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string EdwithinTgeoGeoLogicalFunction::explain(ExplainVerbosity verbosity) const
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

LogicalFunction EdwithinTgeoGeoLogicalFunction::withInferredDataType(const Schema& schema) const
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

SerializableFunction EdwithinTgeoGeoLogicalFunction::serialize() const
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
LogicalFunctionGeneratedRegistrar::RegisterEdwithinTgeoGeoLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 5,
                 "EdwithinTgeoGeoLogicalFunction requires 5 children, but got {}",
                 arguments.children.size());
    return EdwithinTgeoGeoLogicalFunction(arguments.children[0],
                                                   arguments.children[1],
                                                   arguments.children[2],
                                                   arguments.children[3],
                                                   arguments.children[4]);
}

} // namespace NES
