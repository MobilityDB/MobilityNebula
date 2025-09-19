#include <Functions/Meos/TemporalAtStBoxLogicalFunction.hpp>

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

TemporalAtStBoxLogicalFunction::TemporalAtStBoxLogicalFunction(LogicalFunction lon,
                                                               LogicalFunction lat,
                                                               LogicalFunction timestamp,
                                                               LogicalFunction stbox)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
    , hasBorderParam(false)
{
    parameters.reserve(4);
    parameters.push_back(std::move(lon));
    parameters.push_back(std::move(lat));
    parameters.push_back(std::move(timestamp));
    parameters.push_back(std::move(stbox));
}

TemporalAtStBoxLogicalFunction::TemporalAtStBoxLogicalFunction(LogicalFunction lon,
                                                               LogicalFunction lat,
                                                               LogicalFunction timestamp,
                                                               LogicalFunction stbox,
                                                               LogicalFunction borderInclusive)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
    , hasBorderParam(true)
{
    parameters.reserve(5);
    parameters.push_back(std::move(lon));
    parameters.push_back(std::move(lat));
    parameters.push_back(std::move(timestamp));
    parameters.push_back(std::move(stbox));
    parameters.push_back(std::move(borderInclusive));
}

DataType TemporalAtStBoxLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TemporalAtStBoxLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalAtStBoxLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction TemporalAtStBoxLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4 || children.size() == 5,
                 "TemporalAtStBoxLogicalFunction requires 4 or 5 children, but got {}",
                 children.size());
    auto copy = *this;
    copy.parameters = children;
    copy.hasBorderParam = (children.size() == 5);
    return copy;
}

std::string_view TemporalAtStBoxLogicalFunction::getType() const
{
    return NAME;
}

bool TemporalAtStBoxLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TemporalAtStBoxLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters && hasBorderParam == other->hasBorderParam;
    }
    return false;
}

std::string TemporalAtStBoxLogicalFunction::explain(ExplainVerbosity verbosity) const
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

LogicalFunction TemporalAtStBoxLogicalFunction::withInferredDataType(const Schema& schema) const
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
    INVARIANT(newChildren[3].getDataType().isType(DataType::Type::VARSIZED), "STBOX literal must be VARSIZED, but was: {}", newChildren[3].getDataType());
    if (newChildren.size() == 5)
    {
        INVARIANT(newChildren[4].getDataType().isType(DataType::Type::BOOLEAN),
                  "Border flag must be BOOL, but was: {}",
                  newChildren[4].getDataType());
    }

    return withChildren(newChildren);
}

SerializableFunction TemporalAtStBoxLogicalFunction::serialize() const
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
LogicalFunctionGeneratedRegistrar::RegisterTemporalAtStBoxLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() == 4)
    {
        return TemporalAtStBoxLogicalFunction(arguments.children[0],
                                              arguments.children[1],
                                              arguments.children[2],
                                              arguments.children[3]);
    }
    if (arguments.children.size() == 5)
    {
        return TemporalAtStBoxLogicalFunction(arguments.children[0],
                                              arguments.children[1],
                                              arguments.children[2],
                                              arguments.children[3],
                                              arguments.children[4]);
    }
    PRECONDITION(false,
                 "TemporalAtStBoxLogicalFunction requires 4 or 5 children, but got {}",
                 arguments.children.size());
}

} // namespace NES
