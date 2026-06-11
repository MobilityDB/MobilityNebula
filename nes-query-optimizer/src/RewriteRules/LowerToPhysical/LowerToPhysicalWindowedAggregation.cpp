/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.hpp>

#include <cstdint>
#include <memory>
#include <numeric>
#include <ranges>
#include <utility>
#include <vector>

#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationProbePhysicalOperator.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/ColumnTupleBufferRef.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <magic_enum/magic_enum.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <HashMapOptions.hpp>
#include <PhysicalOperator.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <RewriteRuleRegistry.hpp>
// Special-case lowering for TEMPORAL_SEQUENCE (multi-input) aggregation
#ifdef NES_ENABLE_MEOS
#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunctionV2.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalLengthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/PairMeetingAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CrossDistanceAggregationLogicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSequenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalLengthAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/PairMeetingAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/CrossDistanceAggregationPhysicalFunction.hpp>
#endif

#include <Aggregation/Function/Meos/TemporalNumInstantsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalNumSequencesAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalNumTimestampsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTFloatStartValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTFloatEndValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTFloatMinValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTFloatMaxValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTNumberIntegralAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTIntStartValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTIntEndValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTIntMinValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTIntMaxValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTFloatAvgValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTNumberTwAvgAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTIntAvgValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalStartTimestampAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalEndTimestampAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalLowerIncAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalUpperIncAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTPointIsSimpleAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TspatialExtentAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberExtentAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/FloatExtentAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/IntExtentAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/BigintExtentAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TimestamptzExtentAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/FloatUnionAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/IntUnionAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/BigintUnionAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TimestamptzUnionAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TrajectoryWkbAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TLengthExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TgeoCentroidExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointAzimuthExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointAngularDifferenceExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TgeompointToTgeometryExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalCopyExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberAbsExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberDeltaValueExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberAngularDifferenceExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalDerivativeExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalAtMaxExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalAtMinExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalMinusMaxExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalMinusMinExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnpointCumulativeLengthExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnpointSpeedExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnpointToTgeompointExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointCumulativeLengthExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointSpeedExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointGetXExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointGetYExpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberTrendExpAggregationPhysicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointCumulativeLengthExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointSpeedExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointGetXExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointGetYExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberTrendExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointCumulativeLengthExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointSpeedExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointToTgeompointExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberAbsExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberDeltaValueExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberAngularDifferenceExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalDerivativeExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalAtMaxExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalAtMinExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalMinusMaxExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalMinusMinExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TgeoCentroidExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointAzimuthExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointAngularDifferenceExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TgeompointToTgeometryExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalCopyExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TLengthExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TrajectoryWkbAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/FloatUnionAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/IntUnionAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/BigintUnionAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TimestamptzUnionAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/FloatExtentAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/IntExtentAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/BigintExtentAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TimestamptzExtentAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TspatialExtentAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberExtentAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalStartTimestampAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalEndTimestampAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalLowerIncAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalUpperIncAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTPointIsSimpleAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTFloatAvgValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTNumberTwAvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntAvgValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalNumInstantsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalNumSequencesAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalNumTimestampsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTFloatStartValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTFloatEndValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTFloatMinValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTFloatMaxValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTNumberIntegralAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntStartValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntEndValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntMinValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntMaxValueAggregationLogicalFunction.hpp>
namespace NES
{

static std::pair<std::vector<Record::RecordFieldIdentifier>, std::vector<Record::RecordFieldIdentifier>>
getKeyAndValueFields(const WindowedAggregationLogicalOperator& logicalOperator)
{
    std::vector<Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<Record::RecordFieldIdentifier> fieldValueNames;

    /// Getting the key and value field names
    for (const auto& nodeAccess : logicalOperator.getGroupingKeys())
    {
        fieldKeyNames.emplace_back(nodeAccess.getFieldName());
    }
    for (const auto& descriptor : logicalOperator.getWindowAggregation())
    {
        const auto aggregationResultFieldIdentifier = descriptor->onField.getFieldName();
        fieldValueNames.emplace_back(aggregationResultFieldIdentifier);
    }
    return {fieldKeyNames, fieldValueNames};
}

static std::unique_ptr<TimeFunction> getTimeFunction(const WindowedAggregationLogicalOperator& logicalOperator)
{
    auto* const timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(logicalOperator.getWindowType().get());
    if (timeWindow == nullptr)
    {
        throw UnknownWindowType("Window type is not a time based window type");
    }

    switch (timeWindow->getTimeCharacteristic().getType())
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            if (timeWindow->getTimeCharacteristic().field.name == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
            {
                return std::make_unique<IngestionTimeFunction>();
            }
            throw UnknownWindowType(
                "The ingestion time field of a window must be: {}", Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME);
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            /// For event time fields, we look up the reference field name and create an expression to read the field.
            auto timeCharacteristicField = timeWindow->getTimeCharacteristic().field.name;
            auto timeStampField = FieldAccessPhysicalFunction(timeCharacteristicField);
            return std::make_unique<EventTimeFunction>(timeStampField, timeWindow->getTimeCharacteristic().getTimeUnit());
        }
        default: {
            throw UnknownWindowType("Unknown window type: {}", magic_enum::enum_name(timeWindow->getTimeCharacteristic().getType()));
        }
    }
}

namespace
{
std::vector<std::shared_ptr<AggregationPhysicalFunction>>
getAggregationPhysicalFunctions(const WindowedAggregationLogicalOperator& logicalOperator, const QueryExecutionConfiguration& configuration)
{
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    const auto& aggregationDescriptors = logicalOperator.getWindowAggregation();
    for (const auto& descriptor : aggregationDescriptors)
    {
        auto physicalInputType = DataTypeProvider::provideDataType(descriptor->getInputStamp().type);
        auto physicalFinalType = DataTypeProvider::provideDataType(descriptor->getFinalAggregateStamp().type);

        const auto resultFieldIdentifier = descriptor->asField.getFieldName();
        auto layout = std::make_shared<ColumnLayout>(configuration.pageSize.getValue(), logicalOperator.getInputSchemas()[0]);
        auto columnBufferRef = std::make_shared<Interface::BufferRef::ColumnTupleBufferRef>(layout);

        const auto name = descriptor->getName();

        // Custom lowering path for TEMPORAL_SEQUENCE: needs three field functions (lon, lat, ts)
#ifdef NES_ENABLE_MEOS
        if (name == std::string_view("TemporalSequence"))
        {
            auto tsDescriptor = std::dynamic_pointer_cast<TemporalSequenceAggregationLogicalFunctionV2>(descriptor);
            INVARIANT(tsDescriptor != nullptr, "Expected TemporalSequenceAggregationLogicalFunctionV2 for TemporalSequence");

            // Lower the three input fields (lon, lat, timestamp)
            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(tsDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(tsDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(tsDescriptor->getTimestampField());

            // Create a dedicated in-memory layout for the aggregation state (PagedVector) that
            // matches the field identifiers used by the physical function ("lon", "lat", "timestamp").
            // Using the input schema here is incorrect because it would not match the internal
            // record written by the aggregation state, causing lookups by name to fail.
            Schema stateSchema;
            stateSchema.addField("lon", tsDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", tsDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", tsDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalSequenceAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
#endif

        // Custom lowering path for TEMPORAL_LENGTH: same three-input shape as TEMPORAL_SEQUENCE,
        // returns a FLOAT64 (the spheroidal length of the per-(window, group) trajectory) instead of a VARSIZED WKB blob.
        if (name == std::string_view("TemporalLength"))
        {
            auto tlDescriptor = std::dynamic_pointer_cast<TemporalLengthAggregationLogicalFunction>(descriptor);
            INVARIANT(tlDescriptor != nullptr, "Expected TemporalLengthAggregationLogicalFunction for TemporalLength");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(tlDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(tlDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(tlDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", tlDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", tlDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", tlDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalLengthAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }

        // Custom lowering path for PAIR_MEETING (Q5): four input fields (lon, lat, ts, vehicle_id);
        // returns a VARSIZED string-encoded list of meeting pairs.
        if (name == std::string_view("PairMeeting"))
        {
            auto pmDescriptor = std::dynamic_pointer_cast<PairMeetingAggregationLogicalFunction>(descriptor);
            INVARIANT(pmDescriptor != nullptr, "Expected PairMeetingAggregationLogicalFunction for PairMeeting");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(pmDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(pmDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(pmDescriptor->getTimestampField());
            auto vidPF = QueryCompilation::FunctionProvider::lowerFunction(pmDescriptor->getVehicleIdField());

            Schema stateSchema;
            stateSchema.addField("lon", pmDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", pmDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", pmDescriptor->getTimestampField().getDataType());
            stateSchema.addField("vehicle_id", pmDescriptor->getVehicleIdField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<PairMeetingAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                vidPF,
                pmDescriptor->getDMeetMetres(),
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }

        // Custom lowering path for CROSS_DISTANCE (Q9): four input fields (lon, lat, ts, vehicle_id);
        // returns a FLOAT64 (distance between VID_A and VID_B latest positions in the window).
        if (name == std::string_view("CrossDistance"))
        {
            auto cdDescriptor = std::dynamic_pointer_cast<CrossDistanceAggregationLogicalFunction>(descriptor);
            INVARIANT(cdDescriptor != nullptr, "Expected CrossDistanceAggregationLogicalFunction for CrossDistance");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(cdDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(cdDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(cdDescriptor->getTimestampField());
            auto vidPF = QueryCompilation::FunctionProvider::lowerFunction(cdDescriptor->getVehicleIdField());

            Schema stateSchema;
            stateSchema.addField("lon", cdDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", cdDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", cdDescriptor->getTimestampField().getDataType());
            stateSchema.addField("vehicle_id", cdDescriptor->getVehicleIdField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<CrossDistanceAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                vidPF,
                cdDescriptor->getVidA(),
                cdDescriptor->getVidB(),
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalNumInstants (optimizer lowering) */
        if (name == std::string_view("TemporalNumInstants"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalNumInstantsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalNumInstantsAggregationLogicalFunction for TemporalNumInstants");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalNumInstantsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalNumInstants (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalNumSequences (optimizer lowering) */
        if (name == std::string_view("TemporalNumSequences"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalNumSequencesAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalNumSequencesAggregationLogicalFunction for TemporalNumSequences");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalNumSequencesAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalNumSequences (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalNumTimestamps (optimizer lowering) */
        if (name == std::string_view("TemporalNumTimestamps"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalNumTimestampsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalNumTimestampsAggregationLogicalFunction for TemporalNumTimestamps");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalNumTimestampsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalNumTimestamps (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTFloatStartValue (optimizer lowering) */
        if (name == std::string_view("TemporalTFloatStartValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTFloatStartValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTFloatStartValueAggregationLogicalFunction for TemporalTFloatStartValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTFloatStartValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTFloatStartValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTFloatEndValue (optimizer lowering) */
        if (name == std::string_view("TemporalTFloatEndValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTFloatEndValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTFloatEndValueAggregationLogicalFunction for TemporalTFloatEndValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTFloatEndValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTFloatEndValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTFloatMinValue (optimizer lowering) */
        if (name == std::string_view("TemporalTFloatMinValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTFloatMinValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTFloatMinValueAggregationLogicalFunction for TemporalTFloatMinValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTFloatMinValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTFloatMinValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTFloatMaxValue (optimizer lowering) */
        if (name == std::string_view("TemporalTFloatMaxValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTFloatMaxValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTFloatMaxValueAggregationLogicalFunction for TemporalTFloatMaxValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTFloatMaxValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTFloatMaxValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTNumberIntegral (optimizer lowering) */
        if (name == std::string_view("TemporalTNumberIntegral"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTNumberIntegralAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTNumberIntegralAggregationLogicalFunction for TemporalTNumberIntegral");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTNumberIntegralAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTNumberIntegral (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTIntStartValue (optimizer lowering) */
        if (name == std::string_view("TemporalTIntStartValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTIntStartValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTIntStartValueAggregationLogicalFunction for TemporalTIntStartValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTIntStartValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTIntStartValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTIntEndValue (optimizer lowering) */
        if (name == std::string_view("TemporalTIntEndValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTIntEndValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTIntEndValueAggregationLogicalFunction for TemporalTIntEndValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTIntEndValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTIntEndValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTIntMinValue (optimizer lowering) */
        if (name == std::string_view("TemporalTIntMinValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTIntMinValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTIntMinValueAggregationLogicalFunction for TemporalTIntMinValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTIntMinValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTIntMinValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTIntMaxValue (optimizer lowering) */
        if (name == std::string_view("TemporalTIntMaxValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTIntMaxValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTIntMaxValueAggregationLogicalFunction for TemporalTIntMaxValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTIntMaxValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTIntMaxValue (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTFloatAvgValue (optimizer lowering) */
        if (name == std::string_view("TemporalTFloatAvgValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTFloatAvgValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTFloatAvgValueAggregationLogicalFunction for TemporalTFloatAvgValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTFloatAvgValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTFloatAvgValue (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTNumberTwAvg (optimizer lowering) */
        if (name == std::string_view("TemporalTNumberTwAvg"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTNumberTwAvgAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTNumberTwAvgAggregationLogicalFunction for TemporalTNumberTwAvg");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTNumberTwAvgAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTNumberTwAvg (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTIntAvgValue (optimizer lowering) */
        if (name == std::string_view("TemporalTIntAvgValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTIntAvgValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTIntAvgValueAggregationLogicalFunction for TemporalTIntAvgValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTIntAvgValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTIntAvgValue (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalStartTimestamp (optimizer lowering) */
        if (name == std::string_view("TemporalStartTimestamp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalStartTimestampAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalStartTimestampAggregationLogicalFunction for TemporalStartTimestamp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalStartTimestampAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalStartTimestamp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalEndTimestamp (optimizer lowering) */
        if (name == std::string_view("TemporalEndTimestamp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalEndTimestampAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalEndTimestampAggregationLogicalFunction for TemporalEndTimestamp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalEndTimestampAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalEndTimestamp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalLowerInc (optimizer lowering) */
        if (name == std::string_view("TemporalLowerInc"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalLowerIncAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalLowerIncAggregationLogicalFunction for TemporalLowerInc");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalLowerIncAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalLowerInc (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalUpperInc (optimizer lowering) */
        if (name == std::string_view("TemporalUpperInc"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalUpperIncAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalUpperIncAggregationLogicalFunction for TemporalUpperInc");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalUpperIncAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalUpperInc (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalTPointIsSimple (optimizer lowering) */
        if (name == std::string_view("TemporalTPointIsSimple"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTPointIsSimpleAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTPointIsSimpleAggregationLogicalFunction for TemporalTPointIsSimple");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTPointIsSimpleAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalTPointIsSimple (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TspatialExtent (optimizer lowering) */
        if (name == std::string_view("TspatialExtent"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TspatialExtentAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TspatialExtentAggregationLogicalFunction for TspatialExtent");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TspatialExtentAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TspatialExtent (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TnumberExtent (optimizer lowering) */
        if (name == std::string_view("TnumberExtent"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberExtentAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberExtentAggregationLogicalFunction for TnumberExtent");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberExtentAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnumberExtent (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: FloatExtent (optimizer lowering) */
        if (name == std::string_view("FloatExtent"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<FloatExtentAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected FloatExtentAggregationLogicalFunction for FloatExtent");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<FloatExtentAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: FloatExtent (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: IntExtent (optimizer lowering) */
        if (name == std::string_view("IntExtent"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<IntExtentAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected IntExtentAggregationLogicalFunction for IntExtent");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<IntExtentAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: IntExtent (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: BigintExtent (optimizer lowering) */
        if (name == std::string_view("BigintExtent"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<BigintExtentAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected BigintExtentAggregationLogicalFunction for BigintExtent");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<BigintExtentAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: BigintExtent (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TimestamptzExtent (optimizer lowering) */
        if (name == std::string_view("TimestamptzExtent"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TimestamptzExtentAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TimestamptzExtentAggregationLogicalFunction for TimestamptzExtent");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TimestamptzExtentAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TimestamptzExtent (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: FloatUnion (optimizer lowering) */
        if (name == std::string_view("FloatUnion"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<FloatUnionAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected FloatUnionAggregationLogicalFunction for FloatUnion");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<FloatUnionAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: FloatUnion (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: IntUnion (optimizer lowering) */
        if (name == std::string_view("IntUnion"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<IntUnionAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected IntUnionAggregationLogicalFunction for IntUnion");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<IntUnionAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: IntUnion (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: BigintUnion (optimizer lowering) */
        if (name == std::string_view("BigintUnion"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<BigintUnionAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected BigintUnionAggregationLogicalFunction for BigintUnion");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<BigintUnionAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: BigintUnion (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TimestamptzUnion (optimizer lowering) */
        if (name == std::string_view("TimestamptzUnion"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TimestamptzUnionAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TimestamptzUnionAggregationLogicalFunction for TimestamptzUnion");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TimestamptzUnionAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TimestamptzUnion (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TrajectoryWkb (optimizer lowering) */
        if (name == std::string_view("TrajectoryWkb"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrajectoryWkbAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrajectoryWkbAggregationLogicalFunction for TrajectoryWkb");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrajectoryWkbAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TrajectoryWkb (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TLENGTH_EXP (optimizer lowering) */
        if (name == std::string_view("TLengthExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TLengthExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TLengthExpAggregationLogicalFunction for TLENGTH_EXP");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TLengthExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TLENGTH_EXP (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TgeoCentroidExp (optimizer lowering) */
        if (name == std::string_view("TgeoCentroidExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TgeoCentroidExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TgeoCentroidExpAggregationLogicalFunction for TgeoCentroidExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TgeoCentroidExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TgeoCentroidExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TpointAzimuthExp (optimizer lowering) */
        if (name == std::string_view("TpointAzimuthExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointAzimuthExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointAzimuthExpAggregationLogicalFunction for TpointAzimuthExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointAzimuthExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TpointAzimuthExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TpointAngularDifferenceExp (optimizer lowering) */
        if (name == std::string_view("TpointAngularDifferenceExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointAngularDifferenceExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointAngularDifferenceExpAggregationLogicalFunction for TpointAngularDifferenceExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointAngularDifferenceExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TpointAngularDifferenceExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TgeompointToTgeometryExp (optimizer lowering) */
        if (name == std::string_view("TgeompointToTgeometryExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TgeompointToTgeometryExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TgeompointToTgeometryExpAggregationLogicalFunction for TgeompointToTgeometryExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TgeompointToTgeometryExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TgeompointToTgeometryExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalCopyExp (optimizer lowering) */
        if (name == std::string_view("TemporalCopyExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalCopyExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalCopyExpAggregationLogicalFunction for TemporalCopyExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalCopyExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalCopyExp (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TnumberAbsExp (optimizer lowering) */
        if (name == std::string_view("TnumberAbsExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberAbsExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberAbsExpAggregationLogicalFunction for TnumberAbsExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberAbsExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnumberAbsExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TnumberDeltaValueExp (optimizer lowering) */
        if (name == std::string_view("TnumberDeltaValueExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberDeltaValueExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberDeltaValueExpAggregationLogicalFunction for TnumberDeltaValueExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberDeltaValueExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnumberDeltaValueExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TnumberAngularDifferenceExp (optimizer lowering) */
        if (name == std::string_view("TnumberAngularDifferenceExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberAngularDifferenceExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberAngularDifferenceExpAggregationLogicalFunction for TnumberAngularDifferenceExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberAngularDifferenceExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnumberAngularDifferenceExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalDerivativeExp (optimizer lowering) */
        if (name == std::string_view("TemporalDerivativeExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalDerivativeExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalDerivativeExpAggregationLogicalFunction for TemporalDerivativeExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalDerivativeExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalDerivativeExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalAtMaxExp (optimizer lowering) */
        if (name == std::string_view("TemporalAtMaxExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalAtMaxExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalAtMaxExpAggregationLogicalFunction for TemporalAtMaxExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalAtMaxExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalAtMaxExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalAtMinExp (optimizer lowering) */
        if (name == std::string_view("TemporalAtMinExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalAtMinExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalAtMinExpAggregationLogicalFunction for TemporalAtMinExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalAtMinExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalAtMinExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalMinusMaxExp (optimizer lowering) */
        if (name == std::string_view("TemporalMinusMaxExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalMinusMaxExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalMinusMaxExpAggregationLogicalFunction for TemporalMinusMaxExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalMinusMaxExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalMinusMaxExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TemporalMinusMinExp (optimizer lowering) */
        if (name == std::string_view("TemporalMinusMinExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalMinusMinExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalMinusMinExpAggregationLogicalFunction for TemporalMinusMinExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalMinusMinExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TemporalMinusMinExp (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TnpointCumulativeLengthExp (optimizer lowering) */
        if (name == std::string_view("TnpointCumulativeLengthExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointCumulativeLengthExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointCumulativeLengthExpAggregationLogicalFunction for TnpointCumulativeLengthExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointCumulativeLengthExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnpointCumulativeLengthExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TnpointSpeedExp (optimizer lowering) */
        if (name == std::string_view("TnpointSpeedExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointSpeedExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointSpeedExpAggregationLogicalFunction for TnpointSpeedExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointSpeedExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnpointSpeedExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TnpointToTgeompointExp (optimizer lowering) */
        if (name == std::string_view("TnpointToTgeompointExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointToTgeompointExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointToTgeompointExpAggregationLogicalFunction for TnpointToTgeompointExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointToTgeompointExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnpointToTgeompointExp (optimizer lowering) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TpointCumulativeLengthExp (optimizer lowering) */
        if (name == std::string_view("TpointCumulativeLengthExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointCumulativeLengthExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointCumulativeLengthExpAggregationLogicalFunction for TpointCumulativeLengthExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointCumulativeLengthExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TpointCumulativeLengthExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TpointSpeedExp (optimizer lowering) */
        if (name == std::string_view("TpointSpeedExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointSpeedExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointSpeedExpAggregationLogicalFunction for TpointSpeedExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointSpeedExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TpointSpeedExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TpointGetXExp (optimizer lowering) */
        if (name == std::string_view("TpointGetXExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointGetXExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointGetXExpAggregationLogicalFunction for TpointGetXExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointGetXExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TpointGetXExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TpointGetYExp (optimizer lowering) */
        if (name == std::string_view("TpointGetYExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointGetYExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointGetYExpAggregationLogicalFunction for TpointGetYExp");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointGetYExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TpointGetYExp (optimizer lowering) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TnumberTrendExp (optimizer lowering) */
        if (name == std::string_view("TnumberTrendExp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberTrendExpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberTrendExpAggregationLogicalFunction for TnumberTrendExp");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberTrendExpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN AGGREGATION GLUE: TnumberTrendExp (optimizer lowering) */













        // Default path: use registry for single-input aggregations
        auto aggregationInputFunction = QueryCompilation::FunctionProvider::lowerFunction(descriptor->onField);
        auto aggregationArguments = AggregationPhysicalFunctionRegistryArguments(
            std::move(physicalInputType),
            std::move(physicalFinalType),
            std::move(aggregationInputFunction),
            resultFieldIdentifier,
            columnBufferRef);
        if (auto aggregationPhysicalFunction
            = AggregationPhysicalFunctionRegistry::instance().create(std::string(name), std::move(aggregationArguments)))
        {
            aggregationPhysicalFunctions.push_back(aggregationPhysicalFunction.value());
        }
        else
        {
            throw UnknownAggregationType("unknown aggregation type: {}", name);
        }
    }
    return aggregationPhysicalFunctions;
}
}

RewriteRuleResultSubgraph LowerToPhysicalWindowedAggregation::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<WindowedAggregationLogicalOperator>(), "Expected a WindowedAggregationLogicalOperator");
    PRECONDITION(std::ranges::size(logicalOperator.getChildren()) == 1, "Expected one child");
    auto outputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getTraitSet());
    auto inputOriginIdsOpt = getTrait<OutputOriginIdsTrait>(logicalOperator.getChildren().at(0).getTraitSet());
    PRECONDITION(outputOriginIdsOpt.has_value(), "Expected the outputOriginIds trait to be set");
    PRECONDITION(inputOriginIdsOpt.has_value(), "Expected the inputOriginIds trait to be set");
    auto& outputOriginIds = outputOriginIdsOpt.value();
    PRECONDITION(std::ranges::size(outputOriginIds) == 1, "Expected one output origin id");
    PRECONDITION(logicalOperator.getInputSchemas().size() == 1, "Expected one input schema");

    auto aggregation = logicalOperator.getAs<WindowedAggregationLogicalOperator>();
    auto handlerId = getNextOperatorHandlerId();
    auto outputSchema = aggregation.getOutputSchema();
    auto outputOriginId = outputOriginIds[0];
    auto inputOriginIds = inputOriginIdsOpt.value();
    auto timeFunction = getTimeFunction(*aggregation);
    auto windowType = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(aggregation->getWindowType());
    INVARIANT(windowType != nullptr, "Window type must be a time-based window type");
    auto aggregationPhysicalFunctions = getAggregationPhysicalFunctions(*aggregation, conf);

    const auto valueSize = std::accumulate(
        aggregationPhysicalFunctions.begin(),
        aggregationPhysicalFunctions.end(),
        0,
        [](const auto& sum, const auto& function) { return sum + function->getSizeOfStateInBytes(); });

    uint64_t keySize = 0;
    std::vector<PhysicalFunction> keyFunctions;
    auto newInputSchema = aggregation.getInputSchemas()[0];
    for (auto& nodeFunctionKey : aggregation->getGroupingKeys())
    {
        auto loweredFunctionType = nodeFunctionKey.getDataType();
        if (loweredFunctionType.isType(DataType::Type::VARSIZED))
        {
            loweredFunctionType.type = DataType::Type::VARSIZED_POINTER_REP;
            const bool fieldReplaceSuccess = newInputSchema.replaceTypeOfField(nodeFunctionKey.getFieldName(), loweredFunctionType);
            INVARIANT(fieldReplaceSuccess, "Expect to change the type of {} for {}", nodeFunctionKey.getFieldName(), newInputSchema);
        }
        keyFunctions.emplace_back(QueryCompilation::FunctionProvider::lowerFunction(nodeFunctionKey));
        keySize += DataTypeProvider::provideDataType(loweredFunctionType.type).getSizeInBytes();
    }
    const auto entrySize = sizeof(Interface::ChainedHashMapEntry) + keySize + valueSize;
    const auto numberOfBuckets = conf.numberOfPartitions.getValue();
    const auto pageSize = conf.pageSize.getValue();
    const auto entriesPerPage = pageSize / entrySize;

    const auto& [fieldKeyNames, fieldValueNames] = getKeyAndValueFields(*aggregation);
    const auto& [fieldKeys, fieldValues]
        = Interface::BufferRef::ChainedEntryMemoryProvider::createFieldOffsets(newInputSchema, fieldKeyNames, fieldValueNames);

    const auto windowMetaData = WindowMetaData{aggregation->getWindowStartFieldName(), aggregation->getWindowEndFieldName()};

    const HashMapOptions hashMapOptions(
        std::make_unique<Interface::MurMur3HashFunction>(),
        keyFunctions,
        fieldKeys,
        fieldValues,
        entriesPerPage,
        entrySize,
        keySize,
        valueSize,
        pageSize,
        numberOfBuckets);

    auto sliceAndWindowStore
        = std::make_unique<DefaultTimeBasedSliceStore>(windowType->getSize().getTime(), windowType->getSlide().getTime());
    auto handler = std::make_shared<AggregationOperatorHandler>(
        inputOriginIds | std::ranges::to<std::vector>(), outputOriginId, std::move(sliceAndWindowStore), conf.maxNumberOfBuckets);
    auto build = AggregationBuildPhysicalOperator(handlerId, std::move(timeFunction), aggregationPhysicalFunctions, hashMapOptions);
    auto probe = AggregationProbePhysicalOperator(hashMapOptions, aggregationPhysicalFunctions, handlerId, windowMetaData);

    auto buildWrapper = std::make_shared<PhysicalOperatorWrapper>(
        build, newInputSchema, outputSchema, handlerId, handler, PhysicalOperatorWrapper::PipelineLocation::EMIT);

    auto probeWrapper = std::make_shared<PhysicalOperatorWrapper>(
        probe,
        newInputSchema,
        outputSchema,
        handlerId,
        handler,
        PhysicalOperatorWrapper::PipelineLocation::SCAN,
        std::vector{buildWrapper});

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), buildWrapper);
    return {.root = probeWrapper, .leafs = leafes};
}

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterWindowedAggregationRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalWindowedAggregation>(argument.conf);
}
}
