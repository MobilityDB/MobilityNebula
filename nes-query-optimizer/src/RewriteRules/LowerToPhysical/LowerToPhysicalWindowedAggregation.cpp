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
#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunctionV2.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalLengthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/PairMeetingAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CrossDistanceAggregationLogicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSequenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalLengthAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/PairMeetingAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/CrossDistanceAggregationPhysicalFunction.hpp>

#include <Aggregation/Function/Meos/TemporalNumInstantsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalNumSequencesAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalNumTimestampsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatStartValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatEndValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatMinValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatMaxValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberIntegralAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintStartValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintEndValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintMinValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintMaxValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberAvgValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberTwavgAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalStartTimestamptzAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalEndTimestamptzAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalLowerIncAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalUpperIncAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointIsSimpleAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TspatialExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/FloatExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/IntExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/BigintExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TimestamptzExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/FloatUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/IntUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/BigintUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TimestamptzUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointTrajectoryAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TgeoCentroidAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointAzimuthAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointAngularDifferenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalCopyAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberAbsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberDeltaValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberAngularDifferenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalDerivativeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalAtMaxAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalAtMinAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalMinusMaxAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalMinusMinAggregationPhysicalFunction.hpp>
#if NPOINT
#include <Aggregation/Function/Meos/Npoint/TnpointCumulativeLengthAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/Npoint/TnpointSpeedAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/Npoint/TnpointToTgeompointAggregationPhysicalFunction.hpp>
#endif
#include <Aggregation/Function/Meos/TpointCumulativeLengthAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointSpeedAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointGetXAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointGetYAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberTrendAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TgeoStartValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TgeoEndValueAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TgeoConvexHullAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointTwcentroidAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/DateExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/DateUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintTsumTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintTminTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintTmaxTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatTsumTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatTminTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatTmaxTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberTavgTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TboolTandTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TboolTorTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTcountTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintWminTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintWmaxTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TintWsumTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatWminTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatWmaxTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TfloatWsumTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TnumberWavgTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointTcentroidTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TtextTminTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TtextTmaxTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/SpanExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/SetExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/SpansetExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalExtentTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/SpanUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/SpansetUnionTransfnAggregationPhysicalFunction.hpp>
#if CBUFFER
#include <Aggregation/Function/Meos/Cbuffer/CbufferUnionTransfnAggregationPhysicalFunction.hpp>
#endif
#if NPOINT
#include <Aggregation/Function/Meos/Npoint/NpointUnionTransfnAggregationPhysicalFunction.hpp>
#endif
#if POSE
#include <Aggregation/Function/Meos/Pose/PoseUnionTransfnAggregationPhysicalFunction.hpp>
#endif
#include <Aggregation/Function/Meos/TextUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/GeoUnionTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalMergeTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TimestamptzTcountTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TstzsetTcountTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TstzspanTcountTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TstzspansetTcountTransfnAggregationPhysicalFunction.hpp>
#if NPOINT
#include <Aggregation/Function/Meos/Npoint/TnpointTcentroidTransfnAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/FloatsetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/IntsetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/BigintsetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/DatesetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TstzsetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalStartSequenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalEndSequenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSequenceNAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TextsetMakeAggregationPhysicalFunction.hpp>
#if CBUFFER
#include <Aggregation/Function/Meos/Cbuffer/CbuffersetMakeAggregationPhysicalFunction.hpp>
#endif
#if NPOINT
#include <Aggregation/Function/Meos/Npoint/NpointsetMakeAggregationPhysicalFunction.hpp>
#endif
#if POSE
#include <Aggregation/Function/Meos/Pose/PosesetMakeAggregationPhysicalFunction.hpp>
#endif
#include <Aggregation/Function/Meos/GeosetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/GeoCollectGarrayAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/GeoMakelineGarrayAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/GeomArrayUnionAggregationPhysicalFunction.hpp>
#if CBUFFER
#include <Aggregation/Function/Meos/Cbuffer/CbufferarrToGeomAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSegmentsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointMakeSimpleAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSequencesAggregationPhysicalFunction.hpp>
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometryInstantsAggregationPhysicalFunction.hpp>
#endif
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometrySegmentsAggregationPhysicalFunction.hpp>
#endif
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometrySequencesAggregationPhysicalFunction.hpp>
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometryStartSequenceAggregationPhysicalFunction.hpp>
#endif
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometryEndSequenceAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TpointDirectionAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSimplifyMinDistAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalTsampleAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalStopsAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalSegmDurationAggregationPhysicalFunction.hpp>
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometrySequenceNAggregationPhysicalFunction.hpp>
#endif
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometryRestrictValuesAggregationPhysicalFunction.hpp>
#endif
#if RGEO
#include <Aggregation/Function/Meos/Rgeo/TrgeometrySetInterpAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TemporalMergeArrayAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/SpansetMakeAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/Meos/TsequencesetMakeGapsAggregationPhysicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalMergeArrayAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/SpansetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TsequencesetMakeGapsAggregationLogicalFunction.hpp>
#endif
#include <Operators/Windows/Aggregations/Meos/TrgeometrySequenceNAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TrgeometryRestrictValuesAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TrgeometrySetInterpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTsampleAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalStopsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSegmDurationAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSimplifyMinDistAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointDirectionAggregationLogicalFunction.hpp>
#endif
#include <Operators/Windows/Aggregations/Meos/TrgeometryStartSequenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TrgeometryEndSequenceAggregationLogicalFunction.hpp>
#endif
#include <Operators/Windows/Aggregations/Meos/TrgeometryInstantsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TrgeometrySegmentsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TrgeometrySequencesAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSequencesAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSegmentsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointMakeSimpleAggregationLogicalFunction.hpp>
#endif
#include <Operators/Windows/Aggregations/Meos/GeoCollectGarrayAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/GeoMakelineGarrayAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/GeomArrayUnionAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CbufferarrToGeomAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TextsetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CbuffersetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/NpointsetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/PosesetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/GeosetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalStartSequenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalEndSequenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSequenceNAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/DatesetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TstzsetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/FloatsetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/IntsetMakeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/BigintsetMakeAggregationLogicalFunction.hpp>
#endif
#include <Operators/Windows/Aggregations/Meos/TemporalMergeTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TimestamptzTcountTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TstzsetTcountTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TstzspanTcountTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TstzspansetTcountTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointTcentroidTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/SpanExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/SetExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/SpansetExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/SpanUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/SpansetUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CbufferUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/NpointUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/PoseUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TextUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/GeoUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TtextTminTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TtextTmaxTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointTcentroidTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintWminTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintWmaxTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintWsumTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatWminTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatWmaxTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatWsumTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberWavgTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TboolTandTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TboolTorTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTcountTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintTsumTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintTminTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintTmaxTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatTsumTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatTminTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatTmaxTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberTavgTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/DateUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/DateExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TgeoStartValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TgeoEndValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TgeoConvexHullAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointTwcentroidAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointCumulativeLengthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointSpeedAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointGetXAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointGetYAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberTrendAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointCumulativeLengthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointSpeedAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnpointToTgeompointAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberAbsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberDeltaValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberAngularDifferenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalDerivativeAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalAtMaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalAtMinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalMinusMaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalMinusMinAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TgeoCentroidAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointAzimuthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointAngularDifferenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalCopyAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TLengthExpAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointTrajectoryAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/FloatUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/IntUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/BigintUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TimestamptzUnionTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/FloatExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/IntExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/BigintExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TimestamptzExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TspatialExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberExtentTransfnAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalStartTimestamptzAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalEndTimestamptzAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalLowerIncAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalUpperIncAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TpointIsSimpleAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberAvgValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberTwavgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntAvgValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalNumInstantsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalNumSequencesAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalNumTimestampsAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatStartValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatEndValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatMinValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TfloatMaxValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TnumberIntegralAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintStartValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintEndValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintMinValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TintMaxValueAggregationLogicalFunction.hpp>
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
        /* BEGIN CODEGEN GLUE: TemporalNumInstants (optimizer lowering) */
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
        /* END CODEGEN GLUE: TemporalNumInstants (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalNumSequences (optimizer lowering) */
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
        /* END CODEGEN GLUE: TemporalNumSequences (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalNumTimestamps (optimizer lowering) */
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
        /* END CODEGEN GLUE: TemporalNumTimestamps (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatStartValue (optimizer lowering) */
        if (name == std::string_view("TfloatStartValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatStartValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatStartValueAggregationLogicalFunction for TfloatStartValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatStartValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatStartValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatEndValue (optimizer lowering) */
        if (name == std::string_view("TfloatEndValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatEndValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatEndValueAggregationLogicalFunction for TfloatEndValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatEndValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatEndValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatMinValue (optimizer lowering) */
        if (name == std::string_view("TfloatMinValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatMinValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatMinValueAggregationLogicalFunction for TfloatMinValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatMinValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatMinValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatMaxValue (optimizer lowering) */
        if (name == std::string_view("TfloatMaxValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatMaxValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatMaxValueAggregationLogicalFunction for TfloatMaxValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatMaxValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatMaxValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberIntegral (optimizer lowering) */
        if (name == std::string_view("TnumberIntegral"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberIntegralAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberIntegralAggregationLogicalFunction for TnumberIntegral");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberIntegralAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberIntegral (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintStartValue (optimizer lowering) */
        if (name == std::string_view("TintStartValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintStartValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintStartValueAggregationLogicalFunction for TintStartValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintStartValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintStartValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintEndValue (optimizer lowering) */
        if (name == std::string_view("TintEndValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintEndValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintEndValueAggregationLogicalFunction for TintEndValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintEndValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintEndValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintMinValue (optimizer lowering) */
        if (name == std::string_view("TintMinValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintMinValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintMinValueAggregationLogicalFunction for TintMinValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintMinValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintMinValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintMaxValue (optimizer lowering) */
        if (name == std::string_view("TintMaxValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintMaxValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintMaxValueAggregationLogicalFunction for TintMaxValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintMaxValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintMaxValue (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TnumberAvgValue (optimizer lowering) */
        if (name == std::string_view("TnumberAvgValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberAvgValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberAvgValueAggregationLogicalFunction for TnumberAvgValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberAvgValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberAvgValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberTwavg (optimizer lowering) */
        if (name == std::string_view("TnumberTwavg"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberTwavgAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberTwavgAggregationLogicalFunction for TnumberTwavg");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberTwavgAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberTwavg (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalStartTimestamptz (optimizer lowering) */
        if (name == std::string_view("TemporalStartTimestamptz"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalStartTimestamptzAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalStartTimestamptzAggregationLogicalFunction for TemporalStartTimestamptz");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalStartTimestamptzAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalStartTimestamptz (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalEndTimestamptz (optimizer lowering) */
        if (name == std::string_view("TemporalEndTimestamptz"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalEndTimestamptzAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalEndTimestamptzAggregationLogicalFunction for TemporalEndTimestamptz");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalEndTimestamptzAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalEndTimestamptz (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalLowerInc (optimizer lowering) */
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
        /* END CODEGEN GLUE: TemporalLowerInc (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalUpperInc (optimizer lowering) */
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
        /* END CODEGEN GLUE: TemporalUpperInc (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointIsSimple (optimizer lowering) */
        if (name == std::string_view("TpointIsSimple"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointIsSimpleAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointIsSimpleAggregationLogicalFunction for TpointIsSimple");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointIsSimpleAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointIsSimple (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TspatialExtentTransfn (optimizer lowering) */
        if (name == std::string_view("TspatialExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TspatialExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TspatialExtentTransfnAggregationLogicalFunction for TspatialExtentTransfn");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TspatialExtentTransfnAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TspatialExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberExtentTransfn (optimizer lowering) */
        if (name == std::string_view("TnumberExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberExtentTransfnAggregationLogicalFunction for TnumberExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberExtentTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: FloatExtentTransfn (optimizer lowering) */
        if (name == std::string_view("FloatExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<FloatExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected FloatExtentTransfnAggregationLogicalFunction for FloatExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<FloatExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: FloatExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: IntExtentTransfn (optimizer lowering) */
        if (name == std::string_view("IntExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<IntExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected IntExtentTransfnAggregationLogicalFunction for IntExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<IntExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: IntExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: BigintExtentTransfn (optimizer lowering) */
        if (name == std::string_view("BigintExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<BigintExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected BigintExtentTransfnAggregationLogicalFunction for BigintExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<BigintExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: BigintExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TimestamptzExtentTransfn (optimizer lowering) */
        if (name == std::string_view("TimestamptzExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TimestamptzExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TimestamptzExtentTransfnAggregationLogicalFunction for TimestamptzExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TimestamptzExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TimestamptzExtentTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: FloatUnionTransfn (optimizer lowering) */
        if (name == std::string_view("FloatUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<FloatUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected FloatUnionTransfnAggregationLogicalFunction for FloatUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<FloatUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: FloatUnionTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: IntUnionTransfn (optimizer lowering) */
        if (name == std::string_view("IntUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<IntUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected IntUnionTransfnAggregationLogicalFunction for IntUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<IntUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: IntUnionTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: BigintUnionTransfn (optimizer lowering) */
        if (name == std::string_view("BigintUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<BigintUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected BigintUnionTransfnAggregationLogicalFunction for BigintUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<BigintUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: BigintUnionTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TimestamptzUnionTransfn (optimizer lowering) */
        if (name == std::string_view("TimestamptzUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TimestamptzUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TimestamptzUnionTransfnAggregationLogicalFunction for TimestamptzUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TimestamptzUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TimestamptzUnionTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TpointTrajectory (optimizer lowering) */
        if (name == std::string_view("TpointTrajectory"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointTrajectoryAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointTrajectoryAggregationLogicalFunction for TpointTrajectory");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointTrajectoryAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointTrajectory (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TgeoCentroid (optimizer lowering) */
        if (name == std::string_view("TgeoCentroid"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TgeoCentroidAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TgeoCentroidAggregationLogicalFunction for TgeoCentroid");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TgeoCentroidAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TgeoCentroid (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointAzimuth (optimizer lowering) */
        if (name == std::string_view("TpointAzimuth"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointAzimuthAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointAzimuthAggregationLogicalFunction for TpointAzimuth");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointAzimuthAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointAzimuth (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointAngularDifference (optimizer lowering) */
        if (name == std::string_view("TpointAngularDifference"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointAngularDifferenceAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointAngularDifferenceAggregationLogicalFunction for TpointAngularDifference");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointAngularDifferenceAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointAngularDifference (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalCopy (optimizer lowering) */
        if (name == std::string_view("TemporalCopy"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalCopyAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalCopyAggregationLogicalFunction for TemporalCopy");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalCopyAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalCopy (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TnumberAbs (optimizer lowering) */
        if (name == std::string_view("TnumberAbs"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberAbsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberAbsAggregationLogicalFunction for TnumberAbs");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberAbsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberAbs (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberDeltaValue (optimizer lowering) */
        if (name == std::string_view("TnumberDeltaValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberDeltaValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberDeltaValueAggregationLogicalFunction for TnumberDeltaValue");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberDeltaValueAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberDeltaValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberAngularDifference (optimizer lowering) */
        if (name == std::string_view("TnumberAngularDifference"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberAngularDifferenceAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberAngularDifferenceAggregationLogicalFunction for TnumberAngularDifference");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberAngularDifferenceAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberAngularDifference (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalDerivative (optimizer lowering) */
        if (name == std::string_view("TemporalDerivative"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalDerivativeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalDerivativeAggregationLogicalFunction for TemporalDerivative");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalDerivativeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalDerivative (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalAtMax (optimizer lowering) */
        if (name == std::string_view("TemporalAtMax"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalAtMaxAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalAtMaxAggregationLogicalFunction for TemporalAtMax");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalAtMaxAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalAtMax (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalAtMin (optimizer lowering) */
        if (name == std::string_view("TemporalAtMin"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalAtMinAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalAtMinAggregationLogicalFunction for TemporalAtMin");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalAtMinAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalAtMin (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalMinusMax (optimizer lowering) */
        if (name == std::string_view("TemporalMinusMax"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalMinusMaxAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalMinusMaxAggregationLogicalFunction for TemporalMinusMax");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalMinusMaxAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalMinusMax (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalMinusMin (optimizer lowering) */
        if (name == std::string_view("TemporalMinusMin"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalMinusMinAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalMinusMinAggregationLogicalFunction for TemporalMinusMin");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalMinusMinAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalMinusMin (optimizer lowering) */
#if NPOINT
        /* BEGIN CODEGEN GLUE: TnpointCumulativeLength (optimizer lowering) */
        if (name == std::string_view("TnpointCumulativeLength"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointCumulativeLengthAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointCumulativeLengthAggregationLogicalFunction for TnpointCumulativeLength");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointCumulativeLengthAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TnpointCumulativeLength (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnpointSpeed (optimizer lowering) */
        if (name == std::string_view("TnpointSpeed"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointSpeedAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointSpeedAggregationLogicalFunction for TnpointSpeed");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointSpeedAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TnpointSpeed (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnpointToTgeompoint (optimizer lowering) */
        if (name == std::string_view("TnpointToTgeompoint"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointToTgeompointAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointToTgeompointAggregationLogicalFunction for TnpointToTgeompoint");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointToTgeompointAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TnpointToTgeompoint (optimizer lowering) */
#endif
        /* BEGIN CODEGEN GLUE: TpointCumulativeLength (optimizer lowering) */
        if (name == std::string_view("TpointCumulativeLength"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointCumulativeLengthAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointCumulativeLengthAggregationLogicalFunction for TpointCumulativeLength");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointCumulativeLengthAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointCumulativeLength (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointSpeed (optimizer lowering) */
        if (name == std::string_view("TpointSpeed"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointSpeedAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointSpeedAggregationLogicalFunction for TpointSpeed");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointSpeedAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointSpeed (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointGetX (optimizer lowering) */
        if (name == std::string_view("TpointGetX"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointGetXAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointGetXAggregationLogicalFunction for TpointGetX");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointGetXAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointGetX (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointGetY (optimizer lowering) */
        if (name == std::string_view("TpointGetY"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointGetYAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointGetYAggregationLogicalFunction for TpointGetY");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointGetYAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointGetY (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberTrend (optimizer lowering) */
        if (name == std::string_view("TnumberTrend"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberTrendAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberTrendAggregationLogicalFunction for TnumberTrend");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberTrendAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberTrend (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TgeoStartValue (optimizer lowering) */
        if (name == std::string_view("TgeoStartValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TgeoStartValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TgeoStartValueAggregationLogicalFunction for TgeoStartValue");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TgeoStartValueAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TgeoStartValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TgeoEndValue (optimizer lowering) */
        if (name == std::string_view("TgeoEndValue"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TgeoEndValueAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TgeoEndValueAggregationLogicalFunction for TgeoEndValue");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TgeoEndValueAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TgeoEndValue (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TgeoConvexHull (optimizer lowering) */
        if (name == std::string_view("TgeoConvexHull"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TgeoConvexHullAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TgeoConvexHullAggregationLogicalFunction for TgeoConvexHull");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TgeoConvexHullAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TgeoConvexHull (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointTwcentroid (optimizer lowering) */
        if (name == std::string_view("TpointTwcentroid"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointTwcentroidAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointTwcentroidAggregationLogicalFunction for TpointTwcentroid");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointTwcentroidAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointTwcentroid (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: DateExtentTransfn (optimizer lowering) */
        if (name == std::string_view("DateExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<DateExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected DateExtentTransfnAggregationLogicalFunction for DateExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<DateExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: DateExtentTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: DateUnionTransfn (optimizer lowering) */
        if (name == std::string_view("DateUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<DateUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected DateUnionTransfnAggregationLogicalFunction for DateUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<DateUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: DateUnionTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TintTsumTransfn (optimizer lowering) */
        if (name == std::string_view("TintTsumTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintTsumTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintTsumTransfnAggregationLogicalFunction for TintTsumTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintTsumTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintTsumTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintTminTransfn (optimizer lowering) */
        if (name == std::string_view("TintTminTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintTminTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintTminTransfnAggregationLogicalFunction for TintTminTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintTminTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintTminTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintTmaxTransfn (optimizer lowering) */
        if (name == std::string_view("TintTmaxTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintTmaxTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintTmaxTransfnAggregationLogicalFunction for TintTmaxTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintTmaxTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintTmaxTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatTsumTransfn (optimizer lowering) */
        if (name == std::string_view("TfloatTsumTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatTsumTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatTsumTransfnAggregationLogicalFunction for TfloatTsumTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatTsumTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatTsumTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatTminTransfn (optimizer lowering) */
        if (name == std::string_view("TfloatTminTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatTminTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatTminTransfnAggregationLogicalFunction for TfloatTminTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatTminTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatTminTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatTmaxTransfn (optimizer lowering) */
        if (name == std::string_view("TfloatTmaxTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatTmaxTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatTmaxTransfnAggregationLogicalFunction for TfloatTmaxTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatTmaxTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatTmaxTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberTavgTransfn (optimizer lowering) */
        if (name == std::string_view("TnumberTavgTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberTavgTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberTavgTransfnAggregationLogicalFunction for TnumberTavgTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberTavgTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberTavgTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TboolTandTransfn (optimizer lowering) */
        if (name == std::string_view("TboolTandTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TboolTandTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TboolTandTransfnAggregationLogicalFunction for TboolTandTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TboolTandTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TboolTandTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TboolTorTransfn (optimizer lowering) */
        if (name == std::string_view("TboolTorTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TboolTorTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TboolTorTransfnAggregationLogicalFunction for TboolTorTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TboolTorTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TboolTorTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalTcountTransfn (optimizer lowering) */
        if (name == std::string_view("TemporalTcountTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTcountTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTcountTransfnAggregationLogicalFunction for TemporalTcountTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTcountTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalTcountTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TintWminTransfn (optimizer lowering) */
        if (name == std::string_view("TintWminTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintWminTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintWminTransfnAggregationLogicalFunction for TintWminTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintWminTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintWminTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintWmaxTransfn (optimizer lowering) */
        if (name == std::string_view("TintWmaxTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintWmaxTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintWmaxTransfnAggregationLogicalFunction for TintWmaxTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintWmaxTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintWmaxTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TintWsumTransfn (optimizer lowering) */
        if (name == std::string_view("TintWsumTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TintWsumTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TintWsumTransfnAggregationLogicalFunction for TintWsumTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TintWsumTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TintWsumTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatWminTransfn (optimizer lowering) */
        if (name == std::string_view("TfloatWminTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatWminTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatWminTransfnAggregationLogicalFunction for TfloatWminTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatWminTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatWminTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatWmaxTransfn (optimizer lowering) */
        if (name == std::string_view("TfloatWmaxTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatWmaxTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatWmaxTransfnAggregationLogicalFunction for TfloatWmaxTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatWmaxTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatWmaxTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TfloatWsumTransfn (optimizer lowering) */
        if (name == std::string_view("TfloatWsumTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TfloatWsumTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TfloatWsumTransfnAggregationLogicalFunction for TfloatWsumTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TfloatWsumTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TfloatWsumTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TnumberWavgTransfn (optimizer lowering) */
        if (name == std::string_view("TnumberWavgTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnumberWavgTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnumberWavgTransfnAggregationLogicalFunction for TnumberWavgTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnumberWavgTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnumberWavgTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TpointTcentroidTransfn (optimizer lowering) */
        if (name == std::string_view("TpointTcentroidTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointTcentroidTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointTcentroidTransfnAggregationLogicalFunction for TpointTcentroidTransfn");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointTcentroidTransfnAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointTcentroidTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TtextTminTransfn (optimizer lowering) */
        if (name == std::string_view("TtextTminTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TtextTminTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TtextTminTransfnAggregationLogicalFunction for TtextTminTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TtextTminTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TtextTminTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TtextTmaxTransfn (optimizer lowering) */
        if (name == std::string_view("TtextTmaxTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TtextTmaxTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TtextTmaxTransfnAggregationLogicalFunction for TtextTmaxTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TtextTmaxTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TtextTmaxTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: SpanExtentTransfn (optimizer lowering) */
        if (name == std::string_view("SpanExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<SpanExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected SpanExtentTransfnAggregationLogicalFunction for SpanExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<SpanExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: SpanExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: SetExtentTransfn (optimizer lowering) */
        if (name == std::string_view("SetExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<SetExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected SetExtentTransfnAggregationLogicalFunction for SetExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<SetExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: SetExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: SpansetExtentTransfn (optimizer lowering) */
        if (name == std::string_view("SpansetExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<SpansetExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected SpansetExtentTransfnAggregationLogicalFunction for SpansetExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<SpansetExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: SpansetExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalExtentTransfn (optimizer lowering) */
        if (name == std::string_view("TemporalExtentTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalExtentTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalExtentTransfnAggregationLogicalFunction for TemporalExtentTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalExtentTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalExtentTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: SpanUnionTransfn (optimizer lowering) */
        if (name == std::string_view("SpanUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<SpanUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected SpanUnionTransfnAggregationLogicalFunction for SpanUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<SpanUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: SpanUnionTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: SpansetUnionTransfn (optimizer lowering) */
        if (name == std::string_view("SpansetUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<SpansetUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected SpansetUnionTransfnAggregationLogicalFunction for SpansetUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<SpansetUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: SpansetUnionTransfn (optimizer lowering) */

#if CBUFFER
        /* BEGIN CODEGEN GLUE: CbufferUnionTransfn (optimizer lowering) */
        if (name == std::string_view("CbufferUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<CbufferUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected CbufferUnionTransfnAggregationLogicalFunction for CbufferUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<CbufferUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: CbufferUnionTransfn (optimizer lowering) */

#endif
#if NPOINT
        /* BEGIN CODEGEN GLUE: NpointUnionTransfn (optimizer lowering) */
        if (name == std::string_view("NpointUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<NpointUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected NpointUnionTransfnAggregationLogicalFunction for NpointUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<NpointUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: NpointUnionTransfn (optimizer lowering) */

#endif
#if POSE
        /* BEGIN CODEGEN GLUE: PoseUnionTransfn (optimizer lowering) */
        if (name == std::string_view("PoseUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<PoseUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected PoseUnionTransfnAggregationLogicalFunction for PoseUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<PoseUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: PoseUnionTransfn (optimizer lowering) */

#endif
        /* BEGIN CODEGEN GLUE: TextUnionTransfn (optimizer lowering) */
        if (name == std::string_view("TextUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TextUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TextUnionTransfnAggregationLogicalFunction for TextUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TextUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TextUnionTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: GeoUnionTransfn (optimizer lowering) */
        if (name == std::string_view("GeoUnionTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<GeoUnionTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected GeoUnionTransfnAggregationLogicalFunction for GeoUnionTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<GeoUnionTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: GeoUnionTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalMergeTransfn (optimizer lowering) */
        if (name == std::string_view("TemporalMergeTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalMergeTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalMergeTransfnAggregationLogicalFunction for TemporalMergeTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalMergeTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalMergeTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TimestamptzTcountTransfn (optimizer lowering) */
        if (name == std::string_view("TimestamptzTcountTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TimestamptzTcountTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TimestamptzTcountTransfnAggregationLogicalFunction for TimestamptzTcountTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TimestamptzTcountTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TimestamptzTcountTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TstzsetTcountTransfn (optimizer lowering) */
        if (name == std::string_view("TstzsetTcountTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TstzsetTcountTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TstzsetTcountTransfnAggregationLogicalFunction for TstzsetTcountTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TstzsetTcountTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TstzsetTcountTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TstzspanTcountTransfn (optimizer lowering) */
        if (name == std::string_view("TstzspanTcountTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TstzspanTcountTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TstzspanTcountTransfnAggregationLogicalFunction for TstzspanTcountTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TstzspanTcountTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TstzspanTcountTransfn (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TstzspansetTcountTransfn (optimizer lowering) */
        if (name == std::string_view("TstzspansetTcountTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TstzspansetTcountTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TstzspansetTcountTransfnAggregationLogicalFunction for TstzspansetTcountTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TstzspansetTcountTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TstzspansetTcountTransfn (optimizer lowering) */

#if NPOINT
        /* BEGIN CODEGEN GLUE: TnpointTcentroidTransfn (optimizer lowering) */
        if (name == std::string_view("TnpointTcentroidTransfn"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TnpointTcentroidTransfnAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TnpointTcentroidTransfnAggregationLogicalFunction for TnpointTcentroidTransfn");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TnpointTcentroidTransfnAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TnpointTcentroidTransfn (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: FloatsetMake (optimizer lowering) */
        if (name == std::string_view("FloatsetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<FloatsetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected FloatsetMakeAggregationLogicalFunction for FloatsetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<FloatsetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: FloatsetMake (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: IntsetMake (optimizer lowering) */
        if (name == std::string_view("IntsetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<IntsetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected IntsetMakeAggregationLogicalFunction for IntsetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<IntsetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: IntsetMake (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: BigintsetMake (optimizer lowering) */
        if (name == std::string_view("BigintsetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<BigintsetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected BigintsetMakeAggregationLogicalFunction for BigintsetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<BigintsetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: BigintsetMake (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: DatesetMake (optimizer lowering) */
        if (name == std::string_view("DatesetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<DatesetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected DatesetMakeAggregationLogicalFunction for DatesetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<DatesetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: DatesetMake (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TstzsetMake (optimizer lowering) */
        if (name == std::string_view("TstzsetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TstzsetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TstzsetMakeAggregationLogicalFunction for TstzsetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TstzsetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TstzsetMake (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalStartSequence (optimizer lowering) */
        if (name == std::string_view("TemporalStartSequence"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalStartSequenceAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalStartSequenceAggregationLogicalFunction for TemporalStartSequence");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalStartSequenceAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalStartSequence (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalEndSequence (optimizer lowering) */
        if (name == std::string_view("TemporalEndSequence"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalEndSequenceAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalEndSequenceAggregationLogicalFunction for TemporalEndSequence");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalEndSequenceAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalEndSequence (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalSequenceN (optimizer lowering) */
        if (name == std::string_view("TemporalSequenceN"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalSequenceNAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalSequenceNAggregationLogicalFunction for TemporalSequenceN");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalSequenceNAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalSequenceN (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TextsetMake (optimizer lowering) */
        if (name == std::string_view("TextsetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TextsetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TextsetMakeAggregationLogicalFunction for TextsetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TextsetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TextsetMake (optimizer lowering) */

#if CBUFFER
        /* BEGIN CODEGEN GLUE: CbuffersetMake (optimizer lowering) */
        if (name == std::string_view("CbuffersetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<CbuffersetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected CbuffersetMakeAggregationLogicalFunction for CbuffersetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<CbuffersetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: CbuffersetMake (optimizer lowering) */

#endif
#if NPOINT
        /* BEGIN CODEGEN GLUE: NpointsetMake (optimizer lowering) */
        if (name == std::string_view("NpointsetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<NpointsetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected NpointsetMakeAggregationLogicalFunction for NpointsetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<NpointsetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: NpointsetMake (optimizer lowering) */

#endif
#if POSE
        /* BEGIN CODEGEN GLUE: PosesetMake (optimizer lowering) */
        if (name == std::string_view("PosesetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<PosesetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected PosesetMakeAggregationLogicalFunction for PosesetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<PosesetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: PosesetMake (optimizer lowering) */

#endif
        /* BEGIN CODEGEN GLUE: GeosetMake (optimizer lowering) */
        if (name == std::string_view("GeosetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<GeosetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected GeosetMakeAggregationLogicalFunction for GeosetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<GeosetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: GeosetMake (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: GeoCollectGarray (optimizer lowering) */
        if (name == std::string_view("GeoCollectGarray"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<GeoCollectGarrayAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected GeoCollectGarrayAggregationLogicalFunction for GeoCollectGarray");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<GeoCollectGarrayAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: GeoCollectGarray (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: GeoMakelineGarray (optimizer lowering) */
        if (name == std::string_view("GeoMakelineGarray"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<GeoMakelineGarrayAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected GeoMakelineGarrayAggregationLogicalFunction for GeoMakelineGarray");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<GeoMakelineGarrayAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: GeoMakelineGarray (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: GeomArrayUnion (optimizer lowering) */
        if (name == std::string_view("GeomArrayUnion"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<GeomArrayUnionAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected GeomArrayUnionAggregationLogicalFunction for GeomArrayUnion");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<GeomArrayUnionAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: GeomArrayUnion (optimizer lowering) */

#if CBUFFER
        /* BEGIN CODEGEN GLUE: CbufferarrToGeom (optimizer lowering) */
        if (name == std::string_view("CbufferarrToGeom"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<CbufferarrToGeomAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected CbufferarrToGeomAggregationLogicalFunction for CbufferarrToGeom");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<CbufferarrToGeomAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: CbufferarrToGeom (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalSegments (optimizer lowering) */
        if (name == std::string_view("TemporalSegments"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalSegmentsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalSegmentsAggregationLogicalFunction for TemporalSegments");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalSegmentsAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalSegments (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TpointMakeSimple (optimizer lowering) */
        if (name == std::string_view("TpointMakeSimple"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointMakeSimpleAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointMakeSimpleAggregationLogicalFunction for TpointMakeSimple");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointMakeSimpleAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointMakeSimple (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalSequences (optimizer lowering) */
        if (name == std::string_view("TemporalSequences"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalSequencesAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalSequencesAggregationLogicalFunction for TemporalSequences");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalSequencesAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TemporalSequences (optimizer lowering) */
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometryInstants (optimizer lowering) */
        if (name == std::string_view("TrgeometryInstants"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometryInstantsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometryInstantsAggregationLogicalFunction for TrgeometryInstants");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometryInstantsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometryInstants (optimizer lowering) */

#endif
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometrySegments (optimizer lowering) */
        if (name == std::string_view("TrgeometrySegments"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometrySegmentsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometrySegmentsAggregationLogicalFunction for TrgeometrySegments");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometrySegmentsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometrySegments (optimizer lowering) */

#endif
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometrySequences (optimizer lowering) */
        if (name == std::string_view("TrgeometrySequences"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometrySequencesAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometrySequencesAggregationLogicalFunction for TrgeometrySequences");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometrySequencesAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometrySequences (optimizer lowering) */
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometryStartSequence (optimizer lowering) */
        if (name == std::string_view("TrgeometryStartSequence"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometryStartSequenceAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometryStartSequenceAggregationLogicalFunction for TrgeometryStartSequence");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometryStartSequenceAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometryStartSequence (optimizer lowering) */

#endif
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometryEndSequence (optimizer lowering) */
        if (name == std::string_view("TrgeometryEndSequence"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometryEndSequenceAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometryEndSequenceAggregationLogicalFunction for TrgeometryEndSequence");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometryEndSequenceAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometryEndSequence (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TpointDirection (optimizer lowering) */
        if (name == std::string_view("TpointDirection"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TpointDirectionAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TpointDirectionAggregationLogicalFunction for TpointDirection");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TpointDirectionAggregationPhysicalFunction>(
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
        /* END CODEGEN GLUE: TpointDirection (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalSimplifyMinDist (optimizer lowering) */
        if (name == std::string_view("TemporalSimplifyMinDist"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalSimplifyMinDistAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalSimplifyMinDistAggregationLogicalFunction for TemporalSimplifyMinDist");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalSimplifyMinDistAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalSimplifyMinDist (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalTsample (optimizer lowering) */
        if (name == std::string_view("TemporalTsample"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalTsampleAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalTsampleAggregationLogicalFunction for TemporalTsample");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalTsampleAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalTsample (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalStops (optimizer lowering) */
        if (name == std::string_view("TemporalStops"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalStopsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalStopsAggregationLogicalFunction for TemporalStops");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalStopsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalStops (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TemporalSegmDuration (optimizer lowering) */
        if (name == std::string_view("TemporalSegmDuration"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalSegmDurationAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalSegmDurationAggregationLogicalFunction for TemporalSegmDuration");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalSegmDurationAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalSegmDuration (optimizer lowering) */
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometrySequenceN (optimizer lowering) */
        if (name == std::string_view("TrgeometrySequenceN"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometrySequenceNAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometrySequenceNAggregationLogicalFunction for TrgeometrySequenceN");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometrySequenceNAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometrySequenceN (optimizer lowering) */

#endif
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometryRestrictValues (optimizer lowering) */
        if (name == std::string_view("TrgeometryRestrictValues"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometryRestrictValuesAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometryRestrictValuesAggregationLogicalFunction for TrgeometryRestrictValues");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometryRestrictValuesAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometryRestrictValues (optimizer lowering) */

#endif
#if RGEO
        /* BEGIN CODEGEN GLUE: TrgeometrySetInterp (optimizer lowering) */
        if (name == std::string_view("TrgeometrySetInterp"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TrgeometrySetInterpAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TrgeometrySetInterpAggregationLogicalFunction for TrgeometrySetInterp");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TrgeometrySetInterpAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TrgeometrySetInterp (optimizer lowering) */
        /* BEGIN CODEGEN GLUE: TemporalMergeArray (optimizer lowering) */
        if (name == std::string_view("TemporalMergeArray"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TemporalMergeArrayAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TemporalMergeArrayAggregationLogicalFunction for TemporalMergeArray");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TemporalMergeArrayAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TemporalMergeArray (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: SpansetMake (optimizer lowering) */
        if (name == std::string_view("SpansetMake"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<SpansetMakeAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected SpansetMakeAggregationLogicalFunction for SpansetMake");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<SpansetMakeAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: SpansetMake (optimizer lowering) */

        /* BEGIN CODEGEN GLUE: TsequencesetMakeGaps (optimizer lowering) */
        if (name == std::string_view("TsequencesetMakeGaps"))
        {
            auto specificDescriptor = std::dynamic_pointer_cast<TsequencesetMakeGapsAggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected TsequencesetMakeGapsAggregationLogicalFunction for TsequencesetMakeGaps");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<TsequencesetMakeGapsAggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }
        /* END CODEGEN GLUE: TsequencesetMakeGaps (optimizer lowering) */


#endif




#endif

#endif



#endif





#endif






















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
