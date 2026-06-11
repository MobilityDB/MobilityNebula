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

#include <AntlrSQLParser/AntlrSQLQueryPlanCreator.hpp>

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>

#include <AntlrSQLBaseListener.h>
#include <AntlrSQLLexer.h>
#include <AntlrSQLParser.h>
#include <ParserRuleContext.h>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/MulLogicalFunction.hpp>
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/BooleanFunctions/NegateLogicalFunction.hpp>
#include <Functions/BooleanFunctions/OrLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessEqualsLogicalFunction.hpp>
#include <Functions/ComparisonFunctions/LessLogicalFunction.hpp>
#include <Functions/ConcatLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Operators/Windows/Aggregations/ArrayAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MaxAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/MedianAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/VarAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunctionV2.hpp>
#include <Operators/Windows/Aggregations/MinAggregationLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalIntersectsFunction.hpp>
#include <Operators/Windows/Aggregations/SumAggregationLogicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/Aggregations/Meos/VarAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalLengthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/PairMeetingAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CrossDistanceAggregationLogicalFunction.hpp>
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
#include <Operators/Windows/Aggregations/Meos/TemporalTFloatAvgValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTNumberTwAvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTIntAvgValueAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalStartTimestampAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalEndTimestampAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalLowerIncAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalUpperIncAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/TemporalTPointIsSimpleAggregationLogicalFunction.hpp>
#include <Functions/Meos/TemporalIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAtStBoxLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADFloatScalarLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADIntScalarLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTFloatLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTIntLogicalFunction.hpp>
#include <Functions/Meos/TemporalAtGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalMinusGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalACoversTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalACoversTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTCbufferGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTCbufferGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEContainsTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalECoversTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDisjointTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEIntersectsTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalETouchesTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAContainsTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADisjointTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalATouchesTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTPoseGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTPoseTPoseLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTNpointGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalADWithinTNpointTNpointLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTCbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/TemporalNADTCbufferTCbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverEqTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverEqTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverGeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverGeTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverGtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverGtTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverLeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverLeTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverLtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverLtTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverNeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverNeTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverEqFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverEqIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverGeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverGeIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverGeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverGtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverGtIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverGtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverLeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverLeIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverLeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverLtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverLtIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverLtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverNeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverNeIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverNeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTpointGeoLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTpointGeoLogicalFunction.hpp>
#include <Functions/Meos/EverEqTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EverEqTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EverEqTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EverEqTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EverNeTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EverNeTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EverNeTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EverNeTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AboveTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/AdjacentTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AdjacentTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/AfterTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AfterTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/BackTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/BeforeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/BeforeTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/BelowTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/ContainedTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/ContainedTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/ContainsTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/ContainsTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/EverEqTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/EverNeTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/FrontTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/LeftTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/NadTnpointGeoLogicalFunction.hpp>
#include <Functions/Meos/NadTposeGeoLogicalFunction.hpp>
#include <Functions/Meos/OveraboveTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverafterTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/OverafterTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverbackTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverbeforeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/OverbeforeTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverbelowTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverfrontTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverlapsTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/OverlapsTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverleftTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/OverrightTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/RightTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/SameTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/SameTspatialTspatialLogicalFunction.hpp>
#include <Functions/Meos/TboolEndValueLogicalFunction.hpp>
#include <Functions/Meos/TboolStartValueLogicalFunction.hpp>
#include <Functions/Meos/TemporalCmpLogicalFunction.hpp>
#include <Functions/Meos/TemporalDyntimewarpDistanceLogicalFunction.hpp>
#include <Functions/Meos/TemporalEqLogicalFunction.hpp>
#include <Functions/Meos/TemporalFrechetDistanceLogicalFunction.hpp>
#include <Functions/Meos/TemporalGeLogicalFunction.hpp>
#include <Functions/Meos/TemporalGtLogicalFunction.hpp>
#include <Functions/Meos/TemporalHausdorffDistanceLogicalFunction.hpp>
#include <Functions/Meos/TemporalLeLogicalFunction.hpp>
#include <Functions/Meos/TemporalLtLogicalFunction.hpp>
#include <Functions/Meos/TemporalNeLogicalFunction.hpp>
#include <Functions/Meos/TnpointLengthLogicalFunction.hpp>
#include <Functions/Meos/TboolToTintLogicalFunction.hpp>
#include <Functions/Meos/TcbufferToTfloatLogicalFunction.hpp>
#include <Functions/Meos/TfloatCeilLogicalFunction.hpp>
#include <Functions/Meos/TfloatExpLogicalFunction.hpp>
#include <Functions/Meos/TfloatFloorLogicalFunction.hpp>
#include <Functions/Meos/TfloatLnLogicalFunction.hpp>
#include <Functions/Meos/TfloatLog10LogicalFunction.hpp>
#include <Functions/Meos/TfloatRadiansLogicalFunction.hpp>
#include <Functions/Meos/TfloatToTintLogicalFunction.hpp>
#include <Functions/Meos/TintToTfloatLogicalFunction.hpp>
#include <Functions/Meos/AdjacentTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/AfterTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/BeforeTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/ContainedTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/ContainsTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/LeftTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/NadTcbufferStboxLogicalFunction.hpp>
#include <Functions/Meos/NadTfloatTboxLogicalFunction.hpp>
#include <Functions/Meos/NadTgeoStboxLogicalFunction.hpp>
#include <Functions/Meos/NadTintTboxLogicalFunction.hpp>
#include <Functions/Meos/NadTnpointStboxLogicalFunction.hpp>
#include <Functions/Meos/NadTposeStboxLogicalFunction.hpp>
#include <Functions/Meos/OverafterTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/OverbeforeTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/OverlapsTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/OverleftTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/OverrightTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/RightTnumberTboxLogicalFunction.hpp>
#include <Functions/Meos/SameTnumberTboxLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CommonParserFunctions.hpp>
#include <ErrorHandling.hpp>
#include <ParserUtil.hpp>

namespace NES::Parsers
{
LogicalPlan AntlrSQLQueryPlanCreator::getQueryPlan() const
{
    if (sinks.empty())
    {
        throw InvalidQuerySyntax("Query does not contain sink");
    }
    if (queryPlans.empty())
    {
        throw InvalidQuerySyntax("Query could not be parsed");
    }
    /// Todo #421: support multiple sinks
    INVARIANT(!sinks.empty(), "Need at least one sink!");
    return std::visit(
        Overloaded{
            [&](const std::string& sinkName) { return LogicalPlanBuilder::addSink(sinkName, queryPlans.top()); },
            [&](const std::pair<std::string, ConfigMap>& inlineSink)
            {
                const auto& [type, configOptions] = inlineSink;
                const auto sinkConfig = getSinkConfig(configOptions);
                const auto schemaOpt = getSinkSchema(configOptions);
                const Schema schema = (schemaOpt.has_value() ? schemaOpt.value() : Schema{});
                return LogicalPlanBuilder::addInlineSink(type, schema, sinkConfig, queryPlans.top());
            }},
        sinks.front());
}

Windowing::TimeMeasure buildTimeMeasure(const int size, const uint64_t timebase)
{
    switch (timebase) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::MS:
            return API::Milliseconds(size);
        case AntlrSQLLexer::SEC:
            return API::Seconds(size);
        case AntlrSQLLexer::MINUTE:
            return API::Minutes(size);
        case AntlrSQLLexer::HOUR:
            return API::Hours(size);
        case AntlrSQLLexer::DAY:
            return API::Days(size);
        default:
            const AntlrSQLLexer lexer(nullptr);
            const std::string tokenName = std::string(lexer.getVocabulary().getSymbolicName(timebase));
            throw InvalidQuerySyntax("Unknown time unit: {}", tokenName);
    }
}

static LogicalFunction createFunctionFromOpBoolean(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::EQ:
            return EqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::NEQJ:
            return NegateLogicalFunction(EqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction)));
        case AntlrSQLLexer::LT:
            return LessLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GT:
            return GreaterLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::GTE:
            return GreaterEqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::LTE:
            return LessEqualsLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown Comparison Operator: {} of type: {}", lexer.getVocabulary().getSymbolicName(tokenType), tokenType);
    }
}

static LogicalFunction createLogicalBinaryFunction(LogicalFunction leftFunction, LogicalFunction rightFunction, const uint64_t tokenType)
{
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::AND:
            return AndLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        case AntlrSQLLexer::OR:
            return OrLogicalFunction(std::move(leftFunction), std::move(rightFunction));
        default:
            auto lexer = AntlrSQLLexer(nullptr);
            throw InvalidQuerySyntax(
                "Unknown binary function in SQL query for op {} with type: {} and left {} and right {}",
                lexer.getVocabulary().getSymbolicName(tokenType),
                tokenType,
                std::move(leftFunction),
                std::move(rightFunction));
    }
}

void AntlrSQLQueryPlanCreator::enterSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helpers.top().isSelect = true;
    AntlrSQLBaseListener::enterSelectClause(context);
}

void AntlrSQLQueryPlanCreator::enterFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helpers.top().isFrom = true;
    AntlrSQLBaseListener::enterFromClause(context);
}

void AntlrSQLQueryPlanCreator::enterSinkClause(AntlrSQLParser::SinkClauseContext* context)
{
    if (context->sink().empty())
        throw InvalidQuerySyntax("INTO must be followed by at least one sink-identifier.");
    /// Store all specified sinks.
    for (const auto& sink : context->sink())
    {
        if (sink->identifier() != nullptr)
        {
            sinks.emplace_back(bindIdentifier(sink->identifier()));
        }
        else if (sink->inlineSink() != nullptr)
        {
            const auto& sinkInlineSink = sink->inlineSink();

            const auto type = bindIdentifier(sinkInlineSink->type);
            const auto configOptions = bindConfigOptions(sinkInlineSink->parameters->namedConfigExpression());

            sinks.emplace_back(std::make_pair(type, configOptions));
        }
    }
}

void AntlrSQLQueryPlanCreator::exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext* context)
{
    /// If we are exiting a logical binary operator in a join relation, we need to build the binary function for the joinKey and
    /// not for the general function
    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Expected two operands for binary op, got {}: {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto leftFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helpers.top().joinKeyRelationHelper.push_back(function);
    }
    else
    {
        if (helpers.top().functionBuilder.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Expected two operands for binary op, got {}: {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto leftFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();

        const auto opTokenType = context->op->getType();
        const auto function = createLogicalBinaryFunction(leftFunction, rightFunction, opTokenType);
        helpers.top().functionBuilder.push_back(function);
    }
}

void AntlrSQLQueryPlanCreator::exitSelectClause(AntlrSQLParser::SelectClauseContext* context)
{
    helpers.top().functionBuilder.clear();
    helpers.top().isSelect = false;
    AntlrSQLBaseListener::exitSelectClause(context);
}

void AntlrSQLQueryPlanCreator::exitFromClause(AntlrSQLParser::FromClauseContext* context)
{
    helpers.top().isFrom = false;
    AntlrSQLBaseListener::exitFromClause(context);
}

void AntlrSQLQueryPlanCreator::enterWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterWhereClause(context);
}

void AntlrSQLQueryPlanCreator::exitWhereClause(AntlrSQLParser::WhereClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    if (helpers.top().functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There were more than 1 functions in the functionBuilder in exitWhereClause.");
    }
    helpers.top().addWhereClause(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitWhereClause(context);
}

void AntlrSQLQueryPlanCreator::enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context)
{
    auto opTokenType = context->getStart()->getType();
    helpers.top().opBoolean = opTokenType;
    AntlrSQLBaseListener::enterComparisonOperator(context);
}

void AntlrSQLQueryPlanCreator::exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }
    LogicalFunction function;

    if (helpers.top().functionBuilder.size() < 2)
    {
        if (helpers.top().functionBuilder.size() + helpers.top().constantBuilder.size() == 2)
        {
            throw InvalidQuerySyntax(
                "Attempted to use a raw constant in a binary expression. {} in `{}`.",
                fmt::join(helpers.top().constantBuilder, ", "),
                context->getText());
        }
        throw InvalidQuerySyntax(
            "There were less than 2 functions in the functionBuilder in exitArithmeticBinary. `{}`.", context->getText());
    }
    const auto rightFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    const auto leftFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::ASTERISK:
            function = MulLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::SLASH:
            function = DivLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::PLUS:
            function = AddLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::MINUS:
            function = SubLogicalFunction(leftFunction, rightFunction);
            break;
        case AntlrSQLLexer::PERCENT:
            function = ModuloLogicalFunction(leftFunction, rightFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helpers.top().functionBuilder.push_back(function);
}

void AntlrSQLQueryPlanCreator::exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }
    LogicalFunction function;

    if (helpers.top().functionBuilder.empty())
    {
        throw InvalidQuerySyntax("Expected unary operator, got nothing: {}", context->getText());
    }
    const auto innerFunction = helpers.top().functionBuilder.back();
    helpers.top().functionBuilder.pop_back();
    auto opTokenType = context->op->getType();
    switch (opTokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::PLUS:
            function = innerFunction;
            break;
        case AntlrSQLLexer::MINUS:
            function = MulLogicalFunction(
                ConstantValueLogicalFunction(DataTypeProvider::provideDataType(DataType::Type::UINT64), "-1"), innerFunction);
            break;
        default:
            throw InvalidQuerySyntax("Unknown Arithmetic Binary Operator: {} of type: {}", context->op->getText(), opTokenType);
    }
    helpers.top().functionBuilder.push_back(function);
}

void AntlrSQLQueryPlanCreator::enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent);
    const bool isParentRuleTableAlias = (parentContext != nullptr) && parentContext->getRuleIndex() == AntlrSQLParser::RuleTableAlias;
    if (helpers.top().isFrom && !helpers.top().isJoinRelation)
    {
        helpers.top().newSourceName = context->getText();
    }
    else if (helpers.top().isJoinRelation && isParentRuleTableAlias)
    {
        helpers.top().joinSourceRenames.emplace_back(context->getText());
    }
    AntlrSQLBaseListener::enterUnquotedIdentifier(context);
}

void AntlrSQLQueryPlanCreator::enterIdentifier(AntlrSQLParser::IdentifierContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto* const parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }
    if (helpers.top().isGroupBy)
    {
        helpers.top().groupByFields.emplace_back(bindIdentifier(context));
    }
    else if (
        (helpers.top().isWhereOrHaving || helpers.top().isSelect || helpers.top().isWindow)
        && AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().functionBuilder.emplace_back(FieldAccessLogicalFunction(bindIdentifier(context)));
    }
    else if (helpers.top().isFrom and not helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        /// get main source name
        helpers.top().setSource(bindIdentifier(context));
    }
    else if (
        AntlrSQLParser::RuleNamedExpression == parentRuleIndex and helpers.top().isInFunctionCall() and not helpers.top().isJoinRelation
        and not helpers.top().isInAggFunction())
    {
        /// handle renames of identifiers
        if (helpers.top().isArithmeticBinary)
        {
            throw InvalidQuerySyntax("There must not be a binary arithmetic token at this point: {}", context->getText());
        }
        if ((helpers.top().isWhereOrHaving || helpers.top().isSelect))
        {
            /// The user specified named expression (field access or function) with 'AS THE_NAME'
            /// (we handle cases where the user did not specify a name via 'AS' in 'exitNamedExpression')
            const auto attribute = std::move(helpers.top().functionBuilder.back());
            helpers.top().functionBuilder.pop_back();
            helpers.top().addProjection(FieldIdentifier(bindIdentifier(context)), attribute);
        }
    }
    else if (helpers.top().isInAggFunction() and AntlrSQLParser::RuleNamedExpression == parentRuleIndex)
    {
        auto aggFunc = helpers.top().windowAggs.back();
        helpers.top().windowAggs.pop_back();
        aggFunc->asField = (FieldAccessLogicalFunction(bindIdentifier(context)));
        helpers.top().windowAggs.push_back(aggFunc);
        INVARIANT(
            std::nullopt != helpers.top().functionBuilder.back().tryGet<FieldAccessLogicalFunction>(),
            "The functionBuilder should hold the AccessFunction of the name of the field the aggregation is executed on.");
        helpers.top().functionBuilder.pop_back();
        helpers.top().addProjection(std::nullopt, aggFunc->asField);
        helpers.top().hasUnnamedAggregation = false;
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RulePrimaryExpression == parentRuleIndex)
    {
        helpers.top().joinKeyRelationHelper.emplace_back(FieldAccessLogicalFunction(bindIdentifier(context)));
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
    {
        helpers.top().joinSources.push_back(bindIdentifier(context));
    }
    else if (helpers.top().isJoinRelation and AntlrSQLParser::RuleTableAlias == parentRuleIndex)
    {
        helpers.top().joinSourceRenames.push_back(bindIdentifier(context));
    }
}

void AntlrSQLQueryPlanCreator::enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    if (not helpers.empty() and not helpers.top().isFrom and not helpers.top().isSetOperation)
    {
        throw InvalidQuerySyntax("Subqueries are only supported in FROM clauses, but got {}", context->getText());
    }

    const AntlrSQLHelper helper;
    helpers.push(helper);
    AntlrSQLBaseListener::enterPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context)
{
    LogicalPlan queryPlan;

    if (not helpers.top().queryPlans.empty())
    {
        queryPlan = std::move(helpers.top().queryPlans[0]);
    }
    else
    {
        if (helpers.top().getSource().empty())
        {
            const auto [type, configOptions] = helpers.top().getInlineSourceConfig();
            const auto parserConfig = getParserConfig(configOptions);
            const auto sourceConfig = getSourceConfig(configOptions);
            const auto schema = getSourceSchema(configOptions);
            if (!schema.has_value())
            {
                throw InvalidConfigParameter("Inline Source is missing schema definition");
            }

            queryPlan = LogicalPlanBuilder::createLogicalPlan(type, schema.value(), sourceConfig, parserConfig);
        }
        else
        {
            queryPlan = LogicalPlanBuilder::createLogicalPlan(helpers.top().getSource());
        }
    }

    for (auto whereExpr = helpers.top().getWhereClauses().rbegin(); whereExpr != helpers.top().getWhereClauses().rend(); ++whereExpr)
    {
        queryPlan = LogicalPlanBuilder::addSelection(std::move(*whereExpr), queryPlan);
    }

    if (helpers.top().isInAggFunction())
    {
        queryPlan = LogicalPlanBuilder::addWindowAggregation(
            queryPlan, helpers.top().windowType, helpers.top().windowAggs, helpers.top().groupByFields);
    }

    queryPlan = LogicalPlanBuilder::addProjection(helpers.top().getProjections(), helpers.top().asterisk, queryPlan);

    if (helpers.top().windowType != nullptr)
    {
        for (auto havingExpr = helpers.top().getHavingClauses().rbegin(); havingExpr != helpers.top().getHavingClauses().rend();
             ++havingExpr)
        {
            queryPlan = LogicalPlanBuilder::addSelection(*havingExpr, queryPlan);
        }
    }
    helpers.pop();
    if (helpers.empty())
    {
        queryPlans.push(queryPlan);
    }
    else
    {
        auto& subQueryHelper = helpers.top();
        subQueryHelper.queryPlans.push_back(queryPlan);
    }
    AntlrSQLBaseListener::exitPrimaryQuery(context);
}

void AntlrSQLQueryPlanCreator::enterWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helpers.top().isWindow = true;
    AntlrSQLBaseListener::enterWindowClause(context);
}

void AntlrSQLQueryPlanCreator::exitWindowClause(AntlrSQLParser::WindowClauseContext* context)
{
    helpers.top().isWindow = false;
    AntlrSQLBaseListener::exitWindowClause(context);
}

void AntlrSQLQueryPlanCreator::enterTimeUnit(AntlrSQLParser::TimeUnitContext* context)
{
    /// Get Index of Parent Rule to check type of parent rule in conditions
    std::optional<size_t> parentRuleIndex;
    if (const auto parentContext = dynamic_cast<antlr4::ParserRuleContext*>(context->parent); parentContext != nullptr)
    {
        parentRuleIndex = parentContext->getRuleIndex();
    }

    auto* token = context->getStop();
    auto timeunit = token->getType();
    if (parentRuleIndex == AntlrSQLParser::RuleAdvancebyParameter)
    {
        helpers.top().timeUnitAdvanceBy = timeunit;
    }
    else
    {
        helpers.top().timeUnit = timeunit;
    }
}

void AntlrSQLQueryPlanCreator::exitSizeParameter(AntlrSQLParser::SizeParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("SizeParameter must have 'size', a number, and a time unit.");
    }
    helpers.top().size = std::stoi(context->children.at(1)->getText());
    AntlrSQLBaseListener::exitSizeParameter(context);
}

void AntlrSQLQueryPlanCreator::exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("AdvancebyParameter must have 'ADVANCE BY', a number, and a time unit.");
    }
    helpers.top().advanceBy = std::stoi(context->children.at(2)->getText());
    AntlrSQLBaseListener::exitAdvancebyParameter(context);
}

void AntlrSQLQueryPlanCreator::exitTimestampParameter(AntlrSQLParser::TimestampParameterContext* context)
{
    helpers.top().timestamp = bindIdentifier(context->name);
}

/// WINDOWS
void AntlrSQLQueryPlanCreator::exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helpers.top().timestamp.empty())
    {
        helpers.top().windowType = std::make_shared<Windowing::TumblingWindow>(API::IngestionTime(), timeMeasure);
    }
    else
    {
        helpers.top().windowType = std::make_shared<Windowing::TumblingWindow>(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)), timeMeasure);
    }
    AntlrSQLBaseListener::exitTumblingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context)
{
    const auto timeMeasure = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    const auto slidingLength = buildTimeMeasure(helpers.top().advanceBy, helpers.top().timeUnitAdvanceBy);
    /// We use the ingestion time if the query does not have a timestamp fieldname specified
    if (helpers.top().timestamp.empty())
    {
        helpers.top().windowType = Windowing::SlidingWindow::of(API::IngestionTime(), timeMeasure, slidingLength);
    }
    else
    {
        helpers.top().windowType = Windowing::SlidingWindow::of(
            Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)),
            timeMeasure,
            slidingLength);
    }
    AntlrSQLBaseListener::exitSlidingWindow(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper& helper = helpers.top();
    if (context->name == nullptr and helper.functionBuilder.size() == 1
        and helper.functionBuilder.back().tryGet<FieldAccessLogicalFunction>() and not helpers.top().hasUnnamedAggregation)
    {
        /// Project onto the specified field and remove the field access from the active functions.
        helpers.top().addProjection(std::nullopt, std::move(helpers.top().functionBuilder.back()));
        helpers.top().functionBuilder.pop_back();
    }
    else if (helper.isSelect && context->getText() == "*" && helper.functionBuilder.empty())
    {
        helper.asterisk = true;
    }
    /// The user did not specify a new name (... AS THE_NAME) for the aggregation function and we need to generate one.
    else if (context->name == nullptr and not helpers.top().functionBuilder.empty() and helpers.top().hasUnnamedAggregation)
    {
        const auto accessFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto fieldAccessNode = accessFunction.get<FieldAccessLogicalFunction>();
        const auto lastAggregation = helpers.top().windowAggs.back();
        const auto newName = fmt::format("{}_{}", fieldAccessNode.getFieldName(), Util::toUpperCase(lastAggregation->getName()));
        const auto asField = FieldAccessLogicalFunction(newName);
        lastAggregation->asField = asField;
        helpers.top().windowAggs.pop_back();
        helpers.top().windowAggs.push_back(lastAggregation);
        helpers.top().addProjection(std::nullopt, asField);
        helpers.top().hasUnnamedAggregation = false;
    }
    AntlrSQLBaseListener::exitNamedExpression(context);
}

void AntlrSQLQueryPlanCreator::enterFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    AntlrSQLBaseListener::enterFunctionCall(context);
}

void AntlrSQLQueryPlanCreator::enterHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitHavingClause(AntlrSQLParser::HavingClauseContext* context)
{
    helpers.top().isWhereOrHaving = false;
    if (helpers.top().functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("There was more than one function in the functionBuilder in exitHavingClause.");
    }
    helpers.top().addHavingClause(helpers.top().functionBuilder.back());
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitHavingClause(context);
}

void AntlrSQLQueryPlanCreator::exitComparison(AntlrSQLParser::ComparisonContext* context)
{
    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.size() < 2)
        {
            throw InvalidQuerySyntax(
                "Requires two functions but got {} at {}", helpers.top().joinKeyRelationHelper.size(), context->getText());
        }
        const auto rightFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto leftFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helpers.top().opBoolean);
        helpers.top().joinKeyRelationHelper.push_back(function);
    }
    else
    {
        if (helpers.top().functionBuilder.size() < 2)
        {
            throw InvalidQuerySyntax("Comparison requires two parameters, got {}", context->getText());
        }
        const auto rightFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        const auto leftFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();

        const auto function = createFunctionFromOpBoolean(leftFunction, rightFunction, helpers.top().opBoolean);
        helpers.top().functionBuilder.push_back(function);
    }
    AntlrSQLBaseListener::exitComparison(context);
}

void AntlrSQLQueryPlanCreator::enterJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().joinKeyRelationHelper.clear();
    helpers.top().isJoinRelation = true;
    AntlrSQLBaseListener::enterJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext* context)
{
    INVARIANT(helpers.top().isJoinRelation, "Join criteria must be inside a join relation.");
    AntlrSQLBaseListener::enterJoinCriteria(context);
}

void AntlrSQLQueryPlanCreator::enterJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    if (not helpers.top().isJoinRelation)
    {
        throw InvalidQuerySyntax("Join type must be inside a join relation.");
    }
    AntlrSQLBaseListener::enterJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinType(AntlrSQLParser::JoinTypeContext* context)
{
    const auto joinType = context->getText();
    auto tokenType = context->getStop()->getType();

    if (joinType.empty() || tokenType == AntlrSQLLexer::INNER)
    {
        helpers.top().joinType = JoinLogicalOperator::JoinType::INNER_JOIN;
    }
    else
    {
        throw InvalidQuerySyntax("Unknown join type: {}, resolved to token type: {}", joinType, tokenType);
    }
    AntlrSQLBaseListener::exitJoinType(context);
}

void AntlrSQLQueryPlanCreator::exitJoinRelation(AntlrSQLParser::JoinRelationContext* context)
{
    helpers.top().isJoinRelation = false;
    if (helpers.top().joinSources.size() == helpers.top().joinSourceRenames.size() + 1)
    {
        helpers.top().joinSourceRenames.emplace_back("");
    }

    /// we assume that the left query plan is the first element in the queryPlans vector and the right query plan is the second element
    if (helpers.top().queryPlans.size() != 2)
    {
        throw InvalidQuerySyntax(
            "Join relation requires two subqueries, but got {} at {}", helpers.top().queryPlans.size(), context->getText());
    }
    const auto leftQueryPlan = helpers.top().queryPlans[0];
    const auto rightQueryPlan = helpers.top().queryPlans[1];
    helpers.top().queryPlans.clear();

    if (helpers.top().joinKeyRelationHelper.size() != 1)
    {
        throw InvalidQuerySyntax("joinFunction is required but empty at {}", context->getText());
    }
    if (!helpers.top().windowType)
    {
        throw InvalidQuerySyntax("windowType is required but empty at {}", context->getText());
    }
    const auto queryPlan = LogicalPlanBuilder::addJoin(
        leftQueryPlan, rightQueryPlan, helpers.top().joinKeyRelationHelper.at(0), helpers.top().windowType, helpers.top().joinType);
    if (not helpers.empty())
    {
        /// we are in a subquery
        helpers.top().queryPlans.push_back(queryPlan);
    }
    else
    {
        /// for now, we will never enter this branch, because we always have a subquery
        /// as we require the join relations to always be a sub-query
        queryPlans.push(queryPlan);
    }
    AntlrSQLBaseListener::exitJoinRelation(context);
}

void AntlrSQLQueryPlanCreator::exitLogicalNot(AntlrSQLParser::LogicalNotContext* context)
{
    if (helpers.empty())
    {
        throw InvalidQuerySyntax("Parser is confused at {}", context->getText());
    }

    if (helpers.top().isJoinRelation)
    {
        if (helpers.top().joinKeyRelationHelper.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        const auto innerFunction = helpers.top().joinKeyRelationHelper.back();
        helpers.top().joinKeyRelationHelper.pop_back();
        auto negatedFunction = NegateLogicalFunction(innerFunction);
        helpers.top().joinKeyRelationHelper.emplace_back(negatedFunction);
    }
    else
    {
        if (helpers.top().functionBuilder.empty())
        {
            throw InvalidQuerySyntax("Negate requires child op at {}", context->getText());
        }
        const auto innerFunction = helpers.top().functionBuilder.back();
        helpers.top().functionBuilder.pop_back();
        helpers.top().functionBuilder.emplace_back(NegateLogicalFunction(innerFunction));
    }
    AntlrSQLBaseListener::exitLogicalNot(context);
}

void AntlrSQLQueryPlanCreator::exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context)
{
    if (context->children.size() != 1)
    {
        throw InvalidQuerySyntax("When exiting a constant, there must be exactly one children in the context {}", context->getText());
    }
    if (const auto stringLiteralContext = dynamic_cast<AntlrSQLParser::StringLiteralContext*>(context->children.at(0)))
    {
        if (!(stringLiteralContext->getText().size() > 2))
        {
            throw InvalidQuerySyntax(
                "A constant string literal must contain at least two quotes and must not be empty at {}", context->getText());
        }
        helpers.top().constantBuilder.push_back(context->getText().substr(1, stringLiteralContext->getText().size() - 2));
    }
    else
    {
        helpers.top().constantBuilder.push_back(context->getText());
    }
}

void AntlrSQLQueryPlanCreator::exitFunctionCall(AntlrSQLParser::FunctionCallContext* context)
{
    const auto funcName = Util::toUpperCase(context->children[0]->getText());
    const auto tokenType = context->getStart()->getType();

    helpers.top().hasUnnamedAggregation = true;
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::COUNT:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                std::make_shared<CountAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::AVG:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                std::make_shared<AvgAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MAX:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                std::make_shared<MaxAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MIN:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                std::make_shared<MinAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::SUM:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                std::make_shared<SumAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::MEDIAN:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                std::make_shared<MedianAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
                case AntlrSQLLexer::ARRAY_AGG:
            helpers.top().windowAggs.push_back(
                ArrayAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::VAR:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            helpers.top().windowAggs.push_back(
                VarAggregationLogicalFunction::create(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            break;
        case AntlrSQLLexer::TEMPORAL_SEQUENCE:
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_SEQUENCE requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                // Verify all arguments are field access functions
                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_SEQUENCE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalSequenceAggregationLogicalFunctionV2::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                        latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                        timestampFunction.get<FieldAccessLogicalFunction>()));
                // Push back one field access function to satisfy parser expectations
                // This prevents the functionBuilder from being empty when processing the identifier
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        case AntlrSQLLexer::TEMPORAL_LENGTH:
            // Same three-input shape as TEMPORAL_SEQUENCE; differs only in the
            // result type (FLOAT64 instead of VARSIZED). Closes BerlinMOD-Q6 to a
            // full streaming-form cell.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_LENGTH requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_LENGTH arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalLengthAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                     latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                     timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        case AntlrSQLLexer::PAIR_MEETING:
            // Five-arg aggregation: lon, lat, ts, vehicle_id (FieldAccess) + dMeet
            // (numeric constant — meeting-distance threshold in metres). The first four
            // are pulled from functionBuilder; the fifth is pulled from constantBuilder
            // (the parser parks numeric/string literals there). Closes Q5 × 3 cells to
            // full; this branch makes the dMeet configurable per-query.
            {
                if (helpers.top().constantBuilder.empty()) {
                    throw InvalidQuerySyntax(
                        "PAIR_MEETING requires a numeric constant fifth argument (dMeet metres), "
                        "e.g. PAIR_MEETING(lon, lat, timestamp, vehicle_id, 200.0)");
                }
                auto dMeetString = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                double dMeetMetres;
                try {
                    dMeetMetres = std::stod(dMeetString);
                } catch (const std::exception&) {
                    throw InvalidQuerySyntax(
                        "PAIR_MEETING fifth argument must be a numeric constant (dMeet metres), got `{}`",
                        dMeetString);
                }

                if (helpers.top().functionBuilder.size() != 4) {
                    throw InvalidQuerySyntax(
                        "PAIR_MEETING requires exactly five arguments (lon, lat, timestamp, vehicle_id, dMeet), "
                        "got {} field args + 1 constant",
                        helpers.top().functionBuilder.size());
                }

                const auto vidFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !vidFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("PAIR_MEETING field arguments (lon, lat, timestamp, vehicle_id) must be field references");
                }

                helpers.top().windowAggs.push_back(
                    PairMeetingAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                  latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                  timestampFunction.get<FieldAccessLogicalFunction>(),
                                                                  vidFunction.get<FieldAccessLogicalFunction>(),
                                                                  dMeetMetres));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        case AntlrSQLLexer::CROSS_DISTANCE:
            // Six-arg aggregation: lon, lat, ts, vehicle_id (FieldAccess) + vidA, vidB
            // (numeric constants — target vehicle IDs). The first four are pulled from
            // functionBuilder; the fifth and sixth are pulled from constantBuilder.
            // Closes Q9 × 3 cells to full; this branch makes the target vehicle pair
            // configurable per-query. Mirrors PAIR_MEETING's 5-arg constant-parameterization
            // pattern (PR #19).
            {
                // Pull the two vid constants from constantBuilder. Note: the constants
                // are pushed in source order, so the LAST one pushed (vidB in the SQL
                // call) is on top of the stack — pop in reverse order.
                if (helpers.top().constantBuilder.size() < 2) {
                    throw InvalidQuerySyntax(
                        "CROSS_DISTANCE requires two numeric constant arguments (vidA, vidB), "
                        "e.g. CROSS_DISTANCE(lon, lat, timestamp, vehicle_id, 100, 200)");
                }
                auto vidBString = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                auto vidAString = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                uint64_t vidA, vidB;
                try {
                    vidA = std::stoull(vidAString);
                    vidB = std::stoull(vidBString);
                } catch (const std::exception&) {
                    throw InvalidQuerySyntax(
                        "CROSS_DISTANCE constant arguments must be unsigned integers (vidA, vidB), got `{}` and `{}`",
                        vidAString, vidBString);
                }

                if (helpers.top().functionBuilder.size() != 4) {
                    throw InvalidQuerySyntax(
                        "CROSS_DISTANCE requires exactly six arguments (lon, lat, timestamp, vehicle_id, vidA, vidB), "
                        "got {} field args + 2 constants",
                        helpers.top().functionBuilder.size());
                }

                const auto vidFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !vidFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("CROSS_DISTANCE field arguments (lon, lat, timestamp, vehicle_id) must be field references");
                }

                helpers.top().windowAggs.push_back(
                    CrossDistanceAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>(),
                                                                    vidFunction.get<FieldAccessLogicalFunction>(),
                                                                    vidA, vidB));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_GEOMETRY:
            {
                // Convert constants from constantBuilder to ConstantValueLogicalFunction objects
                while (!helpers.top().constantBuilder.empty()) {
                    auto constantValue = std::move(helpers.top().constantBuilder.back());
                    helpers.top().constantBuilder.pop_back();
                    // Assume string constants are VARSIZED (WKT strings)
                    auto dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                    auto constFunction = ConstantValueLogicalFunction(dataType, std::move(constantValue));
                    helpers.top().functionBuilder.push_back(constFunction);
                }

                const auto argCount = helpers.top().functionBuilder.size();
                if (argCount != 4 && argCount != 6) {
                    throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_GEOMETRY requires either 4 arguments (lon1, lat1, timestamp1, static_geometry) or 6 arguments (lon1, lat1, timestamp1, lon2, lat2, timestamp2), but got {}", argCount);
                }

                if (argCount == 4) {
                    // 4-parameter case: temporal-static intersection (lon1, lat1, timestamp1, static_geometry)
                    const auto staticGeometryFunction = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto timestamp1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lat1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lon1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    
                    const auto function = TemporalIntersectsGeometryLogicalFunction(lon1Function, lat1Function, timestamp1Function, staticGeometryFunction);
                    helpers.top().functionBuilder.push_back(function);
                } else {
                    // 6-parameter case: temporal-temporal intersection (lon1, lat1, timestamp1, lon2, lat2, timestamp2)
                    const auto timestamp2Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lat2Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lon2Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto timestamp1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lat1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lon1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    
                    const auto function = TemporalIntersectsGeometryLogicalFunction(lon1Function, lat1Function, timestamp1Function, lon2Function, lat2Function, timestamp2Function);
                    helpers.top().functionBuilder.push_back(function);
                }
            }
            break;
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_GEOMETRY:
            {
                // Convert constants from constantBuilder to ConstantValueLogicalFunction objects
                while (!helpers.top().constantBuilder.empty()) {
                    auto constantValue = std::move(helpers.top().constantBuilder.back());
                    helpers.top().constantBuilder.pop_back();
                    // Assume string constants are VARSIZED (WKT strings)
                    auto dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                    auto constFunction = ConstantValueLogicalFunction(dataType, std::move(constantValue));
                    helpers.top().functionBuilder.push_back(constFunction);
                }
                const auto argCount = helpers.top().functionBuilder.size();
                if (argCount != 4 && argCount != 6) {
                    throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_GEOMETRY requires either 4 arguments (lon1, lat1, timestamp1, static_geometry) or 6 arguments (lon1, lat1, timestamp1, lon2, lat2, timestamp2), but got {}", argCount);
                }
                if (argCount == 4) {
                    // 4-parameter case: temporal-static intersection (lon1, lat1, timestamp1, static_geometry)
                    const auto staticGeometryFunction = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto timestamp1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lat1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lon1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    
                    const auto function = TemporalAIntersectsGeometryLogicalFunction(lon1Function, lat1Function, timestamp1Function, staticGeometryFunction);
                    helpers.top().functionBuilder.push_back(function);
                } else {
                    // 6-parameter case: temporal-temporal intersection (lon1, lat1, timestamp1, lon2, lat2, timestamp2)
                    const auto timestamp2Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lat2Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lon2Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto timestamp1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lat1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    const auto lon1Function = helpers.top().functionBuilder.back();
                    helpers.top().functionBuilder.pop_back();
                    
                    const auto function = TemporalAIntersectsGeometryLogicalFunction(lon1Function, lat1Function, timestamp1Function, lon2Function, lat2Function, timestamp2Function);
                    helpers.top().functionBuilder.push_back(function);
                }
            }
            break;
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_GEOMETRY: 
        {
            // move any literal WKT that’s still on constantBuilder into functionBuilder
            while(!helpers.top().constantBuilder.empty()){
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            const auto n = helpers.top().functionBuilder.size();
            if(n==6){
                auto ts2 = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto lat2= helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto lon2= helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto ts1 = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto lat1= helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto lon1= helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    TemporalEContainsGeometryLogicalFunction(lon1,lat1,ts1,lon2,lat2,ts2));
            } else if(n==4){
                /* decide order by data-type of first arg */
                auto last = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto third= helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto second= helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                auto first = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if(first.getDataType().isType(DataType::Type::VARSIZED)) // static,tgeo
                    helpers.top().functionBuilder.emplace_back(
                        TemporalEContainsGeometryLogicalFunction(first,second,third,last));
                else                                                      // tgeo,static
                    helpers.top().functionBuilder.emplace_back(
                        TemporalEContainsGeometryLogicalFunction(first,second,third,last));
            } else {
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_GEOMETRY expects 4 or 6 arguments");
            }
        }
        break;
        case AntlrSQLLexer::EDWITHIN_TGEO_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
            {
                throw InvalidQuerySyntax("EDWITHIN_TGEO_GEO requires exactly five arguments (lon, lat, timestamp, geometry, distance), but got {}", argCount);
            }

            // Move pending constants into the function builder (WKT last)
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                const auto upperValue = Util::toUpperCase(constantValue);
                if (upperValue == "TRUE" || upperValue == "FALSE")
                {
                    dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
                }
                else
                {
                    char* endPtr = nullptr;
                    std::strtod(constantValue.c_str(), &endPtr);
                    if (endPtr != nullptr && *endPtr == '\0')
                    {
                        dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                    }
                    else
                    {
                        dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                    }
                }
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            const auto total = helpers.top().functionBuilder.size();
            PRECONDITION(total >= 5, "EDWITHIN_TGEO_GEO requires (lon, lat, timestamp, geometry, distance), but got {}", total);

            // Order after move: [lon, lat, ts, distance, geometry]
            auto geometryFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distanceFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestampFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinGeometryLogicalFunction(lonFunction, latFunction, timestampFunction, geometryFunction, distanceFunction));
        }
        break;
        case AntlrSQLLexer::TGEO_AT_STBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4 && argCount != 5)
            {
                throw InvalidQuerySyntax("TGEO_AT_STBOX requires four arguments (lon, lat, timestamp, stbox) with an optional fifth border_inc flag, but got {}", argCount);
            }

            // Move pending constants into the function builder (border first if present, STBOX last)
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                const auto upperValue = Util::toUpperCase(constantValue);
                if (upperValue == "TRUE" || upperValue == "FALSE")
                {
                    dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
                }
                else
                {
                    char* endPtr = nullptr;
                    std::strtod(constantValue.c_str(), &endPtr);
                    if (endPtr != nullptr && *endPtr == '\0')
                    {
                        dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                    }
                    else
                    {
                        dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                    }
                }
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            const auto total = helpers.top().functionBuilder.size();
            PRECONDITION(total >= 4, "TGEO_AT_STBOX requires (lon, lat, timestamp, stbox) with optional border flag, but got {}", total);

            auto stboxFunction = helpers.top().functionBuilder.back();
            helpers.top().functionBuilder.pop_back();

            LogicalFunction borderFlag = ConstantValueLogicalFunction(
                DataTypeProvider::provideDataType(DataType::Type::BOOLEAN), "TRUE");
            if (!helpers.top().functionBuilder.empty() && helpers.top().functionBuilder.back().getDataType().isType(DataType::Type::BOOLEAN))
            {
                borderFlag = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
            }

            auto timestampFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonFunction = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAtStBoxLogicalFunction(lonFunction, latFunction, timestampFunction, stboxFunction, borderFlag));
        }
        break;

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_NAD_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_NAD_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_NAD_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TGEOMETRY requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 7)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TGEOMETRY requires exactly 7 arguments (lonA, latA, tsA, lonB, latB, tsB, distance), but got {}", argCount);

            /* Lift the distance constant */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_GEOMETRY requires exactly 5 arguments (lon, lat, timestamp, geometry, distance), but got {}", argCount);

            /* Lift constants (geometry + distance) — same shape as EDWITHIN_TGEO_GEO */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                const auto upperValue = Util::toUpperCase(constantValue);
                if (upperValue == "TRUE" || upperValue == "FALSE")
                {
                    dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
                }
                else
                {
                    char* endPtr = nullptr;
                    std::strtod(constantValue.c_str(), &endPtr);
                    if (endPtr != nullptr && *endPtr == '\0')
                        dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                    else
                        dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                }
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            /* After lift: [lon, lat, ts, distance, geometry] (geometry pushed last because lifted last in LIFO) */
            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto dist      = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinGeometryLogicalFunction(lon, lat, timestamp, geometry, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TGEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TGEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 7)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TGEOMETRY requires exactly 7 arguments (lonA, latA, tsA, lonB, latB, tsB, distance), but got {}", argCount);

            /* Lift the distance constant */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTGeometryLogicalFunction(lonA, latA, tsA, lonB, latB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TGEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_GEOMETRY */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_FLOAT_SCALAR */
        case AntlrSQLLexer::TEMPORAL_NAD_FLOAT_SCALAR:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEMPORAL_NAD_FLOAT_SCALAR requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADFloatScalarLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_FLOAT_SCALAR */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_INT_SCALAR */
        case AntlrSQLLexer::TEMPORAL_NAD_INT_SCALAR:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEMPORAL_NAD_INT_SCALAR requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADIntScalarLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_INT_SCALAR */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TFLOAT */
        case AntlrSQLLexer::TEMPORAL_NAD_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TFLOAT requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTFloatLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TINT */
        case AntlrSQLLexer::TEMPORAL_NAD_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TINT requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTIntLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_AT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_AT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAtGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_MINUS_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_MINUS_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_MINUS_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalMinusGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_MINUS_GEOMETRY */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACOVERS_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ACOVERS_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ACOVERS_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalACoversTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACOVERS_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TCBUFFER */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACOVERS_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ACOVERS_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ACOVERS_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalACoversTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACOVERS_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY requires exactly 6 arguments (lon, lat, radius, timestamp, blob, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto blobLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTCbufferGeometryLogicalFunction(lon, lat, radius, timestamp, blobLast, distLast));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY requires exactly 6 arguments (lon, lat, radius, timestamp, blob, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto blobLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTCbufferGeometryLogicalFunction(lon, lat, radius, timestamp, blobLast, distLast));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER requires exactly 6 arguments (lon, lat, radius, timestamp, blob, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto blobLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, blobLast, distLast));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER requires exactly 6 arguments (lon, lat, radius, timestamp, blob, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto blobLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, blobLast, distLast));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 9)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER requires exactly 9 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 9)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER requires exactly 9 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TPOSE_GEOMETRY */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TPOSE_TPOSE */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ECONTAINS_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ECONTAINS_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEContainsTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECONTAINS_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ECOVERS_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ECOVERS_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalECoversTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ECOVERS_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_EDISJOINT_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EDISJOINT_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDisjointTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDISJOINT_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEIntersectsTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ETOUCHES_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ETOUCHES_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalETouchesTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ETOUCHES_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ACONTAINS_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ACONTAINS_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAContainsTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ACONTAINS_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ADISJOINT_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ADISJOINT_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADisjointTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADISJOINT_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalAIntersectsTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ATOUCHES_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ATOUCHES_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalATouchesTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ATOUCHES_TNPOINT_TNPOINT */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_NAD_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TPOSE_GEOMETRY requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_NAD_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TPOSE_TPOSE requires exactly 8 arguments (xA, yA, thetaA, tsA, xB, yB, thetaB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto thetaA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto yA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto xA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTPoseTPoseLogicalFunction(xA, yA, thetaA, tsA, xB, yB, thetaB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_NAD_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TNPOINT_GEOMETRY requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_NAD_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TNPOINT_TNPOINT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TNPOINT_TNPOINT */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TPOSE_GEOMETRY requires exactly 6 arguments (lon, lat, radius, timestamp, blob, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto blobLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, blobLast, distLast));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 9)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TPOSE_TPOSE requires exactly 9 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTPoseTPoseLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY requires exactly 5 arguments (lon, lat, timestamp, geometry, distance), but got {}", argCount);

            /* Lift constants (geometry + distance) — same shape as EDWITHIN_TGEO_GEO */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                const auto upperValue = Util::toUpperCase(constantValue);
                if (upperValue == "TRUE" || upperValue == "FALSE")
                {
                    dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
                }
                else
                {
                    char* endPtr = nullptr;
                    std::strtod(constantValue.c_str(), &endPtr);
                    if (endPtr != nullptr && *endPtr == '\0')
                        dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                    else
                        dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                }
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            /* After lift: [lon, lat, ts, distance, geometry] (geometry pushed last because lifted last in LIFO) */
            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto dist      = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_EDWITHIN_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 7)
                throw InvalidQuerySyntax("TEMPORAL_EDWITHIN_TNPOINT_TNPOINT requires exactly 7 arguments (lonA, latA, tsA, lonB, latB, tsB, distance), but got {}", argCount);

            /* Lift the distance constant */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEDWithinTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EDWITHIN_TNPOINT_TNPOINT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TPOSE_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TPOSE_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TPOSE_GEOMETRY requires exactly 6 arguments (lon, lat, radius, timestamp, blob, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto blobLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto distLast  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTPoseGeometryLogicalFunction(lon, lat, radius, timestamp, blobLast, distLast));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TPOSE_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TPOSE_TPOSE */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TPOSE_TPOSE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 9)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TPOSE_TPOSE requires exactly 9 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, distance), but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTPoseTPoseLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TPOSE_TPOSE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY requires exactly 5 arguments (lon, lat, timestamp, geometry, distance), but got {}", argCount);

            /* Lift constants (geometry + distance) — same shape as EDWITHIN_TGEO_GEO */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();

                DataType dataType;
                const auto upperValue = Util::toUpperCase(constantValue);
                if (upperValue == "TRUE" || upperValue == "FALSE")
                {
                    dataType = DataTypeProvider::provideDataType(DataType::Type::BOOLEAN);
                }
                else
                {
                    char* endPtr = nullptr;
                    std::strtod(constantValue.c_str(), &endPtr);
                    if (endPtr != nullptr && *endPtr == '\0')
                        dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                    else
                        dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                }
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            /* After lift: [lon, lat, ts, distance, geometry] (geometry pushed last because lifted last in LIFO) */
            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto dist      = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTNpointGeometryLogicalFunction(lon, lat, timestamp, geometry, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TNPOINT_TNPOINT */
        case AntlrSQLLexer::TEMPORAL_ADWITHIN_TNPOINT_TNPOINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 7)
                throw InvalidQuerySyntax("TEMPORAL_ADWITHIN_TNPOINT_TNPOINT requires exactly 7 arguments (lonA, latA, tsA, lonB, latB, tsB, distance), but got {}", argCount);

            /* Lift the distance constant */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::FLOAT64), std::move(v)));
            }

            auto dist = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalADWithinTNpointTNpointLogicalFunction(lonA, latA, tsA, lonB, latB, tsB, dist));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ADWITHIN_TNPOINT_TNPOINT */
        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_NAD_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TCBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::TEMPORAL_NAD_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTCbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NAD_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::TEMPORAL_NAD_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("TEMPORAL_NAD_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNADTCbufferTCbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NAD_TCBUFFER_TCBUFFER */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_EQ_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TINT_INT */
        case AntlrSQLLexer::ALWAYS_EQ_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_GE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGeTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TINT_INT */
        case AntlrSQLLexer::ALWAYS_GE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGeTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_GT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGtTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TINT_INT */
        case AntlrSQLLexer::ALWAYS_GT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGtTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_LE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLeTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TINT_INT */
        case AntlrSQLLexer::ALWAYS_LE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLeTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_LT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLtTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TINT_INT */
        case AntlrSQLLexer::ALWAYS_LT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLtTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_NE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TINT_INT */
        case AntlrSQLLexer::ALWAYS_NE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_EQ_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TINT_INT */
        case AntlrSQLLexer::EVER_EQ_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_GE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGeTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TINT_INT */
        case AntlrSQLLexer::EVER_GE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGeTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_GT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGtTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TINT_INT */
        case AntlrSQLLexer::EVER_GT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGtTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_LE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLeTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TINT_INT */
        case AntlrSQLLexer::EVER_LE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLeTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_LT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLtTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TINT_INT */
        case AntlrSQLLexer::EVER_LT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLtTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_NE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TFLOAT_FLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTfloatFloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TFLOAT_FLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TINT_INT */
        case AntlrSQLLexer::EVER_NE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TINT_INT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTintIntLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_EQ_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_INT_TINT */
        case AntlrSQLLexer::ALWAYS_EQ_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_EQ_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_EQ_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_GE_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGeFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_INT_TINT */
        case AntlrSQLLexer::ALWAYS_GE_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGeIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_GE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_GE_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGeTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_GT_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGtFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_INT_TINT */
        case AntlrSQLLexer::ALWAYS_GT_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGtIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_GT_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_GT_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysGtTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_LE_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLeFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_INT_TINT */
        case AntlrSQLLexer::ALWAYS_LE_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLeIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_LE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_LE_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLeTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_LT_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLtFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_INT_TINT */
        case AntlrSQLLexer::ALWAYS_LT_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLtIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_LT_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_LT_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysLtTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_NE_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_INT_TINT */
        case AntlrSQLLexer::ALWAYS_NE_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_NE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_NE_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_EQ_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_INT_TINT */
        case AntlrSQLLexer::EVER_EQ_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_EQ_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_EQ_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_GE_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGeFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_INT_TINT */
        case AntlrSQLLexer::EVER_GE_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGeIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_GE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_GE_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGeTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_GT_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGtFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_INT_TINT */
        case AntlrSQLLexer::EVER_GT_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGtIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_GT_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_GT_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverGtTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_LE_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLeFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_INT_TINT */
        case AntlrSQLLexer::EVER_LE_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLeIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_LE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_LE_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLeTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_LT_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLtFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_INT_TINT */
        case AntlrSQLLexer::EVER_LT_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLtIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_LT_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_LT_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverLtTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_NE_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_FLOAT_TFLOAT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeFloatTfloatLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_FLOAT_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_INT_TINT */
        case AntlrSQLLexer::EVER_NE_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_INT_TINT requires exactly 3 arguments (value, timestamp, scalar), but got {}", argCount);

            /* Lift the scalar constant — accept FLOAT64 (strtod-clean) and INT32 */
            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto scalar    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto value     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeIntTintLogicalFunction(value, timestamp, scalar));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_INT_TINT */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_NE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_NE_TEMPORAL_TEMPORAL requires exactly 4 arguments (valueA, tsA, valueB, tsB), but got {}", argCount);

            auto tsB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto valueA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTemporalTemporalLogicalFunction(valueA, tsA, valueB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::ALWAYS_EQ_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("ALWAYS_EQ_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTcbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::ALWAYS_EQ_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("ALWAYS_EQ_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTcbufferTcbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TGEO_GEO */
        case AntlrSQLLexer::ALWAYS_EQ_TGEO_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_EQ_TGEO_GEO requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTgeoGeoLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TGEO_GEO */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TGEO_TGEO */
        case AntlrSQLLexer::ALWAYS_EQ_TGEO_TGEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("ALWAYS_EQ_TGEO_TGEO requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysEqTgeoTgeoLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TGEO_TGEO */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::ALWAYS_NE_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("ALWAYS_NE_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTcbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::ALWAYS_NE_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("ALWAYS_NE_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTcbufferTcbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TGEO_GEO */
        case AntlrSQLLexer::ALWAYS_NE_TGEO_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ALWAYS_NE_TGEO_GEO requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTgeoGeoLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TGEO_GEO */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TGEO_TGEO */
        case AntlrSQLLexer::ALWAYS_NE_TGEO_TGEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("ALWAYS_NE_TGEO_TGEO requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AlwaysNeTgeoTgeoLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TGEO_TGEO */

        /* BEGIN CODEGEN PARSER GLUE: ATOUCHES_TPOINT_GEO */
        case AntlrSQLLexer::ATOUCHES_TPOINT_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ATOUCHES_TPOINT_GEO requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AtouchesTpointGeoLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: ATOUCHES_TPOINT_GEO */

        /* BEGIN CODEGEN PARSER GLUE: ETOUCHES_TPOINT_GEO */
        case AntlrSQLLexer::ETOUCHES_TPOINT_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("ETOUCHES_TPOINT_GEO requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EtouchesTpointGeoLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: ETOUCHES_TPOINT_GEO */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::EVER_EQ_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("EVER_EQ_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTcbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::EVER_EQ_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("EVER_EQ_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTcbufferTcbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TGEO_GEO */
        case AntlrSQLLexer::EVER_EQ_TGEO_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_EQ_TGEO_GEO requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTgeoGeoLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TGEO_GEO */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TGEO_TGEO */
        case AntlrSQLLexer::EVER_EQ_TGEO_TGEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("EVER_EQ_TGEO_TGEO requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverEqTgeoTgeoLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TGEO_TGEO */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TCBUFFER_CBUFFER */
        case AntlrSQLLexer::EVER_NE_TCBUFFER_CBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("EVER_NE_TCBUFFER_CBUFFER requires exactly 5 arguments (lon, lat, radius, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radius    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTcbufferCbufferLogicalFunction(lon, lat, radius, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TCBUFFER_CBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TCBUFFER_TCBUFFER */
        case AntlrSQLLexer::EVER_NE_TCBUFFER_TCBUFFER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 8)
                throw InvalidQuerySyntax("EVER_NE_TCBUFFER_TCBUFFER requires exactly 8 arguments (lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB), but got {}", argCount);

            auto tsB     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA     = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto radiusA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA    = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTcbufferTcbufferLogicalFunction(lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TCBUFFER_TCBUFFER */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TGEO_GEO */
        case AntlrSQLLexer::EVER_NE_TGEO_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("EVER_NE_TGEO_GEO requires exactly 4 arguments (lon, lat, timestamp, geometry), but got {}", argCount);

            /* Lift the WKT constant into the function builder */
            while (!helpers.top().constantBuilder.empty())
            {
                auto v = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(
                    ConstantValueLogicalFunction(
                        DataTypeProvider::provideDataType(DataType::Type::VARSIZED), std::move(v)));
            }

            auto geometry  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto timestamp = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lat       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lon       = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTgeoGeoLogicalFunction(lon, lat, timestamp, geometry));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TGEO_GEO */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TGEO_TGEO */
        case AntlrSQLLexer::EVER_NE_TGEO_TGEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("EVER_NE_TGEO_TGEO requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                EverNeTgeoTgeoLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TGEO_TGEO */
        /* BEGIN CODEGEN PARSER GLUE: ABOVE_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::ABOVE_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("ABOVE_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AboveTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ABOVE_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: ADJACENT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ADJACENT_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("ADJACENT_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AdjacentTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADJACENT_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: ADJACENT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::ADJACENT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("ADJACENT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AdjacentTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADJACENT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: AFTER_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::AFTER_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("AFTER_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AfterTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: AFTER_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: AFTER_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::AFTER_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("AFTER_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                AfterTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: AFTER_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TBOOL_BOOL */
        case AntlrSQLLexer::ALWAYS_EQ_TBOOL_BOOL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TBOOL_BOOL requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqTboolBoolLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TBOOL_BOOL */

        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TBOOL_BOOL */
        case AntlrSQLLexer::ALWAYS_NE_TBOOL_BOOL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TBOOL_BOOL requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeTboolBoolLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TBOOL_BOOL */

        /* BEGIN CODEGEN PARSER GLUE: BACK_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::BACK_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("BACK_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                BackTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: BACK_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: BEFORE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::BEFORE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("BEFORE_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                BeforeTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: BEFORE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: BEFORE_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::BEFORE_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("BEFORE_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                BeforeTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: BEFORE_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: BELOW_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::BELOW_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("BELOW_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                BelowTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: BELOW_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: CONTAINED_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::CONTAINED_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("CONTAINED_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                ContainedTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: CONTAINED_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: CONTAINED_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::CONTAINED_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("CONTAINED_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                ContainedTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: CONTAINED_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: CONTAINS_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::CONTAINS_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("CONTAINS_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                ContainsTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: CONTAINS_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: CONTAINS_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::CONTAINS_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("CONTAINS_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                ContainsTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: CONTAINS_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TBOOL_BOOL */
        case AntlrSQLLexer::EVER_EQ_TBOOL_BOOL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TBOOL_BOOL requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqTboolBoolLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TBOOL_BOOL */

        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TBOOL_BOOL */
        case AntlrSQLLexer::EVER_NE_TBOOL_BOOL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TBOOL_BOOL requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeTboolBoolLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TBOOL_BOOL */

        /* BEGIN CODEGEN PARSER GLUE: FRONT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::FRONT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("FRONT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                FrontTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: FRONT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: LEFT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::LEFT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("LEFT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                LeftTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: LEFT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TNPOINT_GEO */
        case AntlrSQLLexer::NAD_TNPOINT_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("NAD_TNPOINT_GEO requires exactly 4 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTnpointGeoLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TNPOINT_GEO */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TPOSE_GEO */
        case AntlrSQLLexer::NAD_TPOSE_GEO:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("NAD_TPOSE_GEO requires exactly 5 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a4 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTposeGeoLogicalFunction(a0, a1, a2, a3, a4));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TPOSE_GEO */

        /* BEGIN CODEGEN PARSER GLUE: OVERABOVE_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERABOVE_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERABOVE_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OveraboveTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERABOVE_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERAFTER_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::OVERAFTER_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERAFTER_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverafterTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERAFTER_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERAFTER_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERAFTER_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERAFTER_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverafterTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERAFTER_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERBACK_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERBACK_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERBACK_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverbackTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERBACK_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERBEFORE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::OVERBEFORE_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERBEFORE_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverbeforeTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERBEFORE_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERBEFORE_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERBEFORE_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERBEFORE_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverbeforeTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERBEFORE_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERBELOW_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERBELOW_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERBELOW_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverbelowTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERBELOW_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERFRONT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERFRONT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERFRONT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverfrontTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERFRONT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERLAPS_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::OVERLAPS_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERLAPS_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverlapsTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERLAPS_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERLAPS_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERLAPS_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERLAPS_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverlapsTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERLAPS_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERLEFT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERLEFT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERLEFT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverleftTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERLEFT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: OVERRIGHT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::OVERRIGHT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("OVERRIGHT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                OverrightTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERRIGHT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: RIGHT_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::RIGHT_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("RIGHT_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                RightTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: RIGHT_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: SAME_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::SAME_TEMPORAL_TEMPORAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("SAME_TEMPORAL_TEMPORAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                SameTemporalTemporalLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: SAME_TEMPORAL_TEMPORAL */

        /* BEGIN CODEGEN PARSER GLUE: SAME_TSPATIAL_TSPATIAL */
        case AntlrSQLLexer::SAME_TSPATIAL_TSPATIAL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("SAME_TSPATIAL_TSPATIAL requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                SameTspatialTspatialLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: SAME_TSPATIAL_TSPATIAL */

        /* BEGIN CODEGEN PARSER GLUE: TBOOL_END_VALUE */
        case AntlrSQLLexer::TBOOL_END_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TBOOL_END_VALUE requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TboolEndValueLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBOOL_END_VALUE */

        /* BEGIN CODEGEN PARSER GLUE: TBOOL_START_VALUE */
        case AntlrSQLLexer::TBOOL_START_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TBOOL_START_VALUE requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TboolStartValueLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBOOL_START_VALUE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_CMP */
        case AntlrSQLLexer::TEMPORAL_CMP:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_CMP requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalCmpLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_CMP */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_DYNTIMEWARP_DISTANCE */
        case AntlrSQLLexer::TEMPORAL_DYNTIMEWARP_DISTANCE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_DYNTIMEWARP_DISTANCE requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalDyntimewarpDistanceLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_DYNTIMEWARP_DISTANCE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_EQ */
        case AntlrSQLLexer::TEMPORAL_EQ:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_EQ requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalEqLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_EQ */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_FRECHET_DISTANCE */
        case AntlrSQLLexer::TEMPORAL_FRECHET_DISTANCE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_FRECHET_DISTANCE requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalFrechetDistanceLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_FRECHET_DISTANCE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_GE */
        case AntlrSQLLexer::TEMPORAL_GE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_GE requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalGeLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_GE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_GT */
        case AntlrSQLLexer::TEMPORAL_GT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_GT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalGtLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_GT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_HAUSDORFF_DISTANCE */
        case AntlrSQLLexer::TEMPORAL_HAUSDORFF_DISTANCE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_HAUSDORFF_DISTANCE requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalHausdorffDistanceLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_HAUSDORFF_DISTANCE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_LE */
        case AntlrSQLLexer::TEMPORAL_LE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_LE requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalLeLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_LE */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_LT */
        case AntlrSQLLexer::TEMPORAL_LT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_LT requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalLtLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_LT */

        /* BEGIN CODEGEN PARSER GLUE: TEMPORAL_NE */
        case AntlrSQLLexer::TEMPORAL_NE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 6)
                throw InvalidQuerySyntax("TEMPORAL_NE requires exactly 6 arguments (lonA, latA, tsA, lonB, latB, tsB), but got {}", argCount);

            auto tsB  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonB = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto tsA  = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto latA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto lonA = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(
                TemporalNeLogicalFunction(lonA, latA, tsA, lonB, latB, tsB));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_NE */

        /* BEGIN CODEGEN PARSER GLUE: TNPOINT_LENGTH */
        case AntlrSQLLexer::TNPOINT_LENGTH:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNPOINT_LENGTH requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TnpointLengthLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TNPOINT_LENGTH */
        /* BEGIN CODEGEN PARSER GLUE: TBOOL_TO_TINT */
        case AntlrSQLLexer::TBOOL_TO_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TBOOL_TO_TINT requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TboolToTintLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBOOL_TO_TINT */

        /* BEGIN CODEGEN PARSER GLUE: TCBUFFER_TO_TFLOAT */
        case AntlrSQLLexer::TCBUFFER_TO_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TCBUFFER_TO_TFLOAT requires exactly 4 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TcbufferToTfloatLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: TCBUFFER_TO_TFLOAT */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_CEIL */
        case AntlrSQLLexer::TFLOAT_CEIL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_CEIL requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatCeilLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_CEIL */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_EXP */
        case AntlrSQLLexer::TFLOAT_EXP:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_EXP requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatExpLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_EXP */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_FLOOR */
        case AntlrSQLLexer::TFLOAT_FLOOR:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_FLOOR requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatFloorLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_FLOOR */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_LN */
        case AntlrSQLLexer::TFLOAT_LN:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_LN requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatLnLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_LN */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_LOG10 */
        case AntlrSQLLexer::TFLOAT_LOG10:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_LOG10 requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatLog10LogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_LOG10 */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_RADIANS */
        case AntlrSQLLexer::TFLOAT_RADIANS:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_RADIANS requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatRadiansLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_RADIANS */

        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_TO_TINT */
        case AntlrSQLLexer::TFLOAT_TO_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_TO_TINT requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TfloatToTintLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_TO_TINT */

        /* BEGIN CODEGEN PARSER GLUE: TINT_TO_TFLOAT */
        case AntlrSQLLexer::TINT_TO_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TINT_TO_TFLOAT requires exactly 2 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TintToTfloatLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TINT_TO_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ADJACENT_TNUMBER_TBOX */
        case AntlrSQLLexer::ADJACENT_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADJACENT_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AdjacentTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADJACENT_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: AFTER_TNUMBER_TBOX */
        case AntlrSQLLexer::AFTER_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("AFTER_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AfterTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: AFTER_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: BEFORE_TNUMBER_TBOX */
        case AntlrSQLLexer::BEFORE_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("BEFORE_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(BeforeTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: BEFORE_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: CONTAINED_TNUMBER_TBOX */
        case AntlrSQLLexer::CONTAINED_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("CONTAINED_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(ContainedTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: CONTAINED_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: CONTAINS_TNUMBER_TBOX */
        case AntlrSQLLexer::CONTAINS_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("CONTAINS_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(ContainsTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: CONTAINS_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: LEFT_TNUMBER_TBOX */
        case AntlrSQLLexer::LEFT_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("LEFT_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(LeftTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: LEFT_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TCBUFFER_STBOX */
        case AntlrSQLLexer::NAD_TCBUFFER_STBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("NAD_TCBUFFER_STBOX requires exactly 5 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a4 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTcbufferStboxLogicalFunction(a0, a1, a2, a3, a4));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TCBUFFER_STBOX */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TFLOAT_TBOX */
        case AntlrSQLLexer::NAD_TFLOAT_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("NAD_TFLOAT_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTfloatTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TFLOAT_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TGEO_STBOX */
        case AntlrSQLLexer::NAD_TGEO_STBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("NAD_TGEO_STBOX requires exactly 4 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTgeoStboxLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TGEO_STBOX */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TINT_TBOX */
        case AntlrSQLLexer::NAD_TINT_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("NAD_TINT_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTintTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TINT_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TNPOINT_STBOX */
        case AntlrSQLLexer::NAD_TNPOINT_STBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("NAD_TNPOINT_STBOX requires exactly 4 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTnpointStboxLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TNPOINT_STBOX */

        /* BEGIN CODEGEN PARSER GLUE: NAD_TPOSE_STBOX */
        case AntlrSQLLexer::NAD_TPOSE_STBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
                throw InvalidQuerySyntax("NAD_TPOSE_STBOX requires exactly 5 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a4 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a3 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(NadTposeStboxLogicalFunction(a0, a1, a2, a3, a4));
        }
        break;
        /* END CODEGEN PARSER GLUE: NAD_TPOSE_STBOX */

        /* BEGIN CODEGEN PARSER GLUE: OVERAFTER_TNUMBER_TBOX */
        case AntlrSQLLexer::OVERAFTER_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("OVERAFTER_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(OverafterTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERAFTER_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: OVERBEFORE_TNUMBER_TBOX */
        case AntlrSQLLexer::OVERBEFORE_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("OVERBEFORE_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(OverbeforeTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERBEFORE_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: OVERLAPS_TNUMBER_TBOX */
        case AntlrSQLLexer::OVERLAPS_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("OVERLAPS_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(OverlapsTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERLAPS_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: OVERLEFT_TNUMBER_TBOX */
        case AntlrSQLLexer::OVERLEFT_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("OVERLEFT_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(OverleftTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERLEFT_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: OVERRIGHT_TNUMBER_TBOX */
        case AntlrSQLLexer::OVERRIGHT_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("OVERRIGHT_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(OverrightTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: OVERRIGHT_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: RIGHT_TNUMBER_TBOX */
        case AntlrSQLLexer::RIGHT_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("RIGHT_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(RightTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: RIGHT_TNUMBER_TBOX */

        /* BEGIN CODEGEN PARSER GLUE: SAME_TNUMBER_TBOX */
        case AntlrSQLLexer::SAME_TNUMBER_TBOX:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SAME_TNUMBER_TBOX requires exactly 3 arguments, but got {}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {
                auto constantValue = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                DataType dataType;
                char* endPtr = nullptr;
                std::strtod(constantValue.c_str(), &endPtr);
                if (endPtr != nullptr && *endPtr == '\0')
                    dataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
                else
                    dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
                helpers.top().functionBuilder.emplace_back(ConstantValueLogicalFunction(dataType, std::move(constantValue)));
            }

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(SameTnumberTboxLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SAME_TNUMBER_TBOX */
















        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_INSTANTS (case-switch) */
        case AntlrSQLLexer::TEMPORAL_NUM_INSTANTS:
            // Per-(window, group) count of instants in the assembled tgeo trajectory.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_NUM_INSTANTS requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_NUM_INSTANTS arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalNumInstantsAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_INSTANTS (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_SEQUENCES (case-switch) */
        case AntlrSQLLexer::TEMPORAL_NUM_SEQUENCES:
            // Per-(window, group) count of sub-sequences in the assembled tgeo trajectory.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_NUM_SEQUENCES requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_NUM_SEQUENCES arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalNumSequencesAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_SEQUENCES (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_TIMESTAMPS (case-switch) */
        case AntlrSQLLexer::TEMPORAL_NUM_TIMESTAMPS:
            // Per-(window, group) count of distinct timestamps in the assembled tgeo trajectory.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_NUM_TIMESTAMPS requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_NUM_TIMESTAMPS arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalNumTimestampsAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_TIMESTAMPS (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_START_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TFLOAT_START_VALUE:
            // Value at the first instant of the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TFLOAT_START_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_START_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTFloatStartValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_START_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_END_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TFLOAT_END_VALUE:
            // Value at the last instant of the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TFLOAT_END_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_END_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTFloatEndValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_END_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MIN_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TFLOAT_MIN_VALUE:
            // Minimum value across instants of the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TFLOAT_MIN_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_MIN_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTFloatMinValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MIN_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MAX_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TFLOAT_MAX_VALUE:
            // Maximum value across instants of the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TFLOAT_MAX_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_MAX_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTFloatMaxValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MAX_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_INTEGRAL (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TNUMBER_INTEGRAL:
            // Time-weighted integral (area under the value-vs-time curve) of the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TNUMBER_INTEGRAL requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TNUMBER_INTEGRAL arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTNumberIntegralAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_INTEGRAL (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_START_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TINT_START_VALUE:
            // Value at the first instant of the per-(window, group) tint sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TINT_START_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_START_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTIntStartValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_START_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_END_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TINT_END_VALUE:
            // Value at the last instant of the per-(window, group) tint sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TINT_END_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_END_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTIntEndValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_END_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MIN_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TINT_MIN_VALUE:
            // Minimum value across instants of the per-(window, group) tint sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TINT_MIN_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_MIN_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTIntMinValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MIN_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MAX_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TINT_MAX_VALUE:
            // Maximum value across instants of the per-(window, group) tint sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TINT_MAX_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_MAX_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTIntMaxValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MAX_VALUE (case-switch) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_AVG_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TFLOAT_AVG_VALUE:
            // Arithmetic mean of all instant values in the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TFLOAT_AVG_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_AVG_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTFloatAvgValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_AVG_VALUE (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_TWAVG (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TNUMBER_TWAVG:
            // Time-weighted average of values across the per-(window, group) tfloat sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TNUMBER_TWAVG requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TNUMBER_TWAVG arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTNumberTwAvgAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_TWAVG (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_AVG_VALUE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TINT_AVG_VALUE:
            // Arithmetic mean (as double) of all instant values in the per-(window, group) tint sequence.
            if (helpers.top().functionBuilder.size() != 2) {
                throw InvalidQuerySyntax("TEMPORAL_TINT_AVG_VALUE requires exactly two arguments (value, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_AVG_VALUE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTIntAvgValueAggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_AVG_VALUE (case-switch) */
        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_START_TIMESTAMP (case-switch) */
        case AntlrSQLLexer::TEMPORAL_START_TIMESTAMP:
            // TimestampTz (MEOS μs-since-2000) of the first instant in the per-(window, group) tgeo trajectory.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_START_TIMESTAMP requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_START_TIMESTAMP arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalStartTimestampAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_START_TIMESTAMP (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_END_TIMESTAMP (case-switch) */
        case AntlrSQLLexer::TEMPORAL_END_TIMESTAMP:
            // TimestampTz (MEOS μs-since-2000) of the last instant in the per-(window, group) tgeo trajectory.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_END_TIMESTAMP requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_END_TIMESTAMP arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalEndTimestampAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_END_TIMESTAMP (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_LOWER_INC (case-switch) */
        case AntlrSQLLexer::TEMPORAL_LOWER_INC:
            // True if the per-(window, group) tgeo trajectory's lower period bound is inclusive.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_LOWER_INC requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_LOWER_INC arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalLowerIncAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_LOWER_INC (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_UPPER_INC (case-switch) */
        case AntlrSQLLexer::TEMPORAL_UPPER_INC:
            // True if the per-(window, group) tgeo trajectory's upper period bound is inclusive.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_UPPER_INC requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_UPPER_INC arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalUpperIncAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_UPPER_INC (case-switch) */

        /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TPOINT_IS_SIMPLE (case-switch) */
        case AntlrSQLLexer::TEMPORAL_TPOINT_IS_SIMPLE:
            // True if the per-(window, group) tgeo trajectory does not self-intersect.
            if (helpers.top().functionBuilder.size() != 3) {
                throw InvalidQuerySyntax("TEMPORAL_TPOINT_IS_SIMPLE requires exactly three arguments (longitude, latitude, timestamp), but got {}", helpers.top().functionBuilder.size());
            }
            {
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {
                    throw InvalidQuerySyntax("TEMPORAL_TPOINT_IS_SIMPLE arguments must be field references");
                }

                helpers.top().windowAggs.push_back(
                    TemporalTPointIsSimpleAggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }
            break;
        /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TPOINT_IS_SIMPLE (case-switch) */







        default:
            /// Check if the function is a constructor for a datatype
            if (const auto dataType = DataTypeProvider::tryProvideDataType(funcName); dataType.has_value())
            {
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax("Expected constant, got nothing at {}", context->getText());
                }
                helpers.top().hasUnnamedAggregation = false;
                auto value = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                auto constFunctionItem = ConstantValueLogicalFunction(*dataType, std::move(value));
                helpers.top().functionBuilder.emplace_back(constFunctionItem);
            }
            else if (funcName == "VAR")
            {
                if (helpers.top().functionBuilder.empty())
                {
                    throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
                }
                const auto& lastArg = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().windowAggs.push_back(std::make_shared<VarAggregationLogicalFunction>(lastArg));
            }
            else if (funcName == "TEMPORAL_SEQUENCE")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_SEQUENCE requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalSequenceAggregationLogicalFunctionV2::create(lon, lat, ts));
            }
            else if (funcName == "TEMPORAL_LENGTH")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_LENGTH requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalLengthAggregationLogicalFunction::create(lon, lat, ts));
            }
            else if (funcName == "PAIR_MEETING")
            {
                // Five-arg shape: 4 FieldAccess + 1 numeric constant (dMeet metres).
                if (helpers.top().constantBuilder.empty())
                {
                    throw InvalidQuerySyntax(
                        "PAIR_MEETING requires a numeric constant fifth argument (dMeet metres) at {}",
                        context->getText());
                }
                auto dMeetString = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                double dMeetMetres;
                try { dMeetMetres = std::stod(dMeetString); }
                catch (const std::exception&) {
                    throw InvalidQuerySyntax(
                        "PAIR_MEETING fifth argument must be a numeric constant (dMeet metres), got `{}` at {}",
                        dMeetString, context->getText());
                }
                if (helpers.top().functionBuilder.size() < 4)
                {
                    throw InvalidQuerySyntax(
                        "PAIR_MEETING requires four field args + 1 constant at {}", context->getText());
                }
                const auto vid = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(PairMeetingAggregationLogicalFunction::create(lon, lat, ts, vid, dMeetMetres));
            }
            else if (funcName == "CROSS_DISTANCE")
            {
                // Six-arg shape: 4 FieldAccess + 2 numeric constants (vidA, vidB).
                if (helpers.top().constantBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax(
                        "CROSS_DISTANCE requires two numeric constant arguments (vidA, vidB) at {}",
                        context->getText());
                }
            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_INSTANTS (funcName chain) */
            else if (funcName == "TEMPORAL_NUM_INSTANTS")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_NUM_INSTANTS requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalNumInstantsAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_INSTANTS (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_SEQUENCES (funcName chain) */
            else if (funcName == "TEMPORAL_NUM_SEQUENCES")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_NUM_SEQUENCES requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalNumSequencesAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_SEQUENCES (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_TIMESTAMPS (funcName chain) */
            else if (funcName == "TEMPORAL_NUM_TIMESTAMPS")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_NUM_TIMESTAMPS requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalNumTimestampsAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_NUM_TIMESTAMPS (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_START_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TFLOAT_START_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_START_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTFloatStartValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_START_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_END_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TFLOAT_END_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_END_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTFloatEndValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_END_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MIN_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TFLOAT_MIN_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_MIN_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTFloatMinValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MIN_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MAX_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TFLOAT_MAX_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_MAX_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTFloatMaxValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_MAX_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_INTEGRAL (funcName chain) */
            else if (funcName == "TEMPORAL_TNUMBER_INTEGRAL")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TNUMBER_INTEGRAL requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTNumberIntegralAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_INTEGRAL (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_START_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TINT_START_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_START_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTIntStartValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_START_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_END_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TINT_END_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_END_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTIntEndValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_END_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MIN_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TINT_MIN_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_MIN_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTIntMinValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MIN_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MAX_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TINT_MAX_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_MAX_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTIntMaxValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_MAX_VALUE (funcName chain) */
            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_AVG_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TFLOAT_AVG_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TFLOAT_AVG_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTFloatAvgValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TFLOAT_AVG_VALUE (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_TWAVG (funcName chain) */
            else if (funcName == "TEMPORAL_TNUMBER_TWAVG")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TNUMBER_TWAVG requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTNumberTwAvgAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TNUMBER_TWAVG (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_AVG_VALUE (funcName chain) */
            else if (funcName == "TEMPORAL_TINT_AVG_VALUE")
            {
                if (helpers.top().functionBuilder.size() < 2)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TINT_AVG_VALUE requires two arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTIntAvgValueAggregationLogicalFunction::create(value, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TINT_AVG_VALUE (funcName chain) */
            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_START_TIMESTAMP (funcName chain) */
            else if (funcName == "TEMPORAL_START_TIMESTAMP")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_START_TIMESTAMP requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalStartTimestampAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_START_TIMESTAMP (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_END_TIMESTAMP (funcName chain) */
            else if (funcName == "TEMPORAL_END_TIMESTAMP")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_END_TIMESTAMP requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalEndTimestampAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_END_TIMESTAMP (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_LOWER_INC (funcName chain) */
            else if (funcName == "TEMPORAL_LOWER_INC")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_LOWER_INC requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalLowerIncAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_LOWER_INC (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_UPPER_INC (funcName chain) */
            else if (funcName == "TEMPORAL_UPPER_INC")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_UPPER_INC requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalUpperIncAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_UPPER_INC (funcName chain) */

            /* BEGIN CODEGEN AGGREGATION GLUE: TEMPORAL_TPOINT_IS_SIMPLE (funcName chain) */
            else if (funcName == "TEMPORAL_TPOINT_IS_SIMPLE")
            {
                if (helpers.top().functionBuilder.size() < 3)
                {
                    throw InvalidQuerySyntax("TEMPORAL_TPOINT_IS_SIMPLE requires three arguments at {}", context->getText());
                }
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(TemporalTPointIsSimpleAggregationLogicalFunction::create(lon, lat, ts));
            }
            /* END CODEGEN AGGREGATION GLUE: TEMPORAL_TPOINT_IS_SIMPLE (funcName chain) */


                auto vidBString = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                auto vidAString = std::move(helpers.top().constantBuilder.back());
                helpers.top().constantBuilder.pop_back();
                uint64_t vidA, vidB;
                try {
                    vidA = std::stoull(vidAString);
                    vidB = std::stoull(vidBString);
                } catch (const std::exception&) {
                    throw InvalidQuerySyntax(
                        "CROSS_DISTANCE constant arguments must be unsigned integers (vidA, vidB), got `{}` and `{}` at {}",
                        vidAString, vidBString, context->getText());
                }
                if (helpers.top().functionBuilder.size() < 4)
                {
                    throw InvalidQuerySyntax(
                        "CROSS_DISTANCE requires four field args + 2 constants at {}", context->getText());
                }
                const auto vid = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back(CrossDistanceAggregationLogicalFunction::create(lon, lat, ts, vid, vidA, vidB));
            }
            else if (auto logicalFunction = LogicalFunctionProvider::tryProvide(funcName, helpers.top().functionBuilder))
            {
                /// Remove exactly the functions used to create the 'logicalFunction' from the back of the function builder
                helpers.top().functionBuilder.resize(helpers.top().functionBuilder.size() - logicalFunction.value().getChildren().size());
                helpers.top().functionBuilder.push_back(*logicalFunction);
            }
            else if (funcName == "TEMPORAL_INTERSECTS")
            {
                if (helpers.top().functionBuilder.size() != 3) {
                    throw InvalidQuerySyntax("TEMPORAL_INTERSECTS requires exactly three arguments (lon, lat, timestamp), but got {}", helpers.top().functionBuilder.size());
                }
                const auto ts = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                helpers.top().functionBuilder.emplace_back(TemporalIntersectsFunction(lon, lat, ts));
            }
            else
            {
                throw InvalidQuerySyntax("Unknown (aggregation) function: {}, resolved to token type: {}", funcName, tokenType);
            }
    }
}

void AntlrSQLQueryPlanCreator::exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context)
{
    helpers.top().minimumCount = std::stoi(context->getText());
}

void AntlrSQLQueryPlanCreator::enterInlineSource(AntlrSQLParser::InlineSourceContext* context)
{
    const auto type = bindIdentifier(context->type);

    const auto parameters = bindConfigOptions(context->parameters->namedConfigExpression());

    helpers.top().setInlineSource(type, parameters);
}

void AntlrSQLQueryPlanCreator::enterSetOperation(AntlrSQLParser::SetOperationContext*)
{
    AntlrSQLHelper helper;
    helper.isSetOperation = true;
    helpers.push(helper);
}

void AntlrSQLQueryPlanCreator::exitSetOperation(AntlrSQLParser::SetOperationContext* context)
{
    INVARIANT(!helpers.empty(), "the set operation helper should not disappear before this function call");

    auto& helperPlans = helpers.top().queryPlans;
    if (helperPlans.size() < 2)
    {
        throw InvalidQuerySyntax("Union does not have sufficient amount of QueryPlans for unifying.");
    }

    auto rightQuery = std::move(helperPlans.back());
    helperPlans.pop_back();
    auto leftQuery = std::move(helperPlans.back());
    helperPlans.pop_back();
    helpers.pop();

    auto queryPlan = LogicalPlanBuilder::addUnion(std::move(leftQuery), std::move(rightQuery));
    if (!helpers.empty())
    {
        /// we are in a subquery
        helpers.top().queryPlans.push_back(std::move(queryPlan));
    }
    else
    {
        queryPlans.push(std::move(queryPlan));
    }
    AntlrSQLBaseListener::exitSetOperation(context);
}

void AntlrSQLQueryPlanCreator::enterGroupByClause(AntlrSQLParser::GroupByClauseContext* context)
{
    helpers.top().isGroupBy = true;
    AntlrSQLBaseListener::enterGroupByClause(context);
}

void AntlrSQLQueryPlanCreator::exitGroupByClause(AntlrSQLParser::GroupByClauseContext* context)
{
    helpers.top().isGroupBy = false;
    AntlrSQLBaseListener::exitGroupByClause(context);
}
}
