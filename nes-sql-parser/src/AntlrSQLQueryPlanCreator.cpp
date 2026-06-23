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
#include <Functions/Meos/TemporalIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAtStBoxLogicalFunction.hpp>
#include <Functions/Meos/AddBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AddFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AddTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AddTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AddIntTintLogicalFunction.hpp>
#include <Functions/Meos/AddTintIntLogicalFunction.hpp>
#include <Functions/Meos/AddTnumberTnumberLogicalFunction.hpp>
#include <Functions/Meos/DivBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/DivFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/DivTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/DivTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/DivIntTintLogicalFunction.hpp>
#include <Functions/Meos/DivTintIntLogicalFunction.hpp>
#include <Functions/Meos/DivTnumberTnumberLogicalFunction.hpp>
#include <Functions/Meos/MulBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/MulFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/MulTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/MulTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/MulIntTintLogicalFunction.hpp>
#include <Functions/Meos/MulTintIntLogicalFunction.hpp>
#include <Functions/Meos/MulTnumberTnumberLogicalFunction.hpp>
#include <Functions/Meos/SubBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/SubFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/SubTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/SubTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/SubIntTintLogicalFunction.hpp>
#include <Functions/Meos/SubTintIntLogicalFunction.hpp>
#include <Functions/Meos/SubTnumberTnumberLogicalFunction.hpp>
#include <Functions/Meos/TbigintScaleValueLogicalFunction.hpp>
#include <Functions/Meos/TbigintShiftScaleValueLogicalFunction.hpp>
#include <Functions/Meos/TbigintShiftValueLogicalFunction.hpp>
#include <Functions/Meos/TbigintToTfloatLogicalFunction.hpp>
#include <Functions/Meos/TbigintToTintLogicalFunction.hpp>
#include <Functions/Meos/TdistanceTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TdistanceTintIntLogicalFunction.hpp>
#include <Functions/Meos/TdistanceTnumberTnumberLogicalFunction.hpp>
#include <Functions/Meos/TemporalRoundLogicalFunction.hpp>
#include <Functions/Meos/TfloatCeilLogicalFunction.hpp>
#include <Functions/Meos/TfloatCosLogicalFunction.hpp>
#include <Functions/Meos/TfloatDegreesLogicalFunction.hpp>
#include <Functions/Meos/TfloatExpLogicalFunction.hpp>
#include <Functions/Meos/TfloatFloorLogicalFunction.hpp>
#include <Functions/Meos/TfloatLnLogicalFunction.hpp>
#include <Functions/Meos/TfloatLog10LogicalFunction.hpp>
#include <Functions/Meos/TfloatRadiansLogicalFunction.hpp>
#include <Functions/Meos/TfloatScaleValueLogicalFunction.hpp>
#include <Functions/Meos/TfloatShiftScaleValueLogicalFunction.hpp>
#include <Functions/Meos/TfloatShiftValueLogicalFunction.hpp>
#include <Functions/Meos/TfloatSinLogicalFunction.hpp>
#include <Functions/Meos/TfloatTanLogicalFunction.hpp>
#include <Functions/Meos/TfloatToTbigintLogicalFunction.hpp>
#include <Functions/Meos/TfloatToTintLogicalFunction.hpp>
#include <Functions/Meos/TintScaleValueLogicalFunction.hpp>
#include <Functions/Meos/TintShiftScaleValueLogicalFunction.hpp>
#include <Functions/Meos/TintShiftValueLogicalFunction.hpp>
#include <Functions/Meos/TintToTbigintLogicalFunction.hpp>
#include <Functions/Meos/TintToTfloatLogicalFunction.hpp>
#include <Functions/Meos/TnumberAbsLogicalFunction.hpp>
#include <Functions/Meos/EverEqTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverGeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverGtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverLeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverLtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/EverNeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/EverGeTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/EverGtTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/EverLeTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/EverLtTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/EverNeTbigintBigintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverGeTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverGtTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverLeTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverLtTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverNeTbigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverGeTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverGtTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverLeTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverLtTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverNeTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTintIntLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTintTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTintTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTintTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTintTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTintTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTintTintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverGeTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverGtTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverLeTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverLtTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverNeTintIntLogicalFunction.hpp>
#include <Functions/Meos/EverEqTintTintLogicalFunction.hpp>
#include <Functions/Meos/EverGeTintTintLogicalFunction.hpp>
#include <Functions/Meos/EverGtTintTintLogicalFunction.hpp>
#include <Functions/Meos/EverLeTintTintLogicalFunction.hpp>
#include <Functions/Meos/EverLtTintTintLogicalFunction.hpp>
#include <Functions/Meos/EverNeTintTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTfloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverEqFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverGeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverGtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverLeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverLtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverNeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/EverEqIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverGeIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverGtIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverLeIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverLtIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverNeIntTintLogicalFunction.hpp>
#include <Functions/Meos/EverEqBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverGeBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverGtBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverLeBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverLtBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverNeBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeIntTintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeBigintTbigintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/EverNeTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/EverEqBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/EverNeBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/EverEqTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverGeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverGtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverLeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverLtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/EverNeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TandBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/TandTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/TandTboolTboolLogicalFunction.hpp>
#include <Functions/Meos/TnotTboolLogicalFunction.hpp>
#include <Functions/Meos/TorBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/TorTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/TorTboolTboolLogicalFunction.hpp>
#include <Functions/Meos/TeqBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/TeqFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/TeqIntTintLogicalFunction.hpp>
#include <Functions/Meos/TeqTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/TeqTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TeqTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TeqTintIntLogicalFunction.hpp>
#include <Functions/Meos/TneTboolBoolLogicalFunction.hpp>
#include <Functions/Meos/TneBoolTboolLogicalFunction.hpp>
#include <Functions/Meos/TneTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TneFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/TneTintIntLogicalFunction.hpp>
#include <Functions/Meos/TneIntTintLogicalFunction.hpp>
#include <Functions/Meos/TneTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TgeTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TgeFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/TgeTintIntLogicalFunction.hpp>
#include <Functions/Meos/TgeIntTintLogicalFunction.hpp>
#include <Functions/Meos/TgeTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TgtTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TgtFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/TgtTintIntLogicalFunction.hpp>
#include <Functions/Meos/TgtIntTintLogicalFunction.hpp>
#include <Functions/Meos/TgtTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TleTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TleFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/TleTintIntLogicalFunction.hpp>
#include <Functions/Meos/TleIntTintLogicalFunction.hpp>
#include <Functions/Meos/TleTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TltTfloatFloatLogicalFunction.hpp>
#include <Functions/Meos/TltFloatTfloatLogicalFunction.hpp>
#include <Functions/Meos/TltTintIntLogicalFunction.hpp>
#include <Functions/Meos/TltIntTintLogicalFunction.hpp>
#include <Functions/Meos/TltTemporalTemporalLogicalFunction.hpp>
#include <Functions/Meos/TboolToTintLogicalFunction.hpp>
#include <Functions/Meos/EverEqTtextTextLogicalFunction.hpp>
#include <Functions/Meos/EverNeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/EverGeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/EverGtTtextTextLogicalFunction.hpp>
#include <Functions/Meos/EverLeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/EverLtTtextTextLogicalFunction.hpp>
#include <Functions/Meos/EverEqTextTtextLogicalFunction.hpp>
#include <Functions/Meos/EverNeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/EverGeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/EverGtTextTtextLogicalFunction.hpp>
#include <Functions/Meos/EverLeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/EverLtTextTtextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTtextTextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTtextTextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTtextTextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTextTtextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysGtTextTtextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/AlwaysLtTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TeqTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TneTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TeqTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TneTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TgeTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TgeTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TgtTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TgtTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TleTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TleTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TltTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TltTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TtextUpperLogicalFunction.hpp>
#include <Functions/Meos/TtextLowerLogicalFunction.hpp>
#include <Functions/Meos/TtextInitcapLogicalFunction.hpp>
#include <Functions/Meos/TextcatTtextTextLogicalFunction.hpp>
#include <Functions/Meos/TextcatTextTtextLogicalFunction.hpp>
#include <Functions/Meos/TextcatTtextTtextLogicalFunction.hpp>
#include <Functions/Meos/GeomLengthLogicalFunction.hpp>
#include <Functions/Meos/GeomPerimeterLogicalFunction.hpp>
#include <Functions/Meos/GeomAzimuthLogicalFunction.hpp>
#include <Functions/Meos/GeogAreaLogicalFunction.hpp>
#include <Functions/Meos/GeogLengthLogicalFunction.hpp>
#include <Functions/Meos/GeogPerimeterLogicalFunction.hpp>
#include <Functions/Meos/GeomIsEmptyLogicalFunction.hpp>
#include <Functions/Meos/AintersectsTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AcoversTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AdisjointTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AdwithinTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EcontainsTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AcontainsTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/GeoNumPointsLogicalFunction.hpp>
#include <Functions/Meos/GeoNumGeosLogicalFunction.hpp>
#include <Functions/Meos/GeoSridLogicalFunction.hpp>
#include <Functions/Meos/GeoIsUnitaryLogicalFunction.hpp>
#include <Functions/Meos/GeoEqualsLogicalFunction.hpp>
#include <Functions/Meos/GeoSameLogicalFunction.hpp>
#include <Functions/Meos/GeogDistanceLogicalFunction.hpp>
#include <Functions/Meos/NadTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EverEqTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/EverNeTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTgeoGeoLogicalFunction.hpp>
#include <Functions/Meos/GeogIntersectsLogicalFunction.hpp>
#include <Functions/Meos/GeogDwithinLogicalFunction.hpp>
#include <Functions/Meos/GeomIntersectsLogicalFunction.hpp>
#include <Functions/Meos/GeomDwithinLogicalFunction.hpp>
#include <Functions/Meos/AcoversGeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/H3GsPointToCellLogicalFunction.hpp>
#include <Functions/Meos/EverEqTh3indexH3indexLogicalFunction.hpp>
#include <Functions/Meos/EverNeTh3indexH3indexLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTh3indexH3indexLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTh3indexH3indexLogicalFunction.hpp>
#include <Functions/Meos/Th3indexGetResolutionLogicalFunction.hpp>
#include <Functions/Meos/Th3indexGetBaseCellNumberLogicalFunction.hpp>
#include <Functions/Meos/Th3indexIsValidCellLogicalFunction.hpp>
#include <Functions/Meos/Th3indexIsPentagonLogicalFunction.hpp>
#include <Functions/Meos/Th3indexCellToParentNextLogicalFunction.hpp>
#include <Functions/Meos/Th3indexCellToCenterChildNextLogicalFunction.hpp>
#include <Functions/Meos/Th3indexCellToParentLogicalFunction.hpp>
#include <Functions/Meos/Th3indexCellToCenterChildLogicalFunction.hpp>
#include <Functions/Meos/Th3indexCellToChildPosLogicalFunction.hpp>
#include <Functions/Meos/Th3indexAreNeighborCellsLogicalFunction.hpp>
#include <Functions/Meos/Th3indexGridDistanceLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/AintersectsTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/EcoversTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/AcoversTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/EdisjointTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/AdisjointTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/EcontainsTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/AcontainsTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/NadTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/EdwithinTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/AdwithinTcbufferGeoLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AintersectsTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EcoversTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AcoversTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EdisjointTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AdisjointTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EcontainsTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AcontainsTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EverEqTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EverNeTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/NadTcbufferCbufferLogicalFunction.hpp>
#include <Functions/Meos/EverEqTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EverNeTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AintersectsTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EcoversTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AcoversTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AdisjointTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/NadTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/EdwithinTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/AdwithinTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/MindistanceTcbufferTcbufferLogicalFunction.hpp>
#include <Functions/Meos/NadTnpointGeoLogicalFunction.hpp>
#include <Functions/Meos/EverEqTnpointNpointLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTnpointNpointLogicalFunction.hpp>
#include <Functions/Meos/EverNeTnpointNpointLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTnpointNpointLogicalFunction.hpp>
#include <Functions/Meos/NadTnpointNpointLogicalFunction.hpp>
#include <Functions/Meos/EverEqNpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqNpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/EverNeNpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeNpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/EverEqTnpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTnpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/EverNeTnpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTnpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/NadTnpointTnpointLogicalFunction.hpp>
#include <Functions/Meos/NadTposeGeoLogicalFunction.hpp>
#include <Functions/Meos/EverEqTposePoseLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTposePoseLogicalFunction.hpp>
#include <Functions/Meos/EverNeTposePoseLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTposePoseLogicalFunction.hpp>
#include <Functions/Meos/NadTposePoseLogicalFunction.hpp>
#include <Functions/Meos/EverEqPoseTposeLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqPoseTposeLogicalFunction.hpp>
#include <Functions/Meos/EverNePoseTposeLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNePoseTposeLogicalFunction.hpp>
#include <Functions/Meos/EverEqTposeTposeLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTposeTposeLogicalFunction.hpp>
#include <Functions/Meos/EverNeTposeTposeLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTposeTposeLogicalFunction.hpp>
#include <Functions/Meos/NadTposeTposeLogicalFunction.hpp>
#include <Functions/Meos/EverEqTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EverNeTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/NadTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EverEqGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/EverNeGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/EverEqTrgeometryTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTrgeometryTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/EverNeTrgeometryTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTrgeometryTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/NadTrgeometryTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTpcpointGeoLogicalFunction.hpp>
#include <Functions/Meos/NadTpcpointGeoLogicalFunction.hpp>
#include <Functions/Meos/QuadbinPointToCellLogicalFunction.hpp>
#include <Functions/Meos/QuadbinIsValidCellLogicalFunction.hpp>
#include <Functions/Meos/QuadbinGetResolutionLogicalFunction.hpp>
#include <Functions/Meos/QuadbinCellAreaLogicalFunction.hpp>
#include <Functions/Meos/QuadbinCellToQuadkeyLogicalFunction.hpp>
#include <Functions/Meos/QuadbinCellToParentLogicalFunction.hpp>
#include <Functions/Meos/QuadbinTileToCellLogicalFunction.hpp>
#include <Functions/Meos/EverEqTjsonbJsonbLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTjsonbJsonbLogicalFunction.hpp>
#include <Functions/Meos/EverNeTjsonbJsonbLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTjsonbJsonbLogicalFunction.hpp>
#include <Functions/Meos/EverEqTjsonbTjsonbLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTjsonbTjsonbLogicalFunction.hpp>
#include <Functions/Meos/EverNeTjsonbTjsonbLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTjsonbTjsonbLogicalFunction.hpp>
#include <Functions/Meos/GeomBoundaryLogicalFunction.hpp>
#include <Functions/Meos/GeomCentroidLogicalFunction.hpp>
#include <Functions/Meos/GeomConvexHullLogicalFunction.hpp>
#include <Functions/Meos/GeoReverseLogicalFunction.hpp>
#include <Functions/Meos/GeoPointsLogicalFunction.hpp>
#include <Functions/Meos/GeomUnaryUnionLogicalFunction.hpp>
#include <Functions/Meos/GeomDifference2dLogicalFunction.hpp>
#include <Functions/Meos/GeomIntersection2dLogicalFunction.hpp>
#include <Functions/Meos/GeomShortestline2dLogicalFunction.hpp>
#include <Functions/Meos/GeomShortestline3dLogicalFunction.hpp>
#include <Functions/Meos/GeoSetSridLogicalFunction.hpp>
#include <Functions/Meos/GeoTransformLogicalFunction.hpp>
#include <Functions/Meos/GeoRoundLogicalFunction.hpp>
#include <Functions/Meos/GeomBufferLogicalFunction.hpp>
#include <Functions/Meos/LineNumpointsLogicalFunction.hpp>
#include <Functions/Meos/LineLocatePointLogicalFunction.hpp>
#include <Functions/Meos/LineInterpolatePointLogicalFunction.hpp>
#include <Functions/Meos/LineSubstringLogicalFunction.hpp>
#include <Functions/Meos/GeomPointMake2dLogicalFunction.hpp>
#include <Functions/Meos/GeomPointMake3dzLogicalFunction.hpp>
#include <Functions/Meos/GeogPointMake2dLogicalFunction.hpp>
#include <Functions/Meos/GeogPointMake3dzLogicalFunction.hpp>
#include <Functions/Meos/GeomToGeogLogicalFunction.hpp>
#include <Functions/Meos/GeogToGeomLogicalFunction.hpp>
#include <Functions/Meos/GeogCentroidLogicalFunction.hpp>
#include <Functions/Meos/GeoGeoNLogicalFunction.hpp>
#include <Functions/Meos/LinePointNLogicalFunction.hpp>
#include <Functions/Meos/GeomIntersection2dCollLogicalFunction.hpp>
#include <Functions/Meos/GeoAsGeojsonLogicalFunction.hpp>
#include <Functions/Meos/GeoAsHexewkbLogicalFunction.hpp>
#include <Functions/Meos/GeoAsEwktLogicalFunction.hpp>
#include <Functions/Meos/GeoFromGeojsonLogicalFunction.hpp>
#include <Functions/Meos/GeomFromHexewkbLogicalFunction.hpp>
#include <Functions/Meos/GeoTransformPipelineLogicalFunction.hpp>
#include <Functions/Meos/GeomMinBoundingCenterLogicalFunction.hpp>
#include <Functions/Meos/GeomMinBoundingRadiusLogicalFunction.hpp>
#include <Functions/Meos/EverEqTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EverNeTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EcoversTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EdisjointTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EcontainsTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AintersectsTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AcoversTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AdisjointTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AcontainsTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/EdwithinTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/AdwithinTgeoTgeoLogicalFunction.hpp>
#include <Functions/Meos/H3indexEqLogicalFunction.hpp>
#include <Functions/Meos/H3indexNeLogicalFunction.hpp>
#include <Functions/Meos/H3indexLtLogicalFunction.hpp>
#include <Functions/Meos/H3indexLeLogicalFunction.hpp>
#include <Functions/Meos/H3indexGtLogicalFunction.hpp>
#include <Functions/Meos/H3indexGeLogicalFunction.hpp>
#include <Functions/Meos/H3indexCmpLogicalFunction.hpp>
#include <Functions/Meos/H3indexOutLogicalFunction.hpp>
#include <Functions/Meos/H3indexInLogicalFunction.hpp>
#include <Functions/Meos/JsonArrayLengthLogicalFunction.hpp>
#include <Functions/Meos/JsonTypeofLogicalFunction.hpp>
#include <Functions/Meos/JsonObjectFieldTextLogicalFunction.hpp>
#include <Functions/Meos/JsonArrayElementTextLogicalFunction.hpp>
#include <Functions/Meos/JsonbArrayLengthLogicalFunction.hpp>
#include <Functions/Meos/JsonbEqLogicalFunction.hpp>
#include <Functions/Meos/JsonbNeLogicalFunction.hpp>
#include <Functions/Meos/JsonbLtLogicalFunction.hpp>
#include <Functions/Meos/JsonbLeLogicalFunction.hpp>
#include <Functions/Meos/JsonbGtLogicalFunction.hpp>
#include <Functions/Meos/JsonbGeLogicalFunction.hpp>
#include <Functions/Meos/JsonbCmpLogicalFunction.hpp>
#include <Functions/Meos/JsonbContainsLogicalFunction.hpp>
#include <Functions/Meos/JsonbContainedLogicalFunction.hpp>
#include <Functions/Meos/JsonbExistsLogicalFunction.hpp>
#include <Functions/Meos/JsonbToCstringLogicalFunction.hpp>
#include <Functions/Meos/JsonbPrettyLogicalFunction.hpp>
#include <Functions/Meos/JsonbObjectFieldTextLogicalFunction.hpp>
#include <Functions/Meos/JsonbArrayElementTextLogicalFunction.hpp>
#include <Functions/Meos/IntspanMakeLogicalFunction.hpp>
#include <Functions/Meos/FloatspanMakeLogicalFunction.hpp>
#include <Functions/Meos/IntspanLowerLogicalFunction.hpp>
#include <Functions/Meos/IntspanUpperLogicalFunction.hpp>
#include <Functions/Meos/IntspanWidthLogicalFunction.hpp>
#include <Functions/Meos/IntspanLowerIncLogicalFunction.hpp>
#include <Functions/Meos/IntspanUpperIncLogicalFunction.hpp>
#include <Functions/Meos/FloatspanLowerLogicalFunction.hpp>
#include <Functions/Meos/FloatspanUpperLogicalFunction.hpp>
#include <Functions/Meos/FloatspanWidthLogicalFunction.hpp>
#include <Functions/Meos/FloatspanLowerIncLogicalFunction.hpp>
#include <Functions/Meos/FloatspanUpperIncLogicalFunction.hpp>
#include <Functions/Meos/ContainedIntSpanLogicalFunction.hpp>
#include <Functions/Meos/ContainedFloatSpanLogicalFunction.hpp>
#include <Functions/Meos/ContainsSpanIntLogicalFunction.hpp>
#include <Functions/Meos/ContainsSpanFloatLogicalFunction.hpp>
#include <Functions/Meos/ContainedSpanSpanLogicalFunction.hpp>
#include <Functions/Meos/ContainsSpanSpanLogicalFunction.hpp>
#include <Functions/Meos/ContainedFloatspanSpanLogicalFunction.hpp>
#include <Functions/Meos/ContainsFloatspanSpanLogicalFunction.hpp>
#include <Functions/Meos/TextUpperLogicalFunction.hpp>
#include <Functions/Meos/TextLowerLogicalFunction.hpp>
#include <Functions/Meos/TextInitcapLogicalFunction.hpp>
#include <Functions/Meos/QuadbinEqLogicalFunction.hpp>
#include <Functions/Meos/QuadbinNeLogicalFunction.hpp>
#include <Functions/Meos/QuadbinLtLogicalFunction.hpp>
#include <Functions/Meos/QuadbinLeLogicalFunction.hpp>
#include <Functions/Meos/QuadbinGtLogicalFunction.hpp>
#include <Functions/Meos/QuadbinGeLogicalFunction.hpp>
#include <Functions/Meos/QuadbinCmpLogicalFunction.hpp>
#include <Functions/Meos/EverEqQuadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/EverNeQuadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqQuadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeQuadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/EverEqTquadbinQuadbinLogicalFunction.hpp>
#include <Functions/Meos/EverNeTquadbinQuadbinLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTquadbinQuadbinLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTquadbinQuadbinLogicalFunction.hpp>
#include <Functions/Meos/EverEqTquadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/EverNeTquadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/AlwaysEqTquadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/AlwaysNeTquadbinTquadbinLogicalFunction.hpp>
#include <Functions/Meos/GeomRelatePatternLogicalFunction.hpp>
#include <Functions/Meos/GeomIntersects2dLogicalFunction.hpp>
#include <Functions/Meos/GeomDwithin2dLogicalFunction.hpp>
#include <Functions/Meos/GeomContainsLogicalFunction.hpp>
#include <Functions/Meos/GeomDisjoint2dLogicalFunction.hpp>
#include <Functions/Meos/GeomCoversLogicalFunction.hpp>
#include <Functions/Meos/GeomTouchesLogicalFunction.hpp>
#include <Functions/Meos/GeomIntersects3dLogicalFunction.hpp>
#include <Functions/Meos/GeomDwithin3dLogicalFunction.hpp>
#include <Functions/Meos/GeomDistance2dLogicalFunction.hpp>
#include <Functions/Meos/GeomDistance3dLogicalFunction.hpp>
#include <Functions/Meos/EintersectsTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AintersectsTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EdisjointTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AdisjointTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EtouchesTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AtouchesTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EcoversTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AcoversTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EdwithinTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/AdwithinTrgeometryGeoLogicalFunction.hpp>
#include <Functions/Meos/EcontainsGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/AcontainsGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/EcoversGeoTrgeometryLogicalFunction.hpp>
#include <Functions/Meos/AcoversGeoTrgeometryLogicalFunction.hpp>
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
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_COS */
        case AntlrSQLLexer::TFLOAT_COS:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_COS requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatCosLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_COS */
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_SIN */
        case AntlrSQLLexer::TFLOAT_SIN:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_SIN requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatSinLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_SIN */
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_TAN */
        case AntlrSQLLexer::TFLOAT_TAN:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_TAN requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatTanLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_TAN */
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_TO_TBIGINT */
        case AntlrSQLLexer::TFLOAT_TO_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_TO_TBIGINT requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatToTbigintLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_TO_TBIGINT */
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
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_DEGREES */
        case AntlrSQLLexer::TFLOAT_DEGREES:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TFLOAT_DEGREES requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatDegreesLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_DEGREES */
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
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_SCALE_VALUE */
        case AntlrSQLLexer::TFLOAT_SCALE_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TFLOAT_SCALE_VALUE requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatScaleValueLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_SCALE_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_SHIFT_VALUE */
        case AntlrSQLLexer::TFLOAT_SHIFT_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TFLOAT_SHIFT_VALUE requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatShiftValueLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_SHIFT_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TFLOAT_SHIFT_SCALE_VALUE */
        case AntlrSQLLexer::TFLOAT_SHIFT_SCALE_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TFLOAT_SHIFT_SCALE_VALUE requires exactly 4 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TfloatShiftScaleValueLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: TFLOAT_SHIFT_SCALE_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TNUMBER_ABS */
        case AntlrSQLLexer::TNUMBER_ABS:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TNUMBER_ABS requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TnumberAbsLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TNUMBER_ABS */
        /* BEGIN CODEGEN PARSER GLUE: ADD_FLOAT_TFLOAT */
        case AntlrSQLLexer::ADD_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_FLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddFloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ADD_TFLOAT_FLOAT */
        case AntlrSQLLexer::ADD_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: DIV_FLOAT_TFLOAT */
        case AntlrSQLLexer::DIV_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_FLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivFloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: DIV_TFLOAT_FLOAT */
        case AntlrSQLLexer::DIV_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: MUL_FLOAT_TFLOAT */
        case AntlrSQLLexer::MUL_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_FLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulFloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: MUL_TFLOAT_FLOAT */
        case AntlrSQLLexer::MUL_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: SUB_FLOAT_TFLOAT */
        case AntlrSQLLexer::SUB_FLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_FLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubFloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: SUB_TFLOAT_FLOAT */
        case AntlrSQLLexer::SUB_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ADD_BIGINT_TBIGINT */
        case AntlrSQLLexer::ADD_BIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_BIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddBigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ADD_TBIGINT_BIGINT */
        case AntlrSQLLexer::ADD_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ADD_INT_TINT */
        case AntlrSQLLexer::ADD_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_INT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddIntTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_INT_TINT */

        case AntlrSQLLexer::ADD_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: DIV_BIGINT_TBIGINT */
        case AntlrSQLLexer::DIV_BIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_BIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivBigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: DIV_TBIGINT_BIGINT */
        case AntlrSQLLexer::DIV_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: DIV_INT_TINT */
        case AntlrSQLLexer::DIV_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_INT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivIntTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_INT_TINT */

        case AntlrSQLLexer::DIV_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: MUL_BIGINT_TBIGINT */
        case AntlrSQLLexer::MUL_BIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_BIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulBigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: MUL_TBIGINT_BIGINT */
        case AntlrSQLLexer::MUL_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: MUL_INT_TINT */
        case AntlrSQLLexer::MUL_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_INT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulIntTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_INT_TINT */

        case AntlrSQLLexer::MUL_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: SUB_BIGINT_TBIGINT */
        case AntlrSQLLexer::SUB_BIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_BIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubBigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: SUB_TBIGINT_BIGINT */
        case AntlrSQLLexer::SUB_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: SUB_INT_TINT */
        case AntlrSQLLexer::SUB_INT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_INT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubIntTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_INT_TINT */

        case AntlrSQLLexer::SUB_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_TINT_INT */

        /* BEGIN CODEGEN PARSER GLUE: ADD_TNUMBER_TNUMBER */
        case AntlrSQLLexer::ADD_TNUMBER_TNUMBER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ADD_TNUMBER_TNUMBER requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AddTnumberTnumberLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ADD_TNUMBER_TNUMBER */
        /* BEGIN CODEGEN PARSER GLUE: DIV_TNUMBER_TNUMBER */
        case AntlrSQLLexer::DIV_TNUMBER_TNUMBER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("DIV_TNUMBER_TNUMBER requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(DivTnumberTnumberLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: DIV_TNUMBER_TNUMBER */
        /* BEGIN CODEGEN PARSER GLUE: MUL_TNUMBER_TNUMBER */
        case AntlrSQLLexer::MUL_TNUMBER_TNUMBER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("MUL_TNUMBER_TNUMBER requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(MulTnumberTnumberLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: MUL_TNUMBER_TNUMBER */
        /* BEGIN CODEGEN PARSER GLUE: SUB_TNUMBER_TNUMBER */
        case AntlrSQLLexer::SUB_TNUMBER_TNUMBER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("SUB_TNUMBER_TNUMBER requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(SubTnumberTnumberLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: SUB_TNUMBER_TNUMBER */
        /* BEGIN CODEGEN PARSER GLUE: TBIGINT_SCALE_VALUE */
        case AntlrSQLLexer::TBIGINT_SCALE_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TBIGINT_SCALE_VALUE requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TbigintScaleValueLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBIGINT_SCALE_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TBIGINT_SHIFT_SCALE_VALUE */
        case AntlrSQLLexer::TBIGINT_SHIFT_SCALE_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TBIGINT_SHIFT_SCALE_VALUE requires exactly 4 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TbigintShiftScaleValueLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBIGINT_SHIFT_SCALE_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TBIGINT_SHIFT_VALUE */
        case AntlrSQLLexer::TBIGINT_SHIFT_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TBIGINT_SHIFT_VALUE requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TbigintShiftValueLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBIGINT_SHIFT_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TBIGINT_TO_TFLOAT */
        case AntlrSQLLexer::TBIGINT_TO_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TBIGINT_TO_TFLOAT requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TbigintToTfloatLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBIGINT_TO_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TBIGINT_TO_TINT */
        case AntlrSQLLexer::TBIGINT_TO_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TBIGINT_TO_TINT requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TbigintToTintLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TBIGINT_TO_TINT */

        case AntlrSQLLexer::TDISTANCE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TDISTANCE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TdistanceTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TDISTANCE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TDISTANCE_TINT_INT */
        case AntlrSQLLexer::TDISTANCE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TDISTANCE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TdistanceTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TDISTANCE_TINT_INT */

        case AntlrSQLLexer::TDISTANCE_TNUMBER_TNUMBER:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TDISTANCE_TNUMBER_TNUMBER requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TdistanceTnumberTnumberLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TDISTANCE_TNUMBER_TNUMBER */

        case AntlrSQLLexer::TEMPORAL_ROUND:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEMPORAL_ROUND requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TemporalRoundLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEMPORAL_ROUND */
        /* BEGIN CODEGEN PARSER GLUE: TINT_SCALE_VALUE */
        case AntlrSQLLexer::TINT_SCALE_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TINT_SCALE_VALUE requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TintScaleValueLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TINT_SCALE_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TINT_SHIFT_SCALE_VALUE */
        case AntlrSQLLexer::TINT_SHIFT_SCALE_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 4)
                throw InvalidQuerySyntax("TINT_SHIFT_SCALE_VALUE requires exactly 4 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TintShiftScaleValueLogicalFunction(a0, a1, a2, a3));
        }
        break;
        /* END CODEGEN PARSER GLUE: TINT_SHIFT_SCALE_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TINT_SHIFT_VALUE */
        case AntlrSQLLexer::TINT_SHIFT_VALUE:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TINT_SHIFT_VALUE requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TintShiftValueLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TINT_SHIFT_VALUE */
        /* BEGIN CODEGEN PARSER GLUE: TINT_TO_TBIGINT */
        case AntlrSQLLexer::TINT_TO_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TINT_TO_TBIGINT requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TintToTbigintLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TINT_TO_TBIGINT */
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
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_BIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_EQ_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_BIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_GE_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGeBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_BIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_GT_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGtBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_BIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_LE_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLeBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_BIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_LT_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLtBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_BIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_NE_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TBIGINT_BIGINT */
        case AntlrSQLLexer::EVER_EQ_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TBIGINT_BIGINT */
        case AntlrSQLLexer::EVER_GE_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TBIGINT_BIGINT */
        case AntlrSQLLexer::EVER_GT_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TBIGINT_BIGINT */
        case AntlrSQLLexer::EVER_LE_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TBIGINT_BIGINT */
        case AntlrSQLLexer::EVER_LT_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TBIGINT_BIGINT */
        case AntlrSQLLexer::EVER_NE_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TBIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_EQ_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TBIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_GE_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TBIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_GT_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TBIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_LE_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TBIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_LT_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TBIGINT_TBIGINT */
        case AntlrSQLLexer::EVER_NE_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_BOOL_TBOOL */
        case AntlrSQLLexer::EVER_EQ_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_BOOL_TBOOL */
        case AntlrSQLLexer::EVER_NE_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_EQ_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_GE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_GT_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGtFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_LE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_LT_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLtFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_FLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_NE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TBOOL_BOOL */
        case AntlrSQLLexer::EVER_EQ_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TBOOL_BOOL */
        case AntlrSQLLexer::EVER_NE_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_EQ_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_GE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_GT_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGtTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_LE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_LT_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLtTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::EVER_NE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_EQ_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_GE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_GT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_LE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_LT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TFLOAT_FLOAT */
        case AntlrSQLLexer::EVER_NE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TFLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_EQ_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TFLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_GE_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TFLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_GT_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TFLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_LE_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TFLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_LT_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TFLOAT_TFLOAT */
        case AntlrSQLLexer::EVER_NE_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_INT_TINT */
        case AntlrSQLLexer::EVER_EQ_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverEqIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_INT_TINT */
        case AntlrSQLLexer::EVER_GE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_INT_TINT */
        case AntlrSQLLexer::EVER_GT_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverGtIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_INT_TINT */
        case AntlrSQLLexer::EVER_LE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_INT_TINT */
        case AntlrSQLLexer::EVER_LT_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverLtIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_INT_TINT */
        case AntlrSQLLexer::EVER_NE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(EverNeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TINT_INT */
        case AntlrSQLLexer::EVER_EQ_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TINT_INT */
        case AntlrSQLLexer::EVER_GE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TINT_INT */
        case AntlrSQLLexer::EVER_GT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TINT_INT */
        case AntlrSQLLexer::EVER_LE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TINT_INT */
        case AntlrSQLLexer::EVER_LT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TINT_INT */
        case AntlrSQLLexer::EVER_NE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TINT_TINT */
        case AntlrSQLLexer::EVER_EQ_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TINT_TINT */
        case AntlrSQLLexer::EVER_GE_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TINT_TINT */
        case AntlrSQLLexer::EVER_GT_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TINT_TINT */
        case AntlrSQLLexer::EVER_LE_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TINT_TINT */
        case AntlrSQLLexer::EVER_LT_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TINT_TINT */
        case AntlrSQLLexer::EVER_NE_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_INT_TINT */
        case AntlrSQLLexer::ALWAYS_EQ_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_INT_TINT */
        case AntlrSQLLexer::ALWAYS_GE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_INT_TINT */
        case AntlrSQLLexer::ALWAYS_GT_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGtIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_INT_TINT */
        case AntlrSQLLexer::ALWAYS_LE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_INT_TINT */
        case AntlrSQLLexer::ALWAYS_LT_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLtIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_INT_TINT */
        case AntlrSQLLexer::ALWAYS_NE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TINT_INT */
        case AntlrSQLLexer::ALWAYS_EQ_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TINT_INT */
        case AntlrSQLLexer::ALWAYS_GE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TINT_INT */
        case AntlrSQLLexer::ALWAYS_GT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TINT_INT */
        case AntlrSQLLexer::ALWAYS_LE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TINT_INT */
        case AntlrSQLLexer::ALWAYS_LT_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TINT_INT */
        case AntlrSQLLexer::ALWAYS_NE_TINT_INT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TINT_INT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTintIntLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TINT_TINT */
        case AntlrSQLLexer::ALWAYS_EQ_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TINT_TINT */
        case AntlrSQLLexer::ALWAYS_GE_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TINT_TINT */
        case AntlrSQLLexer::ALWAYS_GT_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TINT_TINT */
        case AntlrSQLLexer::ALWAYS_LE_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TINT_TINT */
        case AntlrSQLLexer::ALWAYS_LT_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TINT_TINT */
        case AntlrSQLLexer::ALWAYS_NE_TINT_TINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TINT_TINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTintTintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TINT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_BIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_EQ_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_BIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_GE_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGeBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_BIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_GT_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGtBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_BIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_LE_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLeBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_BIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_LT_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLtBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_BIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_NE_BIGINT_TBIGINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_BIGINT_TBIGINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeBigintTbigintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_BIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TBIGINT_BIGINT */
        case AntlrSQLLexer::ALWAYS_EQ_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TBIGINT_BIGINT */
        case AntlrSQLLexer::ALWAYS_GE_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TBIGINT_BIGINT */
        case AntlrSQLLexer::ALWAYS_GT_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TBIGINT_BIGINT */
        case AntlrSQLLexer::ALWAYS_LE_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TBIGINT_BIGINT */
        case AntlrSQLLexer::ALWAYS_LT_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TBIGINT_BIGINT */
        case AntlrSQLLexer::ALWAYS_NE_TBIGINT_BIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TBIGINT_BIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTbigintBigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TBIGINT_BIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TBIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_EQ_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TBIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_GE_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TBIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_GT_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TBIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_LE_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TBIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_LT_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TBIGINT_TBIGINT */
        case AntlrSQLLexer::ALWAYS_NE_TBIGINT_TBIGINT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TBIGINT_TBIGINT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTbigintTbigintLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TBIGINT_TBIGINT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_BOOL_TBOOL */
        case AntlrSQLLexer::ALWAYS_EQ_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_BOOL_TBOOL */
        case AntlrSQLLexer::ALWAYS_NE_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_EQ_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_GE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_GT_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGtFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_LE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_LT_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLtFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_FLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_NE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TBOOL_BOOL */
        case AntlrSQLLexer::ALWAYS_EQ_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TBOOL_BOOL */
        case AntlrSQLLexer::ALWAYS_NE_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_EQ_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysEqTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_GE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_GT_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysGtTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_LE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_LT_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysLtTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::ALWAYS_NE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(AlwaysNeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: TNOT_TBOOL */
        case AntlrSQLLexer::TNOT_TBOOL:
        {
            const auto argCount = context->expression().size();
            if (argCount != 2)
                throw InvalidQuerySyntax("TNOT_TBOOL requires exactly 2 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TnotTboolLogicalFunction(a0, a1));
        }
        break;
        /* END CODEGEN PARSER GLUE: TNOT_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TAND_BOOL_TBOOL */
        case AntlrSQLLexer::TAND_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TAND_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TandBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TAND_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TAND_TBOOL_BOOL */
        case AntlrSQLLexer::TAND_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TAND_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TandTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TAND_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: TAND_TBOOL_TBOOL */
        case AntlrSQLLexer::TAND_TBOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TAND_TBOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TandTboolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TAND_TBOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TOR_BOOL_TBOOL */
        case AntlrSQLLexer::TOR_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TOR_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TorBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TOR_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TOR_TBOOL_BOOL */
        case AntlrSQLLexer::TOR_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TOR_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TorTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TOR_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: TOR_TBOOL_TBOOL */
        case AntlrSQLLexer::TOR_TBOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TOR_TBOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TorTboolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TOR_TBOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_BOOL_TBOOL */
        case AntlrSQLLexer::TEQ_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_FLOAT_TFLOAT */
        case AntlrSQLLexer::TEQ_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_INT_TINT */
        case AntlrSQLLexer::TEQ_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_TBOOL_BOOL */
        case AntlrSQLLexer::TEQ_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::TEQ_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_TFLOAT_FLOAT */
        case AntlrSQLLexer::TEQ_TFLOAT_FLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_TFLOAT_FLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqTfloatFloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_TINT_INT */
        case AntlrSQLLexer::TEQ_TINT_INT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_TINT_INT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TeqTintIntLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TEQ_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_TBOOL_BOOL */
        case AntlrSQLLexer::TNE_TBOOL_BOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_TBOOL_BOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneTboolBoolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_TBOOL_BOOL */
        /* BEGIN CODEGEN PARSER GLUE: TNE_BOOL_TBOOL */
        case AntlrSQLLexer::TNE_BOOL_TBOOL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_BOOL_TBOOL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneBoolTboolLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_BOOL_TBOOL */
        /* BEGIN CODEGEN PARSER GLUE: TNE_TFLOAT_FLOAT */
        case AntlrSQLLexer::TNE_TFLOAT_FLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_TFLOAT_FLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneTfloatFloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_FLOAT_TFLOAT */
        case AntlrSQLLexer::TNE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_TINT_INT */
        case AntlrSQLLexer::TNE_TINT_INT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_TINT_INT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneTintIntLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_INT_TINT */
        case AntlrSQLLexer::TNE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::TNE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);

            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}

            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();

            helpers.top().functionBuilder.emplace_back(TneTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TNE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: TGE_TFLOAT_FLOAT */
        case AntlrSQLLexer::TGE_TFLOAT_FLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_TFLOAT_FLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgeTfloatFloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TGE_FLOAT_TFLOAT */
        case AntlrSQLLexer::TGE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgeFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TGE_TINT_INT */
        case AntlrSQLLexer::TGE_TINT_INT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_TINT_INT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgeTintIntLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: TGE_INT_TINT */
        case AntlrSQLLexer::TGE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgeIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TGE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::TGE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgeTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: TGT_TFLOAT_FLOAT */
        case AntlrSQLLexer::TGT_TFLOAT_FLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_TFLOAT_FLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgtTfloatFloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGT_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TGT_FLOAT_TFLOAT */
        case AntlrSQLLexer::TGT_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgtFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGT_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TGT_TINT_INT */
        case AntlrSQLLexer::TGT_TINT_INT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_TINT_INT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgtTintIntLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGT_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: TGT_INT_TINT */
        case AntlrSQLLexer::TGT_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgtIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGT_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TGT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::TGT_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TgtTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TGT_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: TLE_TFLOAT_FLOAT */
        case AntlrSQLLexer::TLE_TFLOAT_FLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_TFLOAT_FLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TleTfloatFloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TLE_FLOAT_TFLOAT */
        case AntlrSQLLexer::TLE_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TleFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLE_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TLE_TINT_INT */
        case AntlrSQLLexer::TLE_TINT_INT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_TINT_INT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TleTintIntLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLE_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: TLE_INT_TINT */
        case AntlrSQLLexer::TLE_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TleIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLE_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TLE_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::TLE_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TleTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLE_TEMPORAL_TEMPORAL */
        /* BEGIN CODEGEN PARSER GLUE: TLT_TFLOAT_FLOAT */
        case AntlrSQLLexer::TLT_TFLOAT_FLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_TFLOAT_FLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TltTfloatFloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLT_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TLT_FLOAT_TFLOAT */
        case AntlrSQLLexer::TLT_FLOAT_TFLOAT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_FLOAT_TFLOAT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TltFloatTfloatLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLT_FLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: TLT_TINT_INT */
        case AntlrSQLLexer::TLT_TINT_INT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_TINT_INT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TltTintIntLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLT_TINT_INT */
        /* BEGIN CODEGEN PARSER GLUE: TLT_INT_TINT */
        case AntlrSQLLexer::TLT_INT_TINT:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_INT_TINT requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TltIntTintLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLT_INT_TINT */
        /* BEGIN CODEGEN PARSER GLUE: TLT_TEMPORAL_TEMPORAL */
        case AntlrSQLLexer::TLT_TEMPORAL_TEMPORAL:
        {{
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_TEMPORAL_TEMPORAL requires exactly 3 arguments, but got {{}}", argCount);
            while (!helpers.top().constantBuilder.empty())
            {{
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
            }}
            auto a2 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a1 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            auto a0 = helpers.top().functionBuilder.back(); helpers.top().functionBuilder.pop_back();
            helpers.top().functionBuilder.emplace_back(TltTemporalTemporalLogicalFunction(a0, a1, a2));
        }}
        break;
        /* END CODEGEN PARSER GLUE: TLT_TEMPORAL_TEMPORAL */
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
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TTEXT_TEXT */
        case AntlrSQLLexer::EVER_EQ_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TTEXT_TEXT */
        case AntlrSQLLexer::EVER_NE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TTEXT_TEXT */
        case AntlrSQLLexer::EVER_GE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TTEXT_TEXT */
        case AntlrSQLLexer::EVER_GT_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TTEXT_TEXT */
        case AntlrSQLLexer::EVER_LE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TTEXT_TEXT */
        case AntlrSQLLexer::EVER_LT_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_EQ_TEXT_TTEXT */
        case AntlrSQLLexer::EVER_EQ_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_EQ_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverEqTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_EQ_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_NE_TEXT_TTEXT */
        case AntlrSQLLexer::EVER_NE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_NE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverNeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_NE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GE_TEXT_TTEXT */
        case AntlrSQLLexer::EVER_GE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_GT_TEXT_TTEXT */
        case AntlrSQLLexer::EVER_GT_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_GT_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverGtTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_GT_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LE_TEXT_TTEXT */
        case AntlrSQLLexer::EVER_LE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: EVER_LT_TEXT_TTEXT */
        case AntlrSQLLexer::EVER_LT_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("EVER_LT_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(EverLtTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: EVER_LT_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TTEXT_TEXT */
        case AntlrSQLLexer::ALWAYS_EQ_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TTEXT_TEXT */
        case AntlrSQLLexer::ALWAYS_NE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TTEXT_TEXT */
        case AntlrSQLLexer::ALWAYS_GE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TTEXT_TEXT */
        case AntlrSQLLexer::ALWAYS_GT_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TTEXT_TEXT */
        case AntlrSQLLexer::ALWAYS_LE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TTEXT_TEXT */
        case AntlrSQLLexer::ALWAYS_LT_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TEXT_TTEXT */
        case AntlrSQLLexer::ALWAYS_EQ_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TEXT_TTEXT */
        case AntlrSQLLexer::ALWAYS_NE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TEXT_TTEXT */
        case AntlrSQLLexer::ALWAYS_GE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TEXT_TTEXT */
        case AntlrSQLLexer::ALWAYS_GT_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TEXT_TTEXT */
        case AntlrSQLLexer::ALWAYS_LE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TEXT_TTEXT */
        case AntlrSQLLexer::ALWAYS_LT_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_TTEXT_TEXT */
        case AntlrSQLLexer::TEQ_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TeqTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEQ_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_TTEXT_TEXT */
        case AntlrSQLLexer::TNE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TneTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TNE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: TEQ_TEXT_TTEXT */
        case AntlrSQLLexer::TEQ_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TEQ_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TeqTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TEQ_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: TNE_TEXT_TTEXT */
        case AntlrSQLLexer::TNE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TNE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TneTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TNE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: TGE_TTEXT_TEXT */
        case AntlrSQLLexer::TGE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TgeTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TGE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: TGE_TEXT_TTEXT */
        case AntlrSQLLexer::TGE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TgeTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TGE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: TGT_TTEXT_TEXT */
        case AntlrSQLLexer::TGT_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TgtTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TGT_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: TGT_TEXT_TTEXT */
        case AntlrSQLLexer::TGT_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TGT_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TgtTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TGT_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: TLE_TTEXT_TEXT */
        case AntlrSQLLexer::TLE_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TleTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TLE_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: TLE_TEXT_TTEXT */
        case AntlrSQLLexer::TLE_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLE_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TleTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TLE_TEXT_TTEXT */
        /* BEGIN CODEGEN PARSER GLUE: TLT_TTEXT_TEXT */
        case AntlrSQLLexer::TLT_TTEXT_TEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_TTEXT_TEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TltTtextTextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TLT_TTEXT_TEXT */
        /* BEGIN CODEGEN PARSER GLUE: TLT_TEXT_TTEXT */
        case AntlrSQLLexer::TLT_TEXT_TTEXT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("TLT_TEXT_TTEXT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(TltTextTtextLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: TLT_TEXT_TTEXT */
        case AntlrSQLParser::TTEXT_UPPER: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "TtextUpper requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(TtextUpperLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TTEXT_UPPER */
        case AntlrSQLParser::TTEXT_LOWER: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "TtextLower requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(TtextLowerLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TTEXT_LOWER */
        case AntlrSQLParser::TTEXT_INITCAP: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "TtextInitcap requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(TtextInitcapLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TTEXT_INITCAP */
        case AntlrSQLParser::TEXTCAT_TTEXT_TEXT: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "TextcatTtextText requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(TextcatTtextTextLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: TEXTCAT_TTEXT_TEXT */
        case AntlrSQLParser::TEXTCAT_TEXT_TTEXT: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "TextcatTextTtext requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(TextcatTextTtextLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: TEXTCAT_TEXT_TTEXT */
        case AntlrSQLParser::TEXTCAT_TTEXT_TTEXT: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "TextcatTtextTtext requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(TextcatTtextTtextLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: TEXTCAT_TTEXT_TTEXT */
        case AntlrSQLParser::GEOM_INTERSECTS2D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomIntersects2d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomIntersects2dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        case AntlrSQLParser::GEOM_LENGTH: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomLength requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomLengthLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_LENGTH */
        case AntlrSQLParser::GEOM_PERIMETER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomPerimeter requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomPerimeterLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_PERIMETER */
        case AntlrSQLParser::GEOM_AZIMUTH: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomAzimuth requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomAzimuthLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_AZIMUTH */
        case AntlrSQLParser::GEOG_AREA: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeogArea requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeogAreaLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_AREA */
        case AntlrSQLParser::GEOG_LENGTH: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeogLength requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeogLengthLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_LENGTH */
        case AntlrSQLParser::GEOG_PERIMETER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeogPerimeter requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeogPerimeterLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_PERIMETER */
        case AntlrSQLParser::GEOM_IS_EMPTY: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomIsEmpty requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomIsEmptyLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_IS_EMPTY */
        case AntlrSQLParser::AINTERSECTS_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AintersectsTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
                        auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AintersectsTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: AINTERSECTS_TGEO_GEO */
        case AntlrSQLParser::ACOVERS_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AcoversTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
                        auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AcoversTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_TGEO_GEO */
        case AntlrSQLParser::ADISJOINT_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AdisjointTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
                        auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AdisjointTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ADISJOINT_TGEO_GEO */
        case AntlrSQLParser::ADWITHIN_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AdwithinTgeoGeo requires 5 args but got {}",
                         ctx->functionParam().size());
                        auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AdwithinTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ADWITHIN_TGEO_GEO */
        case AntlrSQLParser::EINTERSECTS_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EintersectsTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1),
                                                        std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TGEO_GEO */
        case AntlrSQLParser::ETOUCHES_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EtouchesTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EtouchesTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1),
                                                        std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ETOUCHES_TGEO_GEO */
        case AntlrSQLParser::ECONTAINS_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EcontainsTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EcontainsTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1),
                                                        std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ECONTAINS_TGEO_GEO */
        case AntlrSQLParser::ACONTAINS_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AcontainsTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AcontainsTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1),
                                                        std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ACONTAINS_TGEO_GEO */
        case AntlrSQLParser::ATOUCHES_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AtouchesTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AtouchesTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1),
                                                        std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ATOUCHES_TGEO_GEO */
        case AntlrSQLParser::GEO_NUM_POINTS: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoNumPoints requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoNumPointsLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_NUM_POINTS */
        case AntlrSQLParser::GEO_NUM_GEOS: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoNumGeos requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoNumGeosLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_NUM_GEOS */
        case AntlrSQLParser::GEO_SRID: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoSrid requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoSridLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_SRID */
        case AntlrSQLParser::GEO_IS_UNITARY: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoIsUnitary requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoIsUnitaryLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_IS_UNITARY */
        case AntlrSQLParser::GEO_EQUALS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoEquals requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoEqualsLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_EQUALS */
        case AntlrSQLParser::GEO_SAME: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoSame requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoSameLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_SAME */
        case AntlrSQLParser::GEOG_DISTANCE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeogDistance requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeogDistanceLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_DISTANCE */
        case AntlrSQLParser::NAD_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "NadTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(NadTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TGEO_GEO */
        case AntlrSQLParser::EVER_EQ_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EverEqTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EverEqTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TGEO_GEO */
        case AntlrSQLParser::EVER_NE_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EverNeTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EverNeTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TGEO_GEO */
        case AntlrSQLParser::ALWAYS_EQ_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AlwaysEqTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TGEO_GEO */
        case AntlrSQLParser::ALWAYS_NE_TGEO_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AlwaysNeTgeoGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTgeoGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TGEO_GEO */
        case AntlrSQLParser::GEOG_INTERSECTS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeogIntersects requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeogIntersectsLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_INTERSECTS */
        case AntlrSQLParser::GEOG_DWITHIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeogDwithin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeogDwithinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_DWITHIN */
        case AntlrSQLParser::GEOM_INTERSECTS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomIntersects requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomIntersectsLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_INTERSECTS */
        case AntlrSQLParser::GEOM_DWITHIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeomDwithin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeomDwithinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DWITHIN */
        case AntlrSQLParser::ACOVERS_GEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AcoversGeoTgeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AcoversGeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_GEO_TGEO */
        case AntlrSQLParser::H3_GS_POINT_TO_CELL: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "H3GsPointToCell requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(H3GsPointToCellLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: H3_GS_POINT_TO_CELL */
        case AntlrSQLParser::EVER_EQ_TH3INDEX_H3INDEX: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverEqTh3indexH3index requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverEqTh3indexH3indexLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TH3INDEX_H3INDEX */
        case AntlrSQLParser::EVER_NE_TH3INDEX_H3INDEX: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverNeTh3indexH3index requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverNeTh3indexH3indexLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TH3INDEX_H3INDEX */
        case AntlrSQLParser::ALWAYS_EQ_TH3INDEX_H3INDEX: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysEqTh3indexH3index requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTh3indexH3indexLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TH3INDEX_H3INDEX */
        case AntlrSQLParser::ALWAYS_NE_TH3INDEX_H3INDEX: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysNeTh3indexH3index requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTh3indexH3indexLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TH3INDEX_H3INDEX */
        case AntlrSQLParser::TH3INDEX_GET_RESOLUTION: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "Th3indexGetResolution requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(Th3indexGetResolutionLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_GET_RESOLUTION */
        case AntlrSQLParser::TH3INDEX_GET_BASE_CELL_NUMBER: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "Th3indexGetBaseCellNumber requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(Th3indexGetBaseCellNumberLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_GET_BASE_CELL_NUMBER */
        case AntlrSQLParser::TH3INDEX_IS_VALID_CELL: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "Th3indexIsValidCell requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(Th3indexIsValidCellLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_IS_VALID_CELL */
        case AntlrSQLParser::TH3INDEX_IS_PENTAGON: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "Th3indexIsPentagon requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(Th3indexIsPentagonLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_IS_PENTAGON */
        case AntlrSQLParser::TH3INDEX_CELL_TO_PARENT_NEXT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "Th3indexCellToParentNext requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(Th3indexCellToParentNextLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_CELL_TO_PARENT_NEXT */
        case AntlrSQLParser::TH3INDEX_CELL_TO_CENTER_CHILD_NEXT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "Th3indexCellToCenterChildNext requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(Th3indexCellToCenterChildNextLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_CELL_TO_CENTER_CHILD_NEXT */
        case AntlrSQLParser::TH3INDEX_CELL_TO_PARENT: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "Th3indexCellToParent requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(Th3indexCellToParentLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_CELL_TO_PARENT */
        case AntlrSQLParser::TH3INDEX_CELL_TO_CENTER_CHILD: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "Th3indexCellToCenterChild requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(Th3indexCellToCenterChildLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_CELL_TO_CENTER_CHILD */
        case AntlrSQLParser::TH3INDEX_CELL_TO_CHILD_POS: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "Th3indexCellToChildPos requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(Th3indexCellToChildPosLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_CELL_TO_CHILD_POS */
        case AntlrSQLParser::TH3INDEX_ARE_NEIGHBOR_CELLS: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "Th3indexAreNeighborCells requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(Th3indexAreNeighborCellsLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_ARE_NEIGHBOR_CELLS */
        case AntlrSQLParser::TH3INDEX_GRID_DISTANCE: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "Th3indexGridDistance requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(Th3indexGridDistanceLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: TH3INDEX_GRID_DISTANCE */
        case AntlrSQLParser::EINTERSECTS_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EintersectsTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TCBUFFER_GEO */
        case AntlrSQLParser::AINTERSECTS_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AintersectsTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AintersectsTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: AINTERSECTS_TCBUFFER_GEO */
        case AntlrSQLParser::ECOVERS_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EcoversTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EcoversTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ECOVERS_TCBUFFER_GEO */
        case AntlrSQLParser::ACOVERS_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AcoversTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AcoversTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_TCBUFFER_GEO */
        case AntlrSQLParser::EDISJOINT_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EdisjointTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EdisjointTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: EDISJOINT_TCBUFFER_GEO */
        case AntlrSQLParser::ADISJOINT_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AdisjointTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AdisjointTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ADISJOINT_TCBUFFER_GEO */
        case AntlrSQLParser::ETOUCHES_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EtouchesTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EtouchesTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ETOUCHES_TCBUFFER_GEO */
        case AntlrSQLParser::ATOUCHES_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AtouchesTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AtouchesTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ATOUCHES_TCBUFFER_GEO */
        case AntlrSQLParser::ECONTAINS_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EcontainsTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EcontainsTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ECONTAINS_TCBUFFER_GEO */
        case AntlrSQLParser::ACONTAINS_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AcontainsTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AcontainsTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ACONTAINS_TCBUFFER_GEO */
        case AntlrSQLParser::NAD_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "NadTcbufferGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(NadTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TCBUFFER_GEO */
        case AntlrSQLParser::EDWITHIN_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EdwithinTcbufferGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EdwithinTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EDWITHIN_TCBUFFER_GEO */
        case AntlrSQLParser::ADWITHIN_TCBUFFER_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AdwithinTcbufferGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AdwithinTcbufferGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ADWITHIN_TCBUFFER_GEO */
        case AntlrSQLParser::EINTERSECTS_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EintersectsTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TCBUFFER_CBUFFER */
        case AntlrSQLParser::AINTERSECTS_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AintersectsTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AintersectsTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: AINTERSECTS_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ECOVERS_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EcoversTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EcoversTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ECOVERS_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ACOVERS_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AcoversTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AcoversTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_TCBUFFER_CBUFFER */
        case AntlrSQLParser::EDISJOINT_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EdisjointTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EdisjointTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EDISJOINT_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ADISJOINT_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AdisjointTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AdisjointTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ADISJOINT_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ETOUCHES_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EtouchesTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EtouchesTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ETOUCHES_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ATOUCHES_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AtouchesTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AtouchesTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ATOUCHES_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ECONTAINS_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EcontainsTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EcontainsTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ECONTAINS_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ACONTAINS_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AcontainsTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AcontainsTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ACONTAINS_TCBUFFER_CBUFFER */
        case AntlrSQLParser::EVER_EQ_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EverEqTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EverEqTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ALWAYS_EQ_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AlwaysEqTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TCBUFFER_CBUFFER */
        case AntlrSQLParser::EVER_NE_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EverNeTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EverNeTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TCBUFFER_CBUFFER */
        case AntlrSQLParser::ALWAYS_NE_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AlwaysNeTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TCBUFFER_CBUFFER */
        case AntlrSQLParser::NAD_TCBUFFER_CBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "NadTcbufferCbuffer requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(NadTcbufferCbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TCBUFFER_CBUFFER */
        case AntlrSQLParser::EVER_EQ_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EverEqTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EverEqTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ALWAYS_EQ_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AlwaysEqTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::EVER_NE_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EverNeTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EverNeTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ALWAYS_NE_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AlwaysNeTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::EINTERSECTS_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EintersectsTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::AINTERSECTS_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AintersectsTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AintersectsTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: AINTERSECTS_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ECOVERS_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EcoversTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EcoversTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ECOVERS_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ACOVERS_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AcoversTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AcoversTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ADISJOINT_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AdisjointTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AdisjointTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ADISJOINT_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ETOUCHES_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EtouchesTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EtouchesTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ETOUCHES_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ATOUCHES_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AtouchesTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AtouchesTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ATOUCHES_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::NAD_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "NadTcbufferTcbuffer requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(NadTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::EDWITHIN_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 9,
                         "EdwithinTcbufferTcbuffer requires 9 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            return LogicalFunction(EdwithinTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8)));
        }
        /* END CODEGEN PARSER GLUE: EDWITHIN_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::ADWITHIN_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 9,
                         "AdwithinTcbufferTcbuffer requires 9 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            return LogicalFunction(AdwithinTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8)));
        }
        /* END CODEGEN PARSER GLUE: ADWITHIN_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::MINDISTANCE_TCBUFFER_TCBUFFER: {
            PRECONDITION(ctx->functionParam().size() == 9,
                         "MindistanceTcbufferTcbuffer requires 9 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            return LogicalFunction(MindistanceTcbufferTcbufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8)));
        }
        /* END CODEGEN PARSER GLUE: MINDISTANCE_TCBUFFER_TCBUFFER */
        case AntlrSQLParser::NAD_TNPOINT_GEO: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "NadTnpointGeo requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(NadTnpointGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TNPOINT_GEO */
        case AntlrSQLParser::EVER_EQ_TNPOINT_NPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EverEqTnpointNpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EverEqTnpointNpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TNPOINT_NPOINT */
        case AntlrSQLParser::ALWAYS_EQ_TNPOINT_NPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AlwaysEqTnpointNpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTnpointNpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TNPOINT_NPOINT */
        case AntlrSQLParser::EVER_NE_TNPOINT_NPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EverNeTnpointNpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EverNeTnpointNpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TNPOINT_NPOINT */
        case AntlrSQLParser::ALWAYS_NE_TNPOINT_NPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AlwaysNeTnpointNpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTnpointNpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TNPOINT_NPOINT */
        case AntlrSQLParser::NAD_TNPOINT_NPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "NadTnpointNpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(NadTnpointNpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TNPOINT_NPOINT */
        case AntlrSQLParser::EVER_EQ_NPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EverEqNpointTnpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EverEqNpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_NPOINT_TNPOINT */
        case AntlrSQLParser::ALWAYS_EQ_NPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AlwaysEqNpointTnpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqNpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_NPOINT_TNPOINT */
        case AntlrSQLParser::EVER_NE_NPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "EverNeNpointTnpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(EverNeNpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_NPOINT_TNPOINT */
        case AntlrSQLParser::ALWAYS_NE_NPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "AlwaysNeNpointTnpoint requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeNpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_NPOINT_TNPOINT */
        case AntlrSQLParser::EVER_EQ_TNPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverEqTnpointTnpoint requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverEqTnpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TNPOINT_TNPOINT */
        case AntlrSQLParser::ALWAYS_EQ_TNPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysEqTnpointTnpoint requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTnpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TNPOINT_TNPOINT */
        case AntlrSQLParser::EVER_NE_TNPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverNeTnpointTnpoint requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverNeTnpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TNPOINT_TNPOINT */
        case AntlrSQLParser::ALWAYS_NE_TNPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysNeTnpointTnpoint requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTnpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TNPOINT_TNPOINT */
        case AntlrSQLParser::NAD_TNPOINT_TNPOINT: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "NadTnpointTnpoint requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(NadTnpointTnpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TNPOINT_TNPOINT */
        case AntlrSQLParser::NAD_TPOSE_GEO: {
            PRECONDITION(ctx->functionParam().size() == 5,
                         "NadTposeGeo requires 5 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            return LogicalFunction(NadTposeGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TPOSE_GEO */
        case AntlrSQLParser::EVER_EQ_TPOSE_POSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EverEqTposePose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EverEqTposePoseLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TPOSE_POSE */
        case AntlrSQLParser::ALWAYS_EQ_TPOSE_POSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AlwaysEqTposePose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTposePoseLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TPOSE_POSE */
        case AntlrSQLParser::EVER_NE_TPOSE_POSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EverNeTposePose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EverNeTposePoseLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TPOSE_POSE */
        case AntlrSQLParser::ALWAYS_NE_TPOSE_POSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AlwaysNeTposePose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTposePoseLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TPOSE_POSE */
        case AntlrSQLParser::NAD_TPOSE_POSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "NadTposePose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(NadTposePoseLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TPOSE_POSE */
        case AntlrSQLParser::EVER_EQ_POSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EverEqPoseTpose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EverEqPoseTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_POSE_TPOSE */
        case AntlrSQLParser::ALWAYS_EQ_POSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AlwaysEqPoseTpose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqPoseTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_POSE_TPOSE */
        case AntlrSQLParser::EVER_NE_POSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EverNePoseTpose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EverNePoseTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_POSE_TPOSE */
        case AntlrSQLParser::ALWAYS_NE_POSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AlwaysNePoseTpose requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNePoseTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_POSE_TPOSE */
        case AntlrSQLParser::EVER_EQ_TPOSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EverEqTposeTpose requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EverEqTposeTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TPOSE_TPOSE */
        case AntlrSQLParser::ALWAYS_EQ_TPOSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AlwaysEqTposeTpose requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTposeTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TPOSE_TPOSE */
        case AntlrSQLParser::EVER_NE_TPOSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "EverNeTposeTpose requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(EverNeTposeTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TPOSE_TPOSE */
        case AntlrSQLParser::ALWAYS_NE_TPOSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "AlwaysNeTposeTpose requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTposeTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TPOSE_TPOSE */
        case AntlrSQLParser::NAD_TPOSE_TPOSE: {
            PRECONDITION(ctx->functionParam().size() == 8,
                         "NadTposeTpose requires 8 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            return LogicalFunction(NadTposeTposeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TPOSE_TPOSE */
        case AntlrSQLParser::EVER_EQ_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverEqTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverEqTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TRGEOMETRY_GEO */
        case AntlrSQLParser::ALWAYS_EQ_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysEqTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TRGEOMETRY_GEO */
        case AntlrSQLParser::EVER_NE_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverNeTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverNeTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TRGEOMETRY_GEO */
        case AntlrSQLParser::ALWAYS_NE_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysNeTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TRGEOMETRY_GEO */
        case AntlrSQLParser::NAD_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "NadTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(NadTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TRGEOMETRY_GEO */
        case AntlrSQLParser::EVER_EQ_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverEqGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverEqGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_GEO_TRGEOMETRY */
        case AntlrSQLParser::ALWAYS_EQ_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysEqGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_GEO_TRGEOMETRY */
        case AntlrSQLParser::EVER_NE_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverNeGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverNeGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_GEO_TRGEOMETRY */
        case AntlrSQLParser::ALWAYS_NE_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysNeGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_GEO_TRGEOMETRY */
        case AntlrSQLParser::EVER_EQ_TRGEOMETRY_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 10,
                         "EverEqTrgeometryTrgeometry requires 10 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            auto arg9 = visit(ctx->functionParam(9)).as<LogicalFunction>();
            return LogicalFunction(EverEqTrgeometryTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8), std::move(arg9)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TRGEOMETRY_TRGEOMETRY */
        case AntlrSQLParser::ALWAYS_EQ_TRGEOMETRY_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 10,
                         "AlwaysEqTrgeometryTrgeometry requires 10 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            auto arg9 = visit(ctx->functionParam(9)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTrgeometryTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8), std::move(arg9)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TRGEOMETRY_TRGEOMETRY */
        case AntlrSQLParser::EVER_NE_TRGEOMETRY_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 10,
                         "EverNeTrgeometryTrgeometry requires 10 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            auto arg9 = visit(ctx->functionParam(9)).as<LogicalFunction>();
            return LogicalFunction(EverNeTrgeometryTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8), std::move(arg9)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TRGEOMETRY_TRGEOMETRY */
        case AntlrSQLParser::ALWAYS_NE_TRGEOMETRY_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 10,
                         "AlwaysNeTrgeometryTrgeometry requires 10 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            auto arg9 = visit(ctx->functionParam(9)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTrgeometryTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8), std::move(arg9)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TRGEOMETRY_TRGEOMETRY */
        case AntlrSQLParser::NAD_TRGEOMETRY_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 10,
                         "NadTrgeometryTrgeometry requires 10 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            auto arg7 = visit(ctx->functionParam(7)).as<LogicalFunction>();
            auto arg8 = visit(ctx->functionParam(8)).as<LogicalFunction>();
            auto arg9 = visit(ctx->functionParam(9)).as<LogicalFunction>();
            return LogicalFunction(NadTrgeometryTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8), std::move(arg9)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TRGEOMETRY_TRGEOMETRY */
        case AntlrSQLParser::EINTERSECTS_TPCPOINT_GEO: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EintersectsTpcpointGeo requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTpcpointGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TPCPOINT_GEO */
        case AntlrSQLParser::NAD_TPCPOINT_GEO: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "NadTpcpointGeo requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(NadTpcpointGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: NAD_TPCPOINT_GEO */
        case AntlrSQLParser::QUADBIN_POINT_TO_CELL: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "QuadbinPointToCell requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(QuadbinPointToCellLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_POINT_TO_CELL */
        case AntlrSQLParser::QUADBIN_IS_VALID_CELL: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "QuadbinIsValidCell requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(QuadbinIsValidCellLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_IS_VALID_CELL */
        case AntlrSQLParser::QUADBIN_GET_RESOLUTION: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "QuadbinGetResolution requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(QuadbinGetResolutionLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_GET_RESOLUTION */
        case AntlrSQLParser::QUADBIN_CELL_AREA: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "QuadbinCellArea requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(QuadbinCellAreaLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_CELL_AREA */
        case AntlrSQLParser::QUADBIN_CELL_TO_QUADKEY: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "QuadbinCellToQuadkey requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(QuadbinCellToQuadkeyLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_CELL_TO_QUADKEY */
        case AntlrSQLParser::QUADBIN_CELL_TO_PARENT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinCellToParent requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinCellToParentLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_CELL_TO_PARENT */
        case AntlrSQLParser::QUADBIN_TILE_TO_CELL: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "QuadbinTileToCell requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(QuadbinTileToCellLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_TILE_TO_CELL */
        case AntlrSQLParser::EVER_EQ_TJSONB_JSONB: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverEqTjsonbJsonb requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverEqTjsonbJsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TJSONB_JSONB */
        case AntlrSQLParser::ALWAYS_EQ_TJSONB_JSONB: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysEqTjsonbJsonb requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTjsonbJsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TJSONB_JSONB */
        case AntlrSQLParser::EVER_NE_TJSONB_JSONB: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverNeTjsonbJsonb requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverNeTjsonbJsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TJSONB_JSONB */
        case AntlrSQLParser::ALWAYS_NE_TJSONB_JSONB: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysNeTjsonbJsonb requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTjsonbJsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TJSONB_JSONB */
        case AntlrSQLParser::EVER_EQ_TJSONB_TJSONB: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EverEqTjsonbTjsonb requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EverEqTjsonbTjsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TJSONB_TJSONB */
        case AntlrSQLParser::ALWAYS_EQ_TJSONB_TJSONB: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AlwaysEqTjsonbTjsonb requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTjsonbTjsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TJSONB_TJSONB */
        case AntlrSQLParser::EVER_NE_TJSONB_TJSONB: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EverNeTjsonbTjsonb requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EverNeTjsonbTjsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TJSONB_TJSONB */
        case AntlrSQLParser::ALWAYS_NE_TJSONB_TJSONB: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AlwaysNeTjsonbTjsonb requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTjsonbTjsonbLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TJSONB_TJSONB */
        case AntlrSQLParser::GEOM_BOUNDARY: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomBoundary requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomBoundaryLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_BOUNDARY */
        case AntlrSQLParser::GEOM_CENTROID: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomCentroid requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomCentroidLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_CENTROID */
        case AntlrSQLParser::GEOM_CONVEX_HULL: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomConvexHull requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomConvexHullLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_CONVEX_HULL */
        case AntlrSQLParser::GEO_REVERSE: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoReverse requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoReverseLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_REVERSE */
        case AntlrSQLParser::GEO_POINTS: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoPoints requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoPointsLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_POINTS */
        case AntlrSQLParser::GEOM_UNARY_UNION: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomUnaryUnion requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomUnaryUnionLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_UNARY_UNION */
        case AntlrSQLParser::GEOM_DIFFERENCE2D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomDifference2d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomDifference2dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DIFFERENCE2D */
        case AntlrSQLParser::GEOM_INTERSECTION2D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomIntersection2d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomIntersection2dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_INTERSECTION2D */
        case AntlrSQLParser::GEOM_SHORTESTLINE2D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomShortestline2d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomShortestline2dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_SHORTESTLINE2D */
        case AntlrSQLParser::GEOM_SHORTESTLINE3D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomShortestline3d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomShortestline3dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_SHORTESTLINE3D */
        case AntlrSQLParser::GEO_SET_SRID: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoSetSrid requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoSetSridLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_SET_SRID */
        case AntlrSQLParser::GEO_TRANSFORM: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoTransform requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoTransformLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_TRANSFORM */
        case AntlrSQLParser::GEO_ROUND: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoRound requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoRoundLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_ROUND */
        case AntlrSQLParser::GEOM_BUFFER: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeomBuffer requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeomBufferLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_BUFFER */
        case AntlrSQLParser::LINE_NUMPOINTS: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "LineNumpoints requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(LineNumpointsLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: LINE_NUMPOINTS */
        case AntlrSQLParser::LINE_LOCATE_POINT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "LineLocatePoint requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(LineLocatePointLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: LINE_LOCATE_POINT */
        case AntlrSQLParser::LINE_INTERPOLATE_POINT: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "LineInterpolatePoint requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(LineInterpolatePointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: LINE_INTERPOLATE_POINT */
        case AntlrSQLParser::LINE_SUBSTRING: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "LineSubstring requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(LineSubstringLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: LINE_SUBSTRING */
        case AntlrSQLParser::GEOM_POINT_MAKE2D: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeomPointMake2d requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeomPointMake2dLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_POINT_MAKE2D */
        case AntlrSQLParser::GEOM_POINT_MAKE3DZ: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "GeomPointMake3dz requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(GeomPointMake3dzLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_POINT_MAKE3DZ */
        case AntlrSQLParser::GEOG_POINT_MAKE2D: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeogPointMake2d requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeogPointMake2dLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_POINT_MAKE2D */
        case AntlrSQLParser::GEOG_POINT_MAKE3DZ: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "GeogPointMake3dz requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(GeogPointMake3dzLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_POINT_MAKE3DZ */
        case AntlrSQLParser::GEOM_TO_GEOG: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomToGeog requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomToGeogLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_TO_GEOG */
        case AntlrSQLParser::GEOG_TO_GEOM: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeogToGeom requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeogToGeomLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_TO_GEOM */
        case AntlrSQLParser::GEOG_CENTROID: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeogCentroid requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeogCentroidLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOG_CENTROID */
        case AntlrSQLParser::GEO_GEO_N: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoGeoN requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoGeoNLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_GEO_N */
        case AntlrSQLParser::LINE_POINT_N: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "LinePointN requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(LinePointNLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: LINE_POINT_N */
        case AntlrSQLParser::GEOM_INTERSECTION2D_COLL: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomIntersection2dColl requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomIntersection2dCollLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_INTERSECTION2D_COLL */
        case AntlrSQLParser::GEO_AS_GEOJSON: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeoAsGeojson requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeoAsGeojsonLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEO_AS_GEOJSON */
        case AntlrSQLParser::GEO_AS_HEXEWKB: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoAsHexewkb requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoAsHexewkbLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_AS_HEXEWKB */
        case AntlrSQLParser::GEO_AS_EWKT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeoAsEwkt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeoAsEwktLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEO_AS_EWKT */
        case AntlrSQLParser::GEO_FROM_GEOJSON: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeoFromGeojson requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeoFromGeojsonLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEO_FROM_GEOJSON */
        case AntlrSQLParser::GEOM_FROM_HEXEWKB: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomFromHexewkb requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomFromHexewkbLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_FROM_HEXEWKB */
        case AntlrSQLParser::GEO_TRANSFORM_PIPELINE: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "GeoTransformPipeline requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(GeoTransformPipelineLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: GEO_TRANSFORM_PIPELINE */
        case AntlrSQLParser::GEOM_MIN_BOUNDING_CENTER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomMinBoundingCenter requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomMinBoundingCenterLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_MIN_BOUNDING_CENTER */
        case AntlrSQLParser::GEOM_MIN_BOUNDING_RADIUS: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "GeomMinBoundingRadius requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(GeomMinBoundingRadiusLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_MIN_BOUNDING_RADIUS */
        case AntlrSQLParser::GEOM_RELATE_PATTERN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeomRelatePattern requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeomRelatePatternLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        case AntlrSQLParser::EVER_EQ_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverEqTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverEqTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TGEO_TGEO */
        case AntlrSQLParser::EVER_NE_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EverNeTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EverNeTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TGEO_TGEO */
        case AntlrSQLParser::ALWAYS_EQ_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysEqTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TGEO_TGEO */
        case AntlrSQLParser::ALWAYS_NE_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AlwaysNeTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TGEO_TGEO */
        case AntlrSQLParser::EINTERSECTS_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EintersectsTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TGEO_TGEO */
        case AntlrSQLParser::ECOVERS_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EcoversTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EcoversTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ECOVERS_TGEO_TGEO */
        case AntlrSQLParser::EDISJOINT_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EdisjointTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EdisjointTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EDISJOINT_TGEO_TGEO */
        case AntlrSQLParser::ECONTAINS_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EcontainsTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EcontainsTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ECONTAINS_TGEO_TGEO */
        case AntlrSQLParser::ETOUCHES_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EtouchesTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EtouchesTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ETOUCHES_TGEO_TGEO */
        case AntlrSQLParser::AINTERSECTS_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AintersectsTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AintersectsTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: AINTERSECTS_TGEO_TGEO */
        case AntlrSQLParser::ACOVERS_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AcoversTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AcoversTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_TGEO_TGEO */
        case AntlrSQLParser::ADISJOINT_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AdisjointTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AdisjointTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ADISJOINT_TGEO_TGEO */
        case AntlrSQLParser::ACONTAINS_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AcontainsTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AcontainsTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ACONTAINS_TGEO_TGEO */
        case AntlrSQLParser::ATOUCHES_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AtouchesTgeoTgeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AtouchesTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ATOUCHES_TGEO_TGEO */
        case AntlrSQLParser::EDWITHIN_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EdwithinTgeoTgeo requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EdwithinTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EDWITHIN_TGEO_TGEO */
        case AntlrSQLParser::ADWITHIN_TGEO_TGEO: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AdwithinTgeoTgeo requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AdwithinTgeoTgeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ADWITHIN_TGEO_TGEO */
        case AntlrSQLParser::H3INDEX_EQ: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexEq requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexEqLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_EQ */
        case AntlrSQLParser::H3INDEX_NE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexNe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexNeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_NE */
        case AntlrSQLParser::H3INDEX_LT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexLt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexLtLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_LT */
        case AntlrSQLParser::H3INDEX_LE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexLe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexLeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_LE */
        case AntlrSQLParser::H3INDEX_GT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexGt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexGtLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_GT */
        case AntlrSQLParser::H3INDEX_GE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexGe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexGeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_GE */
        case AntlrSQLParser::H3INDEX_CMP: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "H3indexCmp requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(H3indexCmpLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_CMP */
        case AntlrSQLParser::H3INDEX_OUT: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "H3indexOut requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(H3indexOutLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_OUT */
        case AntlrSQLParser::H3INDEX_IN: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "H3indexIn requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(H3indexInLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: H3INDEX_IN */
        case AntlrSQLParser::JSON_ARRAY_LENGTH: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "JsonArrayLength requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(JsonArrayLengthLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: JSON_ARRAY_LENGTH */
        case AntlrSQLParser::JSON_TYPEOF: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "JsonTypeof requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(JsonTypeofLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: JSON_TYPEOF */
        case AntlrSQLParser::JSON_OBJECT_FIELD_TEXT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonObjectFieldText requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonObjectFieldTextLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSON_OBJECT_FIELD_TEXT */
        case AntlrSQLParser::JSON_ARRAY_ELEMENT_TEXT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonArrayElementText requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonArrayElementTextLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSON_ARRAY_ELEMENT_TEXT */
        case AntlrSQLParser::JSONB_ARRAY_LENGTH: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "JsonbArrayLength requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(JsonbArrayLengthLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_ARRAY_LENGTH */
        case AntlrSQLParser::JSONB_EQ: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbEq requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbEqLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_EQ */
        case AntlrSQLParser::JSONB_NE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbNe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbNeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_NE */
        case AntlrSQLParser::JSONB_LT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbLt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbLtLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_LT */
        case AntlrSQLParser::JSONB_LE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbLe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbLeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_LE */
        case AntlrSQLParser::JSONB_GT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbGt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbGtLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_GT */
        case AntlrSQLParser::JSONB_GE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbGe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbGeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_GE */
        case AntlrSQLParser::JSONB_CMP: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbCmp requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbCmpLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_CMP */
        case AntlrSQLParser::JSONB_CONTAINS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbContains requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbContainsLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_CONTAINS */
        case AntlrSQLParser::JSONB_CONTAINED: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbContained requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbContainedLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_CONTAINED */
        case AntlrSQLParser::JSONB_EXISTS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbExists requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbExistsLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_EXISTS */
        case AntlrSQLParser::JSONB_TO_CSTRING: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "JsonbToCstring requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(JsonbToCstringLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_TO_CSTRING */
        case AntlrSQLParser::JSONB_PRETTY: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "JsonbPretty requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(JsonbPrettyLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_PRETTY */
        case AntlrSQLParser::JSONB_OBJECT_FIELD_TEXT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbObjectFieldText requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbObjectFieldTextLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_OBJECT_FIELD_TEXT */
        case AntlrSQLParser::JSONB_ARRAY_ELEMENT_TEXT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "JsonbArrayElementText requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(JsonbArrayElementTextLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: JSONB_ARRAY_ELEMENT_TEXT */
        case AntlrSQLParser::INTSPAN_MAKE: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "IntspanMake requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(IntspanMakeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: INTSPAN_MAKE */
        case AntlrSQLParser::FLOATSPAN_MAKE: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "FloatspanMake requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(FloatspanMakeLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: FLOATSPAN_MAKE */
        case AntlrSQLParser::INTSPAN_LOWER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "IntspanLower requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(IntspanLowerLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: INTSPAN_LOWER */
        case AntlrSQLParser::INTSPAN_UPPER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "IntspanUpper requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(IntspanUpperLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: INTSPAN_UPPER */
        case AntlrSQLParser::INTSPAN_WIDTH: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "IntspanWidth requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(IntspanWidthLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: INTSPAN_WIDTH */
        case AntlrSQLParser::INTSPAN_LOWER_INC: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "IntspanLowerInc requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(IntspanLowerIncLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: INTSPAN_LOWER_INC */
        case AntlrSQLParser::INTSPAN_UPPER_INC: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "IntspanUpperInc requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(IntspanUpperIncLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: INTSPAN_UPPER_INC */
        case AntlrSQLParser::FLOATSPAN_LOWER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "FloatspanLower requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(FloatspanLowerLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: FLOATSPAN_LOWER */
        case AntlrSQLParser::FLOATSPAN_UPPER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "FloatspanUpper requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(FloatspanUpperLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: FLOATSPAN_UPPER */
        case AntlrSQLParser::FLOATSPAN_WIDTH: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "FloatspanWidth requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(FloatspanWidthLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: FLOATSPAN_WIDTH */
        case AntlrSQLParser::FLOATSPAN_LOWER_INC: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "FloatspanLowerInc requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(FloatspanLowerIncLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: FLOATSPAN_LOWER_INC */
        case AntlrSQLParser::FLOATSPAN_UPPER_INC: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "FloatspanUpperInc requires 1 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(FloatspanUpperIncLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: FLOATSPAN_UPPER_INC */
        case AntlrSQLParser::CONTAINED_INT_SPAN: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainedIntSpan requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainedIntSpanLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINED_INT_SPAN */
        case AntlrSQLParser::CONTAINED_FLOAT_SPAN: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainedFloatSpan requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainedFloatSpanLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINED_FLOAT_SPAN */
        case AntlrSQLParser::CONTAINS_SPAN_INT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainsSpanInt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainsSpanIntLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINS_SPAN_INT */
        case AntlrSQLParser::CONTAINS_SPAN_FLOAT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainsSpanFloat requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainsSpanFloatLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINS_SPAN_FLOAT */
        case AntlrSQLParser::CONTAINED_SPAN_SPAN: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainedSpanSpan requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainedSpanSpanLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINED_SPAN_SPAN */
        case AntlrSQLParser::CONTAINS_SPAN_SPAN: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainsSpanSpan requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainsSpanSpanLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINS_SPAN_SPAN */
        case AntlrSQLParser::CONTAINED_FLOATSPAN_SPAN: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainedFloatspanSpan requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainedFloatspanSpanLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINED_FLOATSPAN_SPAN */
        case AntlrSQLParser::CONTAINS_FLOATSPAN_SPAN: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "ContainsFloatspanSpan requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(ContainsFloatspanSpanLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: CONTAINS_FLOATSPAN_SPAN */
        case AntlrSQLParser::TEXT_UPPER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "TextUpper requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(TextUpperLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: TEXT_UPPER */
        case AntlrSQLParser::TEXT_LOWER: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "TextLower requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(TextLowerLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: TEXT_LOWER */
        case AntlrSQLParser::TEXT_INITCAP: {
            PRECONDITION(ctx->functionParam().size() == 1,
                         "TextInitcap requires 1 arg but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            return LogicalFunction(TextInitcapLogicalFunction(std::move(arg0)));
        }
        /* END CODEGEN PARSER GLUE: TEXT_INITCAP */
        case AntlrSQLParser::QUADBIN_EQ: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinEq requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinEqLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_EQ */
        case AntlrSQLParser::QUADBIN_NE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinNe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinNeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_NE */
        case AntlrSQLParser::QUADBIN_LT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinLt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinLtLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_LT */
        case AntlrSQLParser::QUADBIN_LE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinLe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinLeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_LE */
        case AntlrSQLParser::QUADBIN_GT: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinGt requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinGtLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_GT */
        case AntlrSQLParser::QUADBIN_GE: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinGe requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinGeLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_GE */
        case AntlrSQLParser::QUADBIN_CMP: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "QuadbinCmp requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(QuadbinCmpLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: QUADBIN_CMP */
        case AntlrSQLParser::EVER_EQ_QUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverEqQuadbinTquadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverEqQuadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_QUADBIN_TQUADBIN */
        case AntlrSQLParser::EVER_NE_QUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverNeQuadbinTquadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverNeQuadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_QUADBIN_TQUADBIN */
        case AntlrSQLParser::ALWAYS_EQ_QUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysEqQuadbinTquadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqQuadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_QUADBIN_TQUADBIN */
        case AntlrSQLParser::ALWAYS_NE_QUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysNeQuadbinTquadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeQuadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_QUADBIN_TQUADBIN */
        case AntlrSQLParser::EVER_EQ_TQUADBIN_QUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverEqTquadbinQuadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverEqTquadbinQuadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TQUADBIN_QUADBIN */
        case AntlrSQLParser::EVER_NE_TQUADBIN_QUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "EverNeTquadbinQuadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(EverNeTquadbinQuadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TQUADBIN_QUADBIN */
        case AntlrSQLParser::ALWAYS_EQ_TQUADBIN_QUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysEqTquadbinQuadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTquadbinQuadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TQUADBIN_QUADBIN */
        case AntlrSQLParser::ALWAYS_NE_TQUADBIN_QUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "AlwaysNeTquadbinQuadbin requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTquadbinQuadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TQUADBIN_QUADBIN */
        case AntlrSQLParser::EVER_EQ_TQUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EverEqTquadbinTquadbin requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EverEqTquadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EVER_EQ_TQUADBIN_TQUADBIN */
        case AntlrSQLParser::EVER_NE_TQUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "EverNeTquadbinTquadbin requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(EverNeTquadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: EVER_NE_TQUADBIN_TQUADBIN */
        case AntlrSQLParser::ALWAYS_EQ_TQUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AlwaysEqTquadbinTquadbin requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AlwaysEqTquadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TQUADBIN_TQUADBIN */
        case AntlrSQLParser::ALWAYS_NE_TQUADBIN_TQUADBIN: {
            PRECONDITION(ctx->functionParam().size() == 4,
                         "AlwaysNeTquadbinTquadbin requires 4 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            return LogicalFunction(AlwaysNeTquadbinTquadbinLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3)));
        }
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TQUADBIN_TQUADBIN */
        /* END CODEGEN PARSER GLUE: GEOM_RELATE_PATTERN */
        /* END CODEGEN PARSER GLUE: GEOM_INTERSECTS2D */
        case AntlrSQLParser::GEOM_DWITHIN2D: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeomDwithin2d requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeomDwithin2dLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DWITHIN2D */
        case AntlrSQLParser::GEOM_CONTAINS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomContains requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomContainsLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_CONTAINS */
        case AntlrSQLParser::GEOM_DISJOINT2D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomDisjoint2d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomDisjoint2dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DISJOINT2D */
        case AntlrSQLParser::GEOM_COVERS: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomCovers requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomCoversLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_COVERS */
        case AntlrSQLParser::GEOM_TOUCHES: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomTouches requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomTouchesLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_TOUCHES */
        case AntlrSQLParser::GEOM_INTERSECTS3D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomIntersects3d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomIntersects3dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_INTERSECTS3D */
        case AntlrSQLParser::GEOM_DWITHIN3D: {
            PRECONDITION(ctx->functionParam().size() == 3,
                         "GeomDwithin3d requires 3 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            return LogicalFunction(GeomDwithin3dLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DWITHIN3D */
        case AntlrSQLParser::GEOM_DISTANCE2D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomDistance2d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomDistance2dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DISTANCE2D */
        case AntlrSQLParser::GEOM_DISTANCE3D: {
            PRECONDITION(ctx->functionParam().size() == 2,
                         "GeomDistance3d requires 2 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            return LogicalFunction(GeomDistance3dLogicalFunction(std::move(arg0), std::move(arg1)));
        }
        /* END CODEGEN PARSER GLUE: GEOM_DISTANCE3D */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_EQ_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_GE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_GT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_LE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_LT_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TFLOAT_FLOAT */
        case AntlrSQLLexer::ALWAYS_NE_TFLOAT_FLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TFLOAT_FLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTfloatFloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TFLOAT_FLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_EQ_TFLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_EQ_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_EQ_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysEqTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_EQ_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GE_TFLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_GE_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GE_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGeTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GE_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_GT_TFLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_GT_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_GT_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysGtTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_GT_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LE_TFLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_LE_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LE_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLeTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LE_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_LT_TFLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_LT_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_LT_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysLtTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_LT_TFLOAT_TFLOAT */
        /* BEGIN CODEGEN PARSER GLUE: ALWAYS_NE_TFLOAT_TFLOAT */
        case AntlrSQLLexer::ALWAYS_NE_TFLOAT_TFLOAT:
        {
            const auto argCount = context->expression().size();
            if (argCount != 3)
                throw InvalidQuerySyntax("ALWAYS_NE_TFLOAT_TFLOAT requires exactly 3 arguments, but got {}", argCount);

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

            helpers.top().functionBuilder.emplace_back(AlwaysNeTfloatTfloatLogicalFunction(a0, a1, a2));
        }
        break;
        /* END CODEGEN PARSER GLUE: ALWAYS_NE_TFLOAT_TFLOAT */
        case AntlrSQLParser::EINTERSECTS_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EintersectsTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EintersectsTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EINTERSECTS_TRGEOMETRY_GEO */
        case AntlrSQLParser::AINTERSECTS_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AintersectsTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AintersectsTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: AINTERSECTS_TRGEOMETRY_GEO */
        case AntlrSQLParser::EDISJOINT_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EdisjointTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EdisjointTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: EDISJOINT_TRGEOMETRY_GEO */
        case AntlrSQLParser::ADISJOINT_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AdisjointTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AdisjointTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ADISJOINT_TRGEOMETRY_GEO */
        case AntlrSQLParser::ETOUCHES_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EtouchesTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EtouchesTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ETOUCHES_TRGEOMETRY_GEO */
        case AntlrSQLParser::ATOUCHES_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AtouchesTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AtouchesTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ATOUCHES_TRGEOMETRY_GEO */
        case AntlrSQLParser::ECOVERS_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EcoversTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EcoversTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ECOVERS_TRGEOMETRY_GEO */
        case AntlrSQLParser::ACOVERS_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AcoversTrgeometryGeo requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AcoversTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_TRGEOMETRY_GEO */
        case AntlrSQLParser::EDWITHIN_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "EdwithinTrgeometryGeo requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(EdwithinTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: EDWITHIN_TRGEOMETRY_GEO */
        case AntlrSQLParser::ADWITHIN_TRGEOMETRY_GEO: {
            PRECONDITION(ctx->functionParam().size() == 7,
                         "AdwithinTrgeometryGeo requires 7 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            auto arg6 = visit(ctx->functionParam(6)).as<LogicalFunction>();
            return LogicalFunction(AdwithinTrgeometryGeoLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6)));
        }
        /* END CODEGEN PARSER GLUE: ADWITHIN_TRGEOMETRY_GEO */
        case AntlrSQLParser::ECONTAINS_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EcontainsGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EcontainsGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ECONTAINS_GEO_TRGEOMETRY */
        case AntlrSQLParser::ACONTAINS_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AcontainsGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AcontainsGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ACONTAINS_GEO_TRGEOMETRY */
        case AntlrSQLParser::ECOVERS_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "EcoversGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(EcoversGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ECOVERS_GEO_TRGEOMETRY */
        case AntlrSQLParser::ACOVERS_GEO_TRGEOMETRY: {
            PRECONDITION(ctx->functionParam().size() == 6,
                         "AcoversGeoTrgeometry requires 6 args but got {}",
                         ctx->functionParam().size());
            auto arg0 = visit(ctx->functionParam(0)).as<LogicalFunction>();
            auto arg1 = visit(ctx->functionParam(1)).as<LogicalFunction>();
            auto arg2 = visit(ctx->functionParam(2)).as<LogicalFunction>();
            auto arg3 = visit(ctx->functionParam(3)).as<LogicalFunction>();
            auto arg4 = visit(ctx->functionParam(4)).as<LogicalFunction>();
            auto arg5 = visit(ctx->functionParam(5)).as<LogicalFunction>();
            return LogicalFunction(AcoversGeoTrgeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5)));
        }
        /* END CODEGEN PARSER GLUE: ACOVERS_GEO_TRGEOMETRY */

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
