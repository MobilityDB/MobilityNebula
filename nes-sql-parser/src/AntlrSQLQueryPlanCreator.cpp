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
