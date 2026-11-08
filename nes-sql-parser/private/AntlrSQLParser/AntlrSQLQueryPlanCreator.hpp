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
#pragma once

#include <memory>
#include <optional>
#include <stack>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <AntlrSQLBaseListener.h>
#include <AntlrSQLParser.h>
#include <AntlrSQLParser/AntlrSQLHelper.hpp>
#include <Plans/LogicalPlan.hpp>
#include <CommonParserFunctions.hpp>

namespace NES::Parsers
{

class AntlrSQLQueryPlanCreator final : public AntlrSQLBaseListener
{
    std::stack<AntlrSQLHelper> helpers;
    std::vector<std::variant<std::string, std::pair<std::string, ConfigMap>>> sinks;
    std::stack<LogicalPlan> queryPlans;

    /// The arguments of a function call in order: an expression as the function builder holds it, a literal
    /// as a constant of its type, a string VARSIZED and a number INT32, INT64 or FLOAT64 as it is written
    [[nodiscard]] std::vector<LogicalFunction> callArguments(AntlrSQLParser::FunctionCallContext* context) const;
    /// The call's arguments removed from the function and constant builders
    void takeCallArguments(AntlrSQLParser::FunctionCallContext* context);
    /// The aggregation registered under the name over the field of the call's first argument and the literals of
    /// the others, none for a call of another shape
    [[nodiscard]] std::optional<std::shared_ptr<WindowAggregationLogicalFunction>>
    aggregationOverField(AntlrSQLParser::FunctionCallContext* context, const std::string& name) const;

public:
    [[nodiscard]] LogicalPlan getQueryPlan() const;

    /// Parsing listener methods (enter/exit pairs)
    void enterPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context) override;
    void exitPrimaryQuery(AntlrSQLParser::PrimaryQueryContext* context) override;
    void enterSelectClause(AntlrSQLParser::SelectClauseContext* context) override;
    void exitSelectClause(AntlrSQLParser::SelectClauseContext* context) override;
    void enterFromClause(AntlrSQLParser::FromClauseContext* context) override;
    void exitFromClause(AntlrSQLParser::FromClauseContext* context) override;
    void enterWhereClause(AntlrSQLParser::WhereClauseContext* context) override;
    void exitWhereClause(AntlrSQLParser::WhereClauseContext* context) override;
    void enterComparisonOperator(AntlrSQLParser::ComparisonOperatorContext* context) override;
    void exitComparison(AntlrSQLParser::ComparisonContext* context) override;
    void enterFunctionCall(AntlrSQLParser::FunctionCallContext* context) override;
    void exitFunctionCall(AntlrSQLParser::FunctionCallContext* context) override;
    void enterHavingClause(AntlrSQLParser::HavingClauseContext* context) override;
    void exitHavingClause(AntlrSQLParser::HavingClauseContext* context) override;
    void enterJoinRelation(AntlrSQLParser::JoinRelationContext* context) override;
    void exitJoinRelation(AntlrSQLParser::JoinRelationContext* context) override;
    void enterWindowClause(AntlrSQLParser::WindowClauseContext* context) override;
    void exitWindowClause(AntlrSQLParser::WindowClauseContext* context) override;
    void enterGroupByClause(AntlrSQLParser::GroupByClauseContext* context) override;
    void exitGroupByClause(AntlrSQLParser::GroupByClauseContext* context) override;
    void enterJoinCriteria(AntlrSQLParser::JoinCriteriaContext* context) override;
    void enterJoinType(AntlrSQLParser::JoinTypeContext* context) override;
    void exitJoinType(AntlrSQLParser::JoinTypeContext* context) override;
    void enterSetOperation(AntlrSQLParser::SetOperationContext* context) override;
    void exitSetOperation(AntlrSQLParser::SetOperationContext* context) override;

    /// enter or exit functions (no pairs)
    void enterSinkClause(AntlrSQLParser::SinkClauseContext* context) override;
    void exitLogicalBinary(AntlrSQLParser::LogicalBinaryContext* context) override;
    void enterUnquotedIdentifier(AntlrSQLParser::UnquotedIdentifierContext* context) override;
    void enterIdentifier(AntlrSQLParser::IdentifierContext* context) override;
    void enterTimeUnit(AntlrSQLParser::TimeUnitContext* context) override;
    void exitSizeParameter(AntlrSQLParser::SizeParameterContext* context) override;
    void exitAdvancebyParameter(AntlrSQLParser::AdvancebyParameterContext* context) override;
    void exitGapParameter(AntlrSQLParser::GapParameterContext* context) override;
    void exitTimestampParameter(AntlrSQLParser::TimestampParameterContext* context) override;
    void exitTumblingWindow(AntlrSQLParser::TumblingWindowContext* context) override;
    void exitSlidingWindow(AntlrSQLParser::SlidingWindowContext* context) override;
    void exitWatermarkParameters(AntlrSQLParser::WatermarkParametersContext* context) override;
    void exitSessionWindow(AntlrSQLParser::SessionWindowContext* context) override;
    void exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context) override;
    void exitArithmeticUnary(AntlrSQLParser::ArithmeticUnaryContext* context) override;
    void exitArithmeticBinary(AntlrSQLParser::ArithmeticBinaryContext* context) override;
    void exitLogicalNot(AntlrSQLParser::LogicalNotContext* context) override;
    void exitConstantDefault(AntlrSQLParser::ConstantDefaultContext* context) override;
    void exitThresholdMinSizeParameter(AntlrSQLParser::ThresholdMinSizeParameterContext* context) override;
    void enterInlineSource(AntlrSQLParser::InlineSourceContext* context) override;
};

}
