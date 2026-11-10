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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <algorithm>
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
#include <Operators/Windows/Aggregations/AggregationLogicalFunctionProvider.hpp>
#include <Operators/Windows/Aggregations/ArrayAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/CountDistinctAggregationLogicalFunction.hpp>
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
#include <Operators/Windows/Aggregations/Meos/TemporalLengthAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/PairMeetingAggregationLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/Meos/CrossDistanceAggregationLogicalFunction.hpp>
#include <Functions/Meos/TemporalIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAIntersectsGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalEDWithinGeometryLogicalFunction.hpp>
#include <Functions/Meos/TemporalAtStBoxLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SessionWindow.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <CommonParserFunctions.hpp>
#include <ErrorHandling.hpp>
#include <ParserUtil.hpp>

namespace NES::Parsers
{
namespace
{
/// The constant an argument of a call is, when the whole argument is one, which the parser keeps in the
/// constant builder (exitConstantDefault)
AntlrSQLParser::ConstantDefaultContext* literalOf(antlr4::tree::ParseTree* argument)
{
    while (argument != nullptr)
    {
        if (auto* constant = dynamic_cast<AntlrSQLParser::ConstantDefaultContext*>(argument))
        {
            return constant;
        }
        if (argument->children.size() != 1)
        {
            return nullptr;
        }
        argument = argument->children.front();
    }
    return nullptr;
}

bool isLiteral(AntlrSQLParser::ExpressionContext* argument)
{
    return literalOf(argument) != nullptr;
}

/// The values of a row of an inline table: the expressions of a row constructor (a, b), or the row itself when it
/// holds one value, as literalOf walks down to the constant an argument is
std::vector<AntlrSQLParser::ExpressionContext*> rowValues(AntlrSQLParser::ExpressionContext* row)
{
    antlr4::tree::ParseTree* node = row;
    while (node != nullptr)
    {
        if (auto* constructor = dynamic_cast<AntlrSQLParser::RowConstructorContext*>(node))
        {
            std::vector<AntlrSQLParser::ExpressionContext*> values;
            for (auto* named : constructor->namedExpression())
            {
                values.push_back(named->expression());
            }
            return values;
        }
        if (node->children.size() != 1)
        {
            break;
        }
        node = node->children.front();
    }
    return {row};
}

/// The constant a literal argument states: a string VARSIZED, a boolean BOOLEAN, a number with a fraction
/// FLOAT64, an integer INT32 where it fits and INT64 otherwise
LogicalFunction literalConstant(AntlrSQLParser::ExpressionContext* argument, std::string value)
{
    auto* constant = literalOf(argument)->constant();
    const auto type = [&]
    {
        if (dynamic_cast<AntlrSQLParser::BooleanLiteralContext*>(constant) != nullptr)
        {
            return DataType::Type::BOOLEAN;
        }
        if (auto* numeric = dynamic_cast<AntlrSQLParser::NumericLiteralContext*>(constant))
        {
            if (dynamic_cast<AntlrSQLParser::FloatLiteralContext*>(numeric->number()) != nullptr)
            {
                return DataType::Type::FLOAT64;
            }
            const auto digits = value.starts_with('-') ? value.substr(1) : value;
            const bool fitsInt32 = digits.size() < 10 or (digits.size() == 10 and digits <= "2147483647");
            return fitsInt32 ? DataType::Type::INT32 : DataType::Type::INT64;
        }
        return DataType::Type::VARSIZED;
    }();
    return ConstantValueLogicalFunction(DataTypeProvider::provideDataType(type), std::move(value));
}
}

std::vector<LogicalFunction> AntlrSQLQueryPlanCreator::callArguments(AntlrSQLParser::FunctionCallContext* context) const
{
    const auto& helper = helpers.top();
    const auto literals = static_cast<size_t>(std::ranges::count_if(context->argument, isLiteral));
    const auto expressions = context->argument.size() - literals;
    if (helper.functionBuilder.size() < expressions or helper.constantBuilder.size() < literals)
    {
        return {};
    }
    auto expression = helper.functionBuilder.end() - static_cast<std::ptrdiff_t>(expressions);
    auto literal = helper.constantBuilder.end() - static_cast<std::ptrdiff_t>(literals);
    std::vector<LogicalFunction> arguments;
    arguments.reserve(context->argument.size());
    for (auto* argument : context->argument)
    {
        if (isLiteral(argument))
        {
            arguments.push_back(literalConstant(argument, *literal++));
        }
        else
        {
            arguments.push_back(*expression++);
        }
    }
    return arguments;
}

std::optional<std::shared_ptr<WindowAggregationLogicalFunction>>
AntlrSQLQueryPlanCreator::aggregationOverField(AntlrSQLParser::FunctionCallContext* context, const std::string& name) const
{
    /// The aggregation named over the field of the first argument, the others being literals
    const auto& helper = helpers.top();
    const auto& arguments = context->argument;
    if (arguments.empty() or isLiteral(arguments.front())
        or not std::all_of(arguments.begin() + 1, arguments.end(), isLiteral)
        or helper.functionBuilder.empty() or not helper.functionBuilder.back().tryGet<FieldAccessLogicalFunction>()
        or helper.constantBuilder.size() < arguments.size() - 1)
    {
        return std::nullopt;
    }
    const std::vector<std::string> literals(
        helper.constantBuilder.end() - static_cast<std::ptrdiff_t>(arguments.size() - 1), helper.constantBuilder.end());
    return AggregationLogicalFunctionProvider::tryProvide(name, helper.functionBuilder.back().get<FieldAccessLogicalFunction>(), literals);
}

void AntlrSQLQueryPlanCreator::takeCallArguments(AntlrSQLParser::FunctionCallContext* context)
{
    auto& helper = helpers.top();
    const auto literals = static_cast<size_t>(std::ranges::count_if(context->argument, isLiteral));
    helper.functionBuilder.resize(helper.functionBuilder.size() - (context->argument.size() - literals));
    helper.constantBuilder.resize(helper.constantBuilder.size() - literals);
}

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

/// The array of CROSS JOIN UNNEST is an expression over the fields of the relation before, read as a WHERE clause reads
/// its predicate; its alias u(x) names the field of the elements, u$x
void AntlrSQLQueryPlanCreator::enterUnnestRelation(AntlrSQLParser::UnnestRelationContext* context)
{
    if (helpers.top().unnest.has_value())
    {
        throw InvalidQuerySyntax("A query takes one CROSS JOIN UNNEST, but got another at {}", context->getText());
    }
    helpers.top().isUnnest = true;
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterUnnestRelation(context);
}

void AntlrSQLQueryPlanCreator::exitUnnestRelation(AntlrSQLParser::UnnestRelationContext* context)
{
    helpers.top().isWhereOrHaving = false;
    helpers.top().isUnnest = false;
    if (helpers.top().functionBuilder.size() != 1)
    {
        throw InvalidQuerySyntax("UNNEST takes one array, but got {} at {}", helpers.top().functionBuilder.size(), context->getText());
    }
    helpers.top().unnest.emplace(
        helpers.top().functionBuilder.back(),
        bindIdentifier(context->alias) + Schema::ATTRIBUTE_NAME_SEPARATOR + bindIdentifier(context->element));
    helpers.top().functionBuilder.clear();
    AntlrSQLBaseListener::exitUnnestRelation(context);
}

/// The values of the inline table of a cross join are expressions read as a WHERE clause reads its predicate; a literal
/// value stays in the constant builder and takes the type literalConstant gives it, and every other value is a function
/// of the function builder. The alias f(x, y) names the fields of the values, f$x and f$y
void AntlrSQLQueryPlanCreator::enterInlineTableRelation(AntlrSQLParser::InlineTableRelationContext* context)
{
    if (helpers.top().inlineTable.has_value() or helpers.top().unnest.has_value())
    {
        throw InvalidQuerySyntax("A query takes one cross join with an inline table or UNNEST, but got another at {}", context->getText());
    }
    helpers.top().isInlineTable = true;
    helpers.top().isWhereOrHaving = true;
    AntlrSQLBaseListener::enterInlineTableRelation(context);
}

void AntlrSQLQueryPlanCreator::exitInlineTableRelation(AntlrSQLParser::InlineTableRelationContext* context)
{
    auto& helper = helpers.top();
    helper.isWhereOrHaving = false;
    helper.isInlineTable = false;
    const auto alias = bindIdentifier(context->alias);
    std::vector<std::string> fields;
    for (auto* column : context->columns)
    {
        fields.push_back(alias + Schema::ATTRIBUTE_NAME_SEPARATOR + bindIdentifier(column));
    }
    std::vector<AntlrSQLParser::ExpressionContext*> cells;
    for (auto* row : context->rows)
    {
        const auto values = rowValues(row);
        if (values.size() != fields.size())
        {
            throw InvalidQuerySyntax("A row of {} holds {} values, but {} names {}", alias, values.size(), alias, fields.size());
        }
        cells.insert(cells.end(), values.begin(), values.end());
    }
    /// The builders hold the values in the order of the cells, the literals in one and the functions in the other
    std::vector<LogicalFunction> values(cells.size());
    for (auto cell = cells.size(); cell-- > 0;)
    {
        if (isLiteral(cells[cell]))
        {
            if (helper.constantBuilder.empty())
            {
                throw InvalidQuerySyntax("A literal of {} is missing at {}", alias, context->getText());
            }
            values[cell] = literalConstant(cells[cell], std::move(helper.constantBuilder.back()));
            helper.constantBuilder.pop_back();
        }
        else
        {
            if (helper.functionBuilder.empty())
            {
                throw InvalidQuerySyntax("A value of {} is missing at {}", alias, context->getText());
            }
            values[cell] = std::move(helper.functionBuilder.back());
            helper.functionBuilder.pop_back();
        }
    }
    helper.inlineTable.emplace(std::move(values), std::move(fields));
    helper.hasUnnamedAggregation = false;
    AntlrSQLBaseListener::exitInlineTableRelation(context);
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
    if (helpers.top().isFrom && !helpers.top().isJoinRelation && !helpers.top().isUnnest && !helpers.top().isInlineTable)
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
    else if (
        helpers.top().isFrom and not helpers.top().isJoinRelation and not helpers.top().isUnnest and not helpers.top().isInlineTable
        and AntlrSQLParser::RuleErrorCapturingIdentifier == parentRuleIndex)
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

    if (helpers.top().unnest.has_value())
    {
        auto [array, elementField] = std::move(*helpers.top().unnest);
        queryPlan = LogicalPlanBuilder::addUnnest(std::move(array), std::move(elementField), queryPlan);
    }
    if (helpers.top().inlineTable.has_value())
    {
        auto [values, fields] = std::move(*helpers.top().inlineTable);
        queryPlan = LogicalPlanBuilder::addInlineTableJoin(std::move(values), std::move(fields), queryPlan);
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

void AntlrSQLQueryPlanCreator::exitGapParameter(AntlrSQLParser::GapParameterContext* context)
{
    if (context->children.size() < 3)
    {
        throw InvalidQuerySyntax("GapParameter must have 'GAP', a number, and a time unit.");
    }
    helpers.top().size = std::stoi(context->children.at(1)->getText());
    AntlrSQLBaseListener::exitGapParameter(context);
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

void AntlrSQLQueryPlanCreator::exitWatermarkParameters(AntlrSQLParser::WatermarkParametersContext* context)
{
    /// WATERMARK(field, n unit) after the window: the watermark of the window's event time trails the largest event
    /// time read by n units, as Flink's WATERMARK FOR field AS field - INTERVAL 'n' unit does
    auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(helpers.top().windowType.get());
    if (timeWindow == nullptr
        or timeWindow->getTimeCharacteristic().getType() != Windowing::TimeCharacteristic::Type::EventTime)
    {
        throw InvalidQuerySyntax("A watermark needs a window over an event time at {}", context->getText());
    }
    const auto field = bindIdentifier(context->watermarkIdentifier);
    if (field != helpers.top().timestamp)
    {
        throw InvalidQuerySyntax(
            "The watermark is on {}, the window on the event time {}, at {}", field, helpers.top().timestamp, context->getText());
    }
    const auto lateness
        = buildTimeMeasure(std::stoi(context->watermark->getText()), context->watermarkTimeUnit->getStop()->getType());
    timeWindow->setAllowedLateness(lateness.getTime());
    AntlrSQLBaseListener::exitWatermarkParameters(context);
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

void AntlrSQLQueryPlanCreator::exitSessionWindow(AntlrSQLParser::SessionWindowContext* context)
{
    /// The grammar requires the timestamp field: a session closes on the event time of its key's records
    const auto gap = buildTimeMeasure(helpers.top().size, helpers.top().timeUnit);
    if (gap.getTime() == 0)
    {
        throw InvalidQuerySyntax("The gap of a session window must be positive.");
    }
    helpers.top().windowType = std::make_shared<Windowing::SessionWindow>(
        Windowing::TimeCharacteristic::createEventTime(FieldAccessLogicalFunction(helpers.top().timestamp)), gap);
    AntlrSQLBaseListener::exitSessionWindow(context);
}

void AntlrSQLQueryPlanCreator::exitNamedExpression(AntlrSQLParser::NamedExpressionContext* context)
{
    AntlrSQLHelper& helper = helpers.top();
    /// A value of a row of an inline table is neither projected nor aggregated: exitInlineTableRelation reads it
    if (helper.isInlineTable)
    {
        AntlrSQLBaseListener::exitNamedExpression(context);
        return;
    }
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
    if (std::dynamic_pointer_cast<Windowing::SessionWindow>(helpers.top().windowType))
    {
        throw InvalidQuerySyntax("A join cannot use a session window, only a tumbling or a sliding one, at {}", context->getText());
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
    /// The MEOS spatiotemporal relationships are called by the names the MEOS catalog states for them (eIntersects,
    /// aIntersects, eContains, eDwithin). The lexer reads those names as identifiers, so they reserve no word and match
    /// regardless of case. Over coordinates and timestamps, 4 or 6 arguments (5 with eDwithin's distance), each takes a
    /// case label of its own, past the lexer's token types; over MEOS values they are the generated MEOS functions.
    constexpr size_t MeosEIntersects = std::numeric_limits<size_t>::max();
    constexpr size_t MeosAIntersects = MeosEIntersects - 1;
    constexpr size_t MeosEContains = MeosEIntersects - 2;
    constexpr size_t MeosEDwithin = MeosEIntersects - 3;
    const auto arguments = context->argument.size();
    const bool overCoordinates = arguments == 4 or arguments == 6;
    const auto tokenType = [&]() -> size_t
    {
        if (funcName == "EINTERSECTS" and overCoordinates) { return MeosEIntersects; }
        if (funcName == "AINTERSECTS" and overCoordinates) { return MeosAIntersects; }
        if (funcName == "ECONTAINS" and overCoordinates) { return MeosEContains; }
        if (funcName == "EDWITHIN" and arguments == 5) { return MeosEDwithin; }
        return context->getStart()->getType();
    }();

    /// Only COUNT takes DISTINCT
    if (context->setQuantifier != nullptr and tokenType != AntlrSQLLexer::COUNT)
    {
        throw InvalidQuerySyntax("DISTINCT applies to COUNT only, but got {}", context->getText());
    }
    helpers.top().hasUnnamedAggregation = true;
    switch (tokenType) /// TODO #619: improve this switch case
    {
        case AntlrSQLLexer::COUNT:
            if (helpers.top().functionBuilder.empty())
            {
                throw InvalidQuerySyntax("Aggregation requires argument at {}", context->getText());
            }
            /// COUNT(DISTINCT x) counts the distinct values of x, as SQL states it
            if (context->setQuantifier != nullptr)
            {
                helpers.top().windowAggs.push_back(std::make_shared<CountDistinctAggregationLogicalFunction>(
                    helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            }
            else
            {
                helpers.top().windowAggs.push_back(
                    std::make_shared<CountAggregationLogicalFunction>(helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>()));
            }
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
            // result type (FLOAT64 instead of VARSIZED).
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
            // (the parser parks numeric/string literals there).
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
            // functionBuilder; the fifth and sixth are pulled from constantBuilder, as
            // PAIR_MEETING pulls its dMeet constant.
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
        case MeosEIntersects:
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
                    throw InvalidQuerySyntax("eIntersects requires either 4 arguments (lon1, lat1, timestamp1, static_geometry) or 6 arguments (lon1, lat1, timestamp1, lon2, lat2, timestamp2), but got {}", argCount);
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
        case MeosAIntersects:
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
                    throw InvalidQuerySyntax("aIntersects requires either 4 arguments (lon1, lat1, timestamp1, static_geometry) or 6 arguments (lon1, lat1, timestamp1, lon2, lat2, timestamp2), but got {}", argCount);
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
        case MeosEContains: 
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
                throw InvalidQuerySyntax("eContains expects 4 or 6 arguments");
            }
        }
        break;
        case MeosEDwithin:
        {
            const auto argCount = context->expression().size();
            if (argCount != 5)
            {
                throw InvalidQuerySyntax("eDwithin requires exactly five arguments (lon, lat, timestamp, geometry, distance), but got {}", argCount);
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
            PRECONDITION(total >= 5, "eDwithin requires (lon, lat, timestamp, geometry, distance), but got {}", total);

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

        default:
            /// Check if the function is a constructor for a datatype: a type named on one literal, as FLOAT64(0.2) or
            /// TGEOMPOINT('012E...'). A type a function shares its name with, as the cast geography(stbox), names the
            /// function on any other argument.
            if (const auto dataType = DataTypeProvider::tryProvideDataType(funcName);
                dataType.has_value()
                and (not LogicalFunctionProvider::contains(funcName)
                     or (context->argument.size() == 1 and isLiteral(context->argument.front()))))
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
            else if (auto aggregation = aggregationOverField(context, funcName))
            {
                /// An aggregation over one field, as COUNT is, and the literals following it; the field is left for
                /// the result's name
                helpers.top().constantBuilder.resize(helpers.top().constantBuilder.size() - (context->argument.size() - 1));
                helpers.top().windowAggs.push_back(*aggregation);
            }
            else if (auto logicalFunction = LogicalFunctionProvider::tryProvide(funcName, callArguments(context)))
            {
                /// The call's arguments are taken from the builders and replaced by the function
                takeCallArguments(context);
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
