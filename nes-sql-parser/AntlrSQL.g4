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

grammar AntlrSQL;

@lexer::postinclude {
#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)
}

@parser::postinclude {
#include <Util/DisableWarningsPragma.hpp>
DISABLE_WARNING_PUSH
DISABLE_WARNING(-Wlogical-op-parentheses)
DISABLE_WARNING(-Wunused-parameter)
}

@parser::members {
      bool SQL_standard_keyword_behavior = false;
      bool legacy_exponent_literal_as_decimal_enabled = false;
}

@lexer::members {
  bool isValidDecimal() {
    int nextChar = _input->LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  bool isHint() {
    int nextChar = _input->LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }
}

singleStatement: statement ';'? EOF;

terminatedStatement: statement ';';
multipleStatements: (statement (';' statement)* ';'?)? EOF;
statement: query | createStatement | dropStatement | showStatement;

createStatement: CREATE createDefinition;
createDefinition: createLogicalSourceDefinition | createPhysicalSourceDefinition | createSinkDefinition;
createLogicalSourceDefinition: LOGICAL SOURCE sourceName=identifier schemaDefinition fromQuery?;

createPhysicalSourceDefinition: PHYSICAL SOURCE FOR logicalSource=identifier
                                TYPE type=identifier
                                (SET '(' options=namedConfigExpressionSeq ')')?;

createSinkDefinition: SINK sinkName=identifier schemaDefinition TYPE type=identifier (SET '(' options=namedConfigExpressionSeq ')')?;


schemaDefinition: '(' columnDefinition (',' columnDefinition)* ')';
columnDefinition: identifierChain typeDefinition;

typeDefinition: DATA_TYPE;

fromQuery: AS query;

dropStatement: DROP dropSubject;
dropSubject: dropQuery | dropSource | dropSink;
dropQuery: QUERY id=unsignedIntegerLiteral;
dropSource: dropLogicalSourceSubject | dropPhysicalSourceSubject;
dropLogicalSourceSubject: LOGICAL SOURCE name=strictIdentifier;
dropPhysicalSourceSubject: PHYSICAL SOURCE id=unsignedIntegerLiteral;
dropSink: SINK name=strictIdentifier;

showStatement: SHOW showSubject (WHERE showFilter)? (FORMAT showFormat)?;
showFormat: TEXT | JSON;
showSubject: QUERIES #showQueriesSubject
    | LOGICAL SOURCES #showLogicalSourcesSubject
    | PHYSICAL SOURCES (FOR logicalSourceName=strictIdentifier)? #showPhysicalSourcesSubject
    | SINKS #showSinksSubject;

showFilter: attr=strictIdentifier EQ value=constant;

query : queryTerm queryOrganization;

queryOrganization:
         (ORDER BY order+=sortItem (',' order+=sortItem)*)?
         (LIMIT (ALL | limit=INTEGER_VALUE))?
         (OFFSET offset=INTEGER_VALUE)?
         ;

queryTerm: queryPrimary #primaryQuery
         |  left=queryTerm setoperator=UNION right=queryTerm  #setOperation
         ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | fromStatement                                                         #fromStmt
    | TABLE multipartIdentifier                                             #table
    | inlineTable                                                           #inlineTableDefault1
    | '(' query ')'                                                         #subquery
    ;
/// new layout to be closer to traditional SQL
querySpecification: selectClause fromClause whereClause? windowedAggregationClause? havingClause? sinkClause?;


fromClause: FROM relation (',' relation)*;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN right=relationPrimary joinCriteria? windowClause
    | NATURAL joinType JOIN right=relationPrimary windowClause
    ;

joinType
    : INNER?
    ;

joinCriteria
    : ON booleanExpression
    ;

relationPrimary
    : multipartIdentifier tableAlias          #tableName
    | '(' query ')'  tableAlias               #aliasedQuery
    | '(' relation ')' tableAlias             #aliasedRelation
    | inlineTable                             #inlineTableDefault2
    | inlineSource                            #inlineDefinedSource
    ;

inlineSource
    : type=identifier '(' parameters=namedConfigExpressionSeq ')'
    ;

schema: SCHEMA schemaDefinition
 ;

fromStatement: fromClause fromStatementBody+;

fromStatementBody: selectClause whereClause? groupByClause?;

selectClause : SELECT (hints+=hint)* namedExpressionSeq;

whereClause: WHERE booleanExpression;

havingClause: HAVING booleanExpression;

inlineTable
    : VALUES expression (',' expression)* tableAlias
    ;

tableAlias
    : (AS? identifier identifierList?)?
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier ('.' parts+=errorCapturingIdentifier)*
    ;

namedConfigExpression: (constant | schema) AS name=identifierChain;

namedExpression
    : expression AS name=identifier
    | expression
    ;

identifier: strictIdentifier;

strictIdentifier
    : IDENTIFIER #unquotedIdentifier
    | quotedIdentifier #quotedIdentifierAlternative;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

identifierChain: strictIdentifier ('.' strictIdentifier)*;

identifierList
    : '(' identifierSeq ')'
    ;

identifierSeq
    : ident+=errorCapturingIdentifier (',' ident+=errorCapturingIdentifier)*
    ;

errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

errorCapturingIdentifierExtra
    : (MINUS identifier)+    #errorIdent
    |                        #realIdent
    ;

namedConfigExpressionSeq: (namedConfigExpression (',' namedConfigExpression)*)?;
namedExpressionSeq
    : namedExpression (',' namedExpression)*
    ;

expression
    : valueExpression
    | booleanExpression
    | identifier
    | schema
    ;

booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | EXISTS '(' query ')'                                         #exists
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression op=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression op=OR right=booleanExpression   #logicalBinary
    ;

/// Problem fixed that the querySpecification rule could match an empty string
windowedAggregationClause:
    groupByClause? windowClause watermarkClause?
    | windowClause groupByClause? watermarkClause?;

groupByClause
    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
      WITH kind=ROLLUP
    | WITH kind=CUBE
    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    | GROUP BY kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')'
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

windowClause
    : WINDOW windowSpec
    ;

watermarkClause: WATERMARK '(' watermarkParameters ')';

watermarkParameters: watermarkIdentifier=identifier ',' watermark=INTEGER_VALUE watermarkTimeUnit=timeUnit;
/// Adding Threshold Windows
windowSpec:
    timeWindow #timeBasedWindow
    | countWindow #countBasedWindow
    | conditionWindow #thresholdBasedWindow
    ;

timeWindow
    : TUMBLING '(' (timestampParameter ',')?  sizeParameter ')'                       #tumblingWindow
    | SLIDING '(' (timestampParameter ',')? sizeParameter ',' advancebyParameter ')' #slidingWindow
    ;

countWindow:
    TUMBLING '(' INTEGER_VALUE ')'    #countBasedTumbling
    ;

conditionWindow
    : THRESHOLD '(' conditionParameter (',' thresholdMinSizeParameter)? ')' #thresholdWindow
    ;

conditionParameter: expression;
thresholdMinSizeParameter: INTEGER_VALUE;

sizeParameter: SIZE INTEGER_VALUE timeUnit;

advancebyParameter: ADVANCE BY INTEGER_VALUE timeUnit;

timeUnit: MS
        | SEC
        | MINUTE
        | HOUR
        | DAY
        ;

timestampParameter: name=identifier;

functionName:  IDENTIFIER | AVG | MAX | MIN | SUM | COUNT | MEDIAN | ARRAY_AGG | VAR | TEMPORAL_SEQUENCE | TEMPORAL_EINTERSECTS_GEOMETRY | TEMPORAL_AINTERSECTS_GEOMETRY | TEMPORAL_ECONTAINS_GEOMETRY | ALWAYS_EQ_TBIGINT_BIGINT | ALWAYS_GE_TBIGINT_BIGINT | ALWAYS_GT_TBIGINT_BIGINT | ALWAYS_LE_TBIGINT_BIGINT | ALWAYS_LT_TBIGINT_BIGINT | ALWAYS_NE_TBIGINT_BIGINT | ALWAYS_EQ_TFLOAT_FLOAT | ALWAYS_GE_TFLOAT_FLOAT | ALWAYS_GT_TFLOAT_FLOAT | ALWAYS_LE_TFLOAT_FLOAT | ALWAYS_LT_TFLOAT_FLOAT | ALWAYS_NE_TFLOAT_FLOAT | ALWAYS_EQ_TINT_INT | ALWAYS_GE_TINT_INT | ALWAYS_GT_TINT_INT | ALWAYS_LE_TINT_INT | ALWAYS_LT_TINT_INT | ALWAYS_NE_TINT_INT | EDWITHIN_TGEO_GEO | EVER_EQ_TBIGINT_BIGINT | EVER_GE_TBIGINT_BIGINT | EVER_GT_TBIGINT_BIGINT | EVER_LE_TBIGINT_BIGINT | EVER_LT_TBIGINT_BIGINT | EVER_NE_TBIGINT_BIGINT | EVER_EQ_TFLOAT_FLOAT | EVER_GE_TFLOAT_FLOAT | EVER_GT_TFLOAT_FLOAT | EVER_LE_TFLOAT_FLOAT | EVER_LT_TFLOAT_FLOAT | EVER_NE_TFLOAT_FLOAT | EVER_EQ_TFLOAT_TFLOAT | EVER_GE_TFLOAT_TFLOAT | EVER_GT_TFLOAT_TFLOAT | EVER_LE_TFLOAT_TFLOAT | EVER_LT_TFLOAT_TFLOAT | EVER_NE_TFLOAT_TFLOAT | EVER_EQ_TINT_INT | EVER_GE_TINT_INT | EVER_GT_TINT_INT | EVER_LE_TINT_INT | EVER_LT_TINT_INT | EVER_NE_TINT_INT | TGEO_AT_STBOX | ADD_BIGINT_TBIGINT | ADD_FLOAT_TFLOAT | ADD_INT_TINT | ADD_TBIGINT_BIGINT | ADD_TFLOAT_FLOAT | ADD_TINT_INT | ADD_TNUMBER_TNUMBER | DIV_BIGINT_TBIGINT | DIV_FLOAT_TFLOAT | DIV_INT_TINT | DIV_TBIGINT_BIGINT | DIV_TFLOAT_FLOAT | DIV_TINT_INT | DIV_TNUMBER_TNUMBER | MUL_BIGINT_TBIGINT | MUL_FLOAT_TFLOAT | MUL_INT_TINT | MUL_TBIGINT_BIGINT | MUL_TFLOAT_FLOAT | MUL_TINT_INT | MUL_TNUMBER_TNUMBER | SUB_BIGINT_TBIGINT | SUB_FLOAT_TFLOAT | SUB_INT_TINT | SUB_TBIGINT_BIGINT | SUB_TFLOAT_FLOAT | SUB_TINT_INT | SUB_TNUMBER_TNUMBER | TDISTANCE_TFLOAT_FLOAT | TDISTANCE_TINT_INT | TDISTANCE_TNUMBER_TNUMBER | TEMPORAL_ROUND | TFLOAT_CEIL | TFLOAT_COS | TFLOAT_DEGREES | TFLOAT_EXP | TFLOAT_FLOOR | TFLOAT_LN | TFLOAT_LOG10 | TFLOAT_RADIANS | TFLOAT_SCALE_VALUE | TFLOAT_SHIFT_SCALE_VALUE | TBIGINT_SCALE_VALUE | TBIGINT_SHIFT_SCALE_VALUE | TBIGINT_SHIFT_VALUE | TBIGINT_TO_TFLOAT | TBIGINT_TO_TINT | TFLOAT_SHIFT_VALUE | TFLOAT_SIN | TFLOAT_TAN | TFLOAT_TO_TBIGINT | TFLOAT_TO_TINT | TINT_SCALE_VALUE | TINT_SHIFT_SCALE_VALUE | TINT_SHIFT_VALUE | TINT_TO_TBIGINT | TINT_TO_TFLOAT | TNUMBER_ABS;

sinkClause: INTO sink (',' sink)*;

sink: identifier | inlineSink;

inlineSink
    : type=identifier '(' parameters=namedConfigExpressionSeq ')'
    ;

nullNotnull
    : NOT? NULLTOKEN
    ;

streamName: IDENTIFIER;

fileFormat: CSV_FORMAT;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrder=(LAST | FIRST))?
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN '(' expression (',' expression)* ')'
    | NOT? kind=IN '(' query ')'
    | NOT? kind=RLIKE pattern=valueExpression
    | NOT? kind=LIKE quantifier=(ANY | SOME | ALL) ('('')' | '(' expression (',' expression)* ')')
    | NOT? kind=LIKE pattern=valueExpression (ESCAPE escapeChar=STRING)?
    | IS nullNotnull
    | IS NOT? kind=(TRUE | FALSE | UNKNOWN)
    | IS NOT? kind=DISTINCT FROM right=valueExpression
    ;


valueExpression
    : (functionName | typeDefinition) '(' (argument+=expression (',' argument+=expression)*)? ')'                 #functionCall
    | op=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression op=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression op=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression       #arithmeticBinary
    | left=valueExpression op=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression op=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression op=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    | primaryExpression                                                                      #valueExpressionDefault
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
    ;

hint
    : '/*+' hintStatements+=hintStatement (','? hintStatements+=hintStatement)* '*/'
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier '(' parameters+=primaryExpression (',' parameters+=primaryExpression)* ')'
    ;

primaryExpression
    : ASTERISK                                                                                 #star
    | qualifiedName '.' ASTERISK                                                               #star
    | base=primaryExpression '.' fieldName=identifier                                          #dereference
    | '(' query ')'                                                                            #subqueryExpression
    | '(' namedExpression (',' namedExpression)+ ')'                                           #rowConstructor
    | '(' expression ')'                                                                       #parenthesizedExpression
    | constant                                                                                 #constantDefault
    | identifier                                                                               #columnReference
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

number
    : MINUS? INTEGER_VALUE              #integerLiteral
    | MINUS? FLOAT_LITERAL              #floatLiteral
    ;

unsignedIntegerLiteral: INTEGER_VALUE;

signedIntegerLiteral: MINUS INTEGER_VALUE;

constant
    : NULLTOKEN                                                                                #nullLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING                                                                                  #stringLiteral
    ;

booleanValue
    : TRUE | FALSE
    ;


ALL: 'ALL' | 'all';
AND: 'AND' | 'and';
ANY: 'ANY';
AS: 'AS' | 'as';
ASC: 'ASC' | 'asc';
AT: 'AT';
BETWEEN: 'BETWEEN' | 'between';
BY: 'BY' | 'by';
COMMENT: 'COMMENT';
CUBE: 'CUBE';
DELETE: 'DELETE';
DESC: 'DESC' | 'desc';
DISTINCT: 'DISTINCT';
DIV: 'DIV';
DROP: 'DROP';
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
EXISTS: 'EXISTS';
FALSE: 'FALSE';
FIRST: 'FIRST';
FOR: 'FOR';
FROM: 'FROM' | 'from';
FULL: 'FULL';
GROUP: 'GROUP' | 'group';
GROUPING: 'GROUPING';
HAVING: 'HAVING' | 'having';
IF: 'IF';
IN: 'IN' | 'in';
INNER: 'INNER' | 'inner';
INSERT: 'INSERT' | 'insert';
INTO: 'INTO' | 'into';
IS: 'IS'  'is';
JOIN: 'JOIN' | 'join';
LAST: 'LAST';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT' | 'limit';
LIST: 'LIST';
MERGE: 'MERGE' | 'merge';
NATURAL: 'NATURAL';
NOT: 'NOT' | 'not' | '!';
NULLTOKEN:'NULL';
NULLS: 'NULLS';
OF: 'OF';
ON: 'ON' | 'on';
OR: 'OR' | 'or';
ORDER: 'ORDER' | 'order';
QUERY: 'QUERY';
RECOVER: 'RECOVER';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE' | 'REGEXP';
ROLLUP: 'ROLLUP';
SCHEMA: 'SCHEMA';
SELECT: 'SELECT' | 'select';
SETS: 'SETS';
SOME: 'SOME';
START: 'START';
TABLE: 'TABLE';
TO: 'TO';
TRUE: 'TRUE';
TYPE: 'TYPE';
UNION: 'UNION' | 'union';
UNKNOWN: 'UNKNOWN';
USE: 'USE';
USING: 'USING';
VALUES: 'VALUES';
WHEN: 'WHEN';
WHERE: 'WHERE' | 'where';
WINDOW: 'WINDOW' | 'window';
WITH: 'WITH';
SET: 'SET';
TUMBLING: 'TUMBLING' | 'tumbling';
SLIDING: 'SLIDING' | 'sliding';
THRESHOLD : 'THRESHOLD'|'threshold';
SIZE: 'SIZE' | 'size';
ADVANCE: 'ADVANCE' | 'advance';
MS: 'MS' | 'ms';
SEC: 'SEC' | 'sec';
MINUTE: 'MINUTE' | 'minute' | 'MINUTES' | 'minutes';
HOUR: 'HOUR' | 'hour' | 'HOURS' | 'hours';
DAY: 'DAY' | 'day' | 'DAYS' | 'days';
MIN: 'MIN' | 'min';
MAX: 'MAX' | 'max';
AVG: 'AVG' | 'avg';
SUM: 'SUM' | 'sum';
COUNT: 'COUNT' | 'count';
MEDIAN: 'MEDIAN' | 'median';
VAR: 'VAR' | 'var';
ARRAY_AGG: 'ARRAY_AGG' | 'array_agg';
TEMPORAL_SEQUENCE: 'TEMPORAL_SEQUENCE' | 'temporal_sequence';
TEMPORAL_EINTERSECTS_GEOMETRY: 'TEMPORAL_EINTERSECTS_GEOMETRY' | 'temporal_eintersects_geometry';
TEMPORAL_AINTERSECTS_GEOMETRY: 'TEMPORAL_AINTERSECTS_GEOMETRY' | 'temporal_aintersects_geometry';
TEMPORAL_ECONTAINS_GEOMETRY: 'TEMPORAL_ECONTAINS_GEOMETRY' | 'temporal_econtains_geometry';
ALWAYS_EQ_TBIGINT_BIGINT: 'ALWAYS_EQ_TBIGINT_BIGINT' | 'always_eq_tbigint_bigint';
ALWAYS_GE_TBIGINT_BIGINT: 'ALWAYS_GE_TBIGINT_BIGINT' | 'always_ge_tbigint_bigint';
ALWAYS_GT_TBIGINT_BIGINT: 'ALWAYS_GT_TBIGINT_BIGINT' | 'always_gt_tbigint_bigint';
ALWAYS_LE_TBIGINT_BIGINT: 'ALWAYS_LE_TBIGINT_BIGINT' | 'always_le_tbigint_bigint';
ALWAYS_LT_TBIGINT_BIGINT: 'ALWAYS_LT_TBIGINT_BIGINT' | 'always_lt_tbigint_bigint';
ALWAYS_NE_TBIGINT_BIGINT: 'ALWAYS_NE_TBIGINT_BIGINT' | 'always_ne_tbigint_bigint';
ALWAYS_EQ_TFLOAT_FLOAT: 'ALWAYS_EQ_TFLOAT_FLOAT' | 'always_eq_tfloat_float';
ALWAYS_GE_TFLOAT_FLOAT: 'ALWAYS_GE_TFLOAT_FLOAT' | 'always_ge_tfloat_float';
ALWAYS_GT_TFLOAT_FLOAT: 'ALWAYS_GT_TFLOAT_FLOAT' | 'always_gt_tfloat_float';
ALWAYS_LE_TFLOAT_FLOAT: 'ALWAYS_LE_TFLOAT_FLOAT' | 'always_le_tfloat_float';
ALWAYS_LT_TFLOAT_FLOAT: 'ALWAYS_LT_TFLOAT_FLOAT' | 'always_lt_tfloat_float';
ALWAYS_NE_TFLOAT_FLOAT: 'ALWAYS_NE_TFLOAT_FLOAT' | 'always_ne_tfloat_float';
ALWAYS_EQ_TINT_INT: 'ALWAYS_EQ_TINT_INT' | 'always_eq_tint_int';
ALWAYS_GE_TINT_INT: 'ALWAYS_GE_TINT_INT' | 'always_ge_tint_int';
ALWAYS_GT_TINT_INT: 'ALWAYS_GT_TINT_INT' | 'always_gt_tint_int';
ALWAYS_LE_TINT_INT: 'ALWAYS_LE_TINT_INT' | 'always_le_tint_int';
ALWAYS_LT_TINT_INT: 'ALWAYS_LT_TINT_INT' | 'always_lt_tint_int';
ALWAYS_NE_TINT_INT: 'ALWAYS_NE_TINT_INT' | 'always_ne_tint_int';
EDWITHIN_TGEO_GEO: 'EDWITHIN_TGEO_GEO' | 'edwithin_tgeo_geo';
EVER_EQ_TBIGINT_BIGINT: 'EVER_EQ_TBIGINT_BIGINT' | 'ever_eq_tbigint_bigint';
EVER_GE_TBIGINT_BIGINT: 'EVER_GE_TBIGINT_BIGINT' | 'ever_ge_tbigint_bigint';
EVER_GT_TBIGINT_BIGINT: 'EVER_GT_TBIGINT_BIGINT' | 'ever_gt_tbigint_bigint';
EVER_LE_TBIGINT_BIGINT: 'EVER_LE_TBIGINT_BIGINT' | 'ever_le_tbigint_bigint';
EVER_LT_TBIGINT_BIGINT: 'EVER_LT_TBIGINT_BIGINT' | 'ever_lt_tbigint_bigint';
EVER_NE_TBIGINT_BIGINT: 'EVER_NE_TBIGINT_BIGINT' | 'ever_ne_tbigint_bigint';
EVER_EQ_TFLOAT_FLOAT: 'EVER_EQ_TFLOAT_FLOAT' | 'ever_eq_tfloat_float';
EVER_GE_TFLOAT_FLOAT: 'EVER_GE_TFLOAT_FLOAT' | 'ever_ge_tfloat_float';
EVER_GT_TFLOAT_FLOAT: 'EVER_GT_TFLOAT_FLOAT' | 'ever_gt_tfloat_float';
EVER_LE_TFLOAT_FLOAT: 'EVER_LE_TFLOAT_FLOAT' | 'ever_le_tfloat_float';
EVER_LT_TFLOAT_FLOAT: 'EVER_LT_TFLOAT_FLOAT' | 'ever_lt_tfloat_float';
EVER_NE_TFLOAT_FLOAT: 'EVER_NE_TFLOAT_FLOAT' | 'ever_ne_tfloat_float';
EVER_EQ_TFLOAT_TFLOAT: 'EVER_EQ_TFLOAT_TFLOAT' | 'ever_eq_tfloat_tfloat';
EVER_GE_TFLOAT_TFLOAT: 'EVER_GE_TFLOAT_TFLOAT' | 'ever_ge_tfloat_tfloat';
EVER_GT_TFLOAT_TFLOAT: 'EVER_GT_TFLOAT_TFLOAT' | 'ever_gt_tfloat_tfloat';
EVER_LE_TFLOAT_TFLOAT: 'EVER_LE_TFLOAT_TFLOAT' | 'ever_le_tfloat_tfloat';
EVER_LT_TFLOAT_TFLOAT: 'EVER_LT_TFLOAT_TFLOAT' | 'ever_lt_tfloat_tfloat';
EVER_NE_TFLOAT_TFLOAT: 'EVER_NE_TFLOAT_TFLOAT' | 'ever_ne_tfloat_tfloat';
EVER_EQ_TINT_INT: 'EVER_EQ_TINT_INT' | 'ever_eq_tint_int';
EVER_GE_TINT_INT: 'EVER_GE_TINT_INT' | 'ever_ge_tint_int';
EVER_GT_TINT_INT: 'EVER_GT_TINT_INT' | 'ever_gt_tint_int';
EVER_LE_TINT_INT: 'EVER_LE_TINT_INT' | 'ever_le_tint_int';
EVER_LT_TINT_INT: 'EVER_LT_TINT_INT' | 'ever_lt_tint_int';
EVER_NE_TINT_INT: 'EVER_NE_TINT_INT' | 'ever_ne_tint_int';
TGEO_AT_STBOX: 'TGEO_AT_STBOX' | 'tgeo_at_stbox';
ADD_BIGINT_TBIGINT: 'ADD_BIGINT_TBIGINT' | 'add_bigint_tbigint';
ADD_FLOAT_TFLOAT: 'ADD_FLOAT_TFLOAT' | 'add_float_tfloat';
ADD_TBIGINT_BIGINT: 'ADD_TBIGINT_BIGINT' | 'add_tbigint_bigint';
ADD_TFLOAT_FLOAT: 'ADD_TFLOAT_FLOAT' | 'add_tfloat_float';
ADD_INT_TINT: 'ADD_INT_TINT' | 'add_int_tint';
ADD_TINT_INT: 'ADD_TINT_INT' | 'add_tint_int';
ADD_TNUMBER_TNUMBER: 'ADD_TNUMBER_TNUMBER' | 'add_tnumber_tnumber';
DIV_BIGINT_TBIGINT: 'DIV_BIGINT_TBIGINT' | 'div_bigint_tbigint';
DIV_FLOAT_TFLOAT: 'DIV_FLOAT_TFLOAT' | 'div_float_tfloat';
DIV_TBIGINT_BIGINT: 'DIV_TBIGINT_BIGINT' | 'div_tbigint_bigint';
DIV_TFLOAT_FLOAT: 'DIV_TFLOAT_FLOAT' | 'div_tfloat_float';
DIV_INT_TINT: 'DIV_INT_TINT' | 'div_int_tint';
DIV_TINT_INT: 'DIV_TINT_INT' | 'div_tint_int';
DIV_TNUMBER_TNUMBER: 'DIV_TNUMBER_TNUMBER' | 'div_tnumber_tnumber';
MUL_BIGINT_TBIGINT: 'MUL_BIGINT_TBIGINT' | 'mul_bigint_tbigint';
MUL_FLOAT_TFLOAT: 'MUL_FLOAT_TFLOAT' | 'mul_float_tfloat';
MUL_TBIGINT_BIGINT: 'MUL_TBIGINT_BIGINT' | 'mul_tbigint_bigint';
MUL_TFLOAT_FLOAT: 'MUL_TFLOAT_FLOAT' | 'mul_tfloat_float';
MUL_INT_TINT: 'MUL_INT_TINT' | 'mul_int_tint';
MUL_TINT_INT: 'MUL_TINT_INT' | 'mul_tint_int';
MUL_TNUMBER_TNUMBER: 'MUL_TNUMBER_TNUMBER' | 'mul_tnumber_tnumber';
SUB_BIGINT_TBIGINT: 'SUB_BIGINT_TBIGINT' | 'sub_bigint_tbigint';
SUB_FLOAT_TFLOAT: 'SUB_FLOAT_TFLOAT' | 'sub_float_tfloat';
SUB_TBIGINT_BIGINT: 'SUB_TBIGINT_BIGINT' | 'sub_tbigint_bigint';
SUB_TFLOAT_FLOAT: 'SUB_TFLOAT_FLOAT' | 'sub_tfloat_float';
SUB_INT_TINT: 'SUB_INT_TINT' | 'sub_int_tint';
SUB_TINT_INT: 'SUB_TINT_INT' | 'sub_tint_int';
SUB_TNUMBER_TNUMBER: 'SUB_TNUMBER_TNUMBER' | 'sub_tnumber_tnumber';
TBIGINT_SCALE_VALUE: 'TBIGINT_SCALE_VALUE' | 'tbigint_scale_value';
TBIGINT_SHIFT_SCALE_VALUE: 'TBIGINT_SHIFT_SCALE_VALUE' | 'tbigint_shift_scale_value';
TBIGINT_SHIFT_VALUE: 'TBIGINT_SHIFT_VALUE' | 'tbigint_shift_value';
TBIGINT_TO_TFLOAT: 'TBIGINT_TO_TFLOAT' | 'tbigint_to_tfloat';
TBIGINT_TO_TINT: 'TBIGINT_TO_TINT' | 'tbigint_to_tint';
TDISTANCE_TFLOAT_FLOAT: 'TDISTANCE_TFLOAT_FLOAT' | 'tdistance_tfloat_float';
TDISTANCE_TINT_INT: 'TDISTANCE_TINT_INT' | 'tdistance_tint_int';
TDISTANCE_TNUMBER_TNUMBER: 'TDISTANCE_TNUMBER_TNUMBER' | 'tdistance_tnumber_tnumber';
TEMPORAL_ROUND: 'TEMPORAL_ROUND' | 'temporal_round';
TFLOAT_CEIL: 'TFLOAT_CEIL' | 'tfloat_ceil';
TFLOAT_COS: 'TFLOAT_COS' | 'tfloat_cos';
TFLOAT_DEGREES: 'TFLOAT_DEGREES' | 'tfloat_degrees';
TFLOAT_EXP: 'TFLOAT_EXP' | 'tfloat_exp';
TFLOAT_FLOOR: 'TFLOAT_FLOOR' | 'tfloat_floor';
TFLOAT_LN: 'TFLOAT_LN' | 'tfloat_ln';
TFLOAT_LOG10: 'TFLOAT_LOG10' | 'tfloat_log10';
TFLOAT_RADIANS: 'TFLOAT_RADIANS' | 'tfloat_radians';
TFLOAT_SCALE_VALUE: 'TFLOAT_SCALE_VALUE' | 'tfloat_scale_value';
TFLOAT_SHIFT_SCALE_VALUE: 'TFLOAT_SHIFT_SCALE_VALUE' | 'tfloat_shift_scale_value';
TFLOAT_SHIFT_VALUE: 'TFLOAT_SHIFT_VALUE' | 'tfloat_shift_value';
TFLOAT_SIN: 'TFLOAT_SIN' | 'tfloat_sin';
TFLOAT_TAN: 'TFLOAT_TAN' | 'tfloat_tan';
TFLOAT_TO_TBIGINT: 'TFLOAT_TO_TBIGINT' | 'tfloat_to_tbigint';
TFLOAT_TO_TINT: 'TFLOAT_TO_TINT' | 'tfloat_to_tint';
TINT_SCALE_VALUE: 'TINT_SCALE_VALUE' | 'tint_scale_value';
TINT_SHIFT_SCALE_VALUE: 'TINT_SHIFT_SCALE_VALUE' | 'tint_shift_scale_value';
TINT_SHIFT_VALUE: 'TINT_SHIFT_VALUE' | 'tint_shift_value';
TINT_TO_TBIGINT: 'TINT_TO_TBIGINT' | 'tint_to_tbigint';
TINT_TO_TFLOAT: 'TINT_TO_TFLOAT' | 'tint_to_tfloat';
TNUMBER_ABS: 'TNUMBER_ABS' | 'tnumber_abs';
WATERMARK: 'WATERMARK' | 'watermark';
OFFSET: 'OFFSET' | 'offset';
LOCALHOST: 'LOCALHOST' | 'localhost';
CSV_FORMAT : 'CSV_FORMAT';
AT_MOST_ONCE : 'AT_MOST_ONCE';
AT_LEAST_ONCE : 'AT_LEAST_ONCE';
JSON: 'JSON';
TEXT: 'TEXT';
///--NebulaSQL-KEYWORD-LIST-END
///****************************
/// End of the keywords list
///****************************



BOOLEAN_VALUE: 'true' | 'false';
EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
CONCAT_PIPE: '||';
HAT: '^';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT?
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;


fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : ('a'..'z'|'A'..'Z'|'_')
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;


SINKS: 'SINKS';
SOURCES: 'SOURCES' | 'sources';
QUERIES: 'QUERIES' | 'queries';


DATA_TYPE: INTEGER_SIGNED_TYPE | INTEGER_UNSIGNED_TYPE | FLOATING_POINT_TYPE | CHAR_TYPE | VARSIZED_TYPE | BOOLEAN_TYPE;

INTEGER_UNSIGNED_TYPE: UNSIGNED_TYPE_QUALIFIER INTEGER_BASES_TYPES | 'UINT8' | 'UINT16' | 'UINT32' | 'UINT64';
INTEGER_SIGNED_TYPE: INTEGER_BASES_TYPES | 'INT64' | 'INT32' | 'INT16' | 'INT8';
INTEGER_BASES_TYPES: TINY_INT_TYPE | SMALL_INT_TYPE | NORMAL_INT_TYPE | BIG_INT_TYPE;
TINY_INT_TYPE: 'TINYINT';
SMALL_INT_TYPE: 'SMALLINT';
NORMAL_INT_TYPE: 'INT' | 'INTEGER';
BIG_INT_TYPE: 'BIGINT';
FLOATING_POINT_TYPE: 'FLOAT32' | 'FLOAT64';
CHAR_TYPE: 'CHAR';
VARSIZED_TYPE: 'VARSIZED';
BOOLEAN_TYPE: 'BOOLEAN';

UNSIGNED_TYPE_QUALIFIER: 'UNSIGNED ';



SHOW : 'SHOW';
FORMAT : 'FORMAT';
CREATE : 'CREATE';
SOURCE : 'SOURCE';
LOGICAL: 'LOGICAL';
PHYSICAL: 'PHYSICAL';
SINK : 'SINK';

//Make sure that you add lexer rules for keywords before the identifier rule,
//otherwise it will take priority and your grammars will not work

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')*
    ;

/// Catch-all for anything we can't recognize.
/// We use this to be able to ignore and recover all the text
/// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
