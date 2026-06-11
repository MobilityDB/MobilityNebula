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

functionName:  IDENTIFIER | AVG | MAX | MIN | SUM | COUNT | MEDIAN | ARRAY_AGG | VAR | TEMPORAL_SEQUENCE | TEMPORAL_LENGTH | PAIR_MEETING | CROSS_DISTANCE | TEMPORAL_EINTERSECTS_GEOMETRY | TEMPORAL_AINTERSECTS_GEOMETRY | TEMPORAL_ECONTAINS_GEOMETRY | EDWITHIN_TGEO_GEO | TGEO_AT_STBOX | TEMPORAL_ADISJOINT_GEOMETRY | TEMPORAL_ECONTAINS_TGEOMETRY | TEMPORAL_ECOVERS_TGEOMETRY | TEMPORAL_EDISJOINT_TGEOMETRY | TEMPORAL_EINTERSECTS_TGEOMETRY | TEMPORAL_ETOUCHES_TGEOMETRY | TEMPORAL_ACONTAINS_TGEOMETRY | TEMPORAL_ADISJOINT_TGEOMETRY | TEMPORAL_AINTERSECTS_TGEOMETRY | TEMPORAL_ATOUCHES_TGEOMETRY | TEMPORAL_NAD_GEOMETRY | TEMPORAL_NAD_TGEOMETRY | TEMPORAL_EDWITHIN_TGEOMETRY | TEMPORAL_ADWITHIN_GEOMETRY | TEMPORAL_ADWITHIN_TGEOMETRY | TEMPORAL_EDISJOINT_GEOMETRY | TEMPORAL_ATOUCHES_GEOMETRY | TEMPORAL_ECOVERS_GEOMETRY | TEMPORAL_ACONTAINS_GEOMETRY | TEMPORAL_ETOUCHES_GEOMETRY | TEMPORAL_NAD_FLOAT_SCALAR | TEMPORAL_NAD_INT_SCALAR | TEMPORAL_NAD_TFLOAT | TEMPORAL_NAD_TINT | TEMPORAL_AT_GEOMETRY | TEMPORAL_MINUS_GEOMETRY | TEMPORAL_NUM_INSTANTS | TEMPORAL_NUM_SEQUENCES | TEMPORAL_NUM_TIMESTAMPS | TEMPORAL_TFLOAT_START_VALUE | TEMPORAL_TFLOAT_END_VALUE | TEMPORAL_TFLOAT_MIN_VALUE | TEMPORAL_TFLOAT_MAX_VALUE | TEMPORAL_TNUMBER_INTEGRAL | TEMPORAL_TINT_START_VALUE | TEMPORAL_TINT_END_VALUE | TEMPORAL_TINT_MIN_VALUE | TEMPORAL_TINT_MAX_VALUE | TEMPORAL_TFLOAT_AVG_VALUE | TEMPORAL_TNUMBER_TWAVG | TEMPORAL_TINT_AVG_VALUE | TEMPORAL_START_TIMESTAMP | TEMPORAL_END_TIMESTAMP | TEMPORAL_LOWER_INC | TEMPORAL_UPPER_INC | TEMPORAL_TPOINT_IS_SIMPLE | TEMPORAL_ECONTAINS_TCBUFFER | TEMPORAL_ECOVERS_TCBUFFER | TEMPORAL_EDISJOINT_TCBUFFER | TEMPORAL_EINTERSECTS_TCBUFFER | TEMPORAL_ETOUCHES_TCBUFFER | TEMPORAL_ACONTAINS_TCBUFFER | TEMPORAL_ACOVERS_TCBUFFER | TEMPORAL_ADISJOINT_TCBUFFER | TEMPORAL_AINTERSECTS_TCBUFFER | TEMPORAL_ATOUCHES_TCBUFFER | TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER | TEMPORAL_ECOVERS_TCBUFFER_CBUFFER | TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER | TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER | TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER | TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER | TEMPORAL_ACOVERS_TCBUFFER_CBUFFER | TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER | TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER | TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER | TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER | TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER | TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER | TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER | TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER | TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER | TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY | TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY | TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER | TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER | TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER | TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER | TEMPORAL_ECONTAINS_TPOSE_GEOMETRY | TEMPORAL_ECOVERS_TPOSE_GEOMETRY | TEMPORAL_EDISJOINT_TPOSE_GEOMETRY | TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY | TEMPORAL_ETOUCHES_TPOSE_GEOMETRY | TEMPORAL_ACONTAINS_TPOSE_GEOMETRY | TEMPORAL_ADISJOINT_TPOSE_GEOMETRY | TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY | TEMPORAL_ATOUCHES_TPOSE_GEOMETRY | TEMPORAL_ECONTAINS_TPOSE_TPOSE | TEMPORAL_ECOVERS_TPOSE_TPOSE | TEMPORAL_EDISJOINT_TPOSE_TPOSE | TEMPORAL_EINTERSECTS_TPOSE_TPOSE | TEMPORAL_ETOUCHES_TPOSE_TPOSE | TEMPORAL_ACONTAINS_TPOSE_TPOSE | TEMPORAL_ADISJOINT_TPOSE_TPOSE | TEMPORAL_AINTERSECTS_TPOSE_TPOSE | TEMPORAL_ATOUCHES_TPOSE_TPOSE | TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY | TEMPORAL_ECONTAINS_TNPOINT_TNPOINT | TEMPORAL_ECOVERS_TNPOINT_GEOMETRY | TEMPORAL_ECOVERS_TNPOINT_TNPOINT | TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY | TEMPORAL_EDISJOINT_TNPOINT_TNPOINT | TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY | TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT | TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY | TEMPORAL_ETOUCHES_TNPOINT_TNPOINT | TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY | TEMPORAL_ACONTAINS_TNPOINT_TNPOINT | TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY | TEMPORAL_ADISJOINT_TNPOINT_TNPOINT | TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY | TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT | TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY | TEMPORAL_ATOUCHES_TNPOINT_TNPOINT | TEMPORAL_NAD_TPOSE_GEOMETRY | TEMPORAL_NAD_TPOSE_TPOSE | TEMPORAL_NAD_TNPOINT_GEOMETRY | TEMPORAL_NAD_TNPOINT_TNPOINT | TEMPORAL_EDWITHIN_TPOSE_GEOMETRY | TEMPORAL_EDWITHIN_TPOSE_TPOSE | TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY | TEMPORAL_EDWITHIN_TNPOINT_TNPOINT | TEMPORAL_ADWITHIN_TPOSE_GEOMETRY | TEMPORAL_ADWITHIN_TPOSE_TPOSE | TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY | TEMPORAL_ADWITHIN_TNPOINT_TNPOINT | TEMPORAL_NAD_TCBUFFER | TEMPORAL_NAD_TCBUFFER_CBUFFER | TEMPORAL_NAD_TCBUFFER_TCBUFFER | ALWAYS_EQ_TFLOAT_FLOAT | ALWAYS_EQ_TINT_INT | ALWAYS_GE_TFLOAT_FLOAT | ALWAYS_GE_TINT_INT | ALWAYS_GT_TFLOAT_FLOAT | ALWAYS_GT_TINT_INT | ALWAYS_LE_TFLOAT_FLOAT | ALWAYS_LE_TINT_INT | ALWAYS_LT_TFLOAT_FLOAT | ALWAYS_LT_TINT_INT | ALWAYS_NE_TFLOAT_FLOAT | ALWAYS_NE_TINT_INT | EVER_EQ_TFLOAT_FLOAT | EVER_EQ_TINT_INT | EVER_GE_TFLOAT_FLOAT | EVER_GE_TINT_INT | EVER_GT_TFLOAT_FLOAT | EVER_GT_TINT_INT | EVER_LE_TFLOAT_FLOAT | EVER_LE_TINT_INT | EVER_LT_TFLOAT_FLOAT | EVER_LT_TINT_INT | EVER_NE_TFLOAT_FLOAT | EVER_NE_TINT_INT | ALWAYS_EQ_FLOAT_TFLOAT | ALWAYS_EQ_INT_TINT | ALWAYS_EQ_TEMPORAL_TEMPORAL | ALWAYS_GE_FLOAT_TFLOAT | ALWAYS_GE_INT_TINT | ALWAYS_GE_TEMPORAL_TEMPORAL | ALWAYS_GT_FLOAT_TFLOAT | ALWAYS_GT_INT_TINT | ALWAYS_GT_TEMPORAL_TEMPORAL | ALWAYS_LE_FLOAT_TFLOAT | ALWAYS_LE_INT_TINT | ALWAYS_LE_TEMPORAL_TEMPORAL | ALWAYS_LT_FLOAT_TFLOAT | ALWAYS_LT_INT_TINT | ALWAYS_LT_TEMPORAL_TEMPORAL | ALWAYS_NE_FLOAT_TFLOAT | ALWAYS_NE_INT_TINT | ALWAYS_NE_TEMPORAL_TEMPORAL | EVER_EQ_FLOAT_TFLOAT | EVER_EQ_INT_TINT | EVER_EQ_TEMPORAL_TEMPORAL | EVER_GE_FLOAT_TFLOAT | EVER_GE_INT_TINT | EVER_GE_TEMPORAL_TEMPORAL | EVER_GT_FLOAT_TFLOAT | EVER_GT_INT_TINT | EVER_GT_TEMPORAL_TEMPORAL | EVER_LE_FLOAT_TFLOAT | EVER_LE_INT_TINT | EVER_LE_TEMPORAL_TEMPORAL | EVER_LT_FLOAT_TFLOAT | EVER_LT_INT_TINT | EVER_LT_TEMPORAL_TEMPORAL | EVER_NE_FLOAT_TFLOAT | EVER_NE_INT_TINT | EVER_NE_TEMPORAL_TEMPORAL | ALWAYS_EQ_TCBUFFER_CBUFFER | ALWAYS_EQ_TCBUFFER_TCBUFFER | ALWAYS_EQ_TGEO_GEO | ALWAYS_EQ_TGEO_TGEO | ALWAYS_NE_TCBUFFER_CBUFFER | ALWAYS_NE_TCBUFFER_TCBUFFER | ALWAYS_NE_TGEO_GEO | ALWAYS_NE_TGEO_TGEO | ATOUCHES_TPOINT_GEO | ETOUCHES_TPOINT_GEO | EVER_EQ_TCBUFFER_CBUFFER | EVER_EQ_TCBUFFER_TCBUFFER | EVER_EQ_TGEO_GEO | EVER_EQ_TGEO_TGEO | EVER_NE_TCBUFFER_CBUFFER | EVER_NE_TCBUFFER_TCBUFFER | EVER_NE_TGEO_GEO | EVER_NE_TGEO_TGEO | ABOVE_TSPATIAL_TSPATIAL | ADJACENT_TEMPORAL_TEMPORAL | ADJACENT_TSPATIAL_TSPATIAL | AFTER_TEMPORAL_TEMPORAL | AFTER_TSPATIAL_TSPATIAL | ALWAYS_EQ_TBOOL_BOOL | ALWAYS_NE_TBOOL_BOOL | BACK_TSPATIAL_TSPATIAL | BEFORE_TEMPORAL_TEMPORAL | BEFORE_TSPATIAL_TSPATIAL | BELOW_TSPATIAL_TSPATIAL | CONTAINED_TEMPORAL_TEMPORAL | CONTAINED_TSPATIAL_TSPATIAL | CONTAINS_TEMPORAL_TEMPORAL | CONTAINS_TSPATIAL_TSPATIAL | EVER_EQ_TBOOL_BOOL | EVER_NE_TBOOL_BOOL | FRONT_TSPATIAL_TSPATIAL | LEFT_TSPATIAL_TSPATIAL | NAD_TNPOINT_GEO | NAD_TPOSE_GEO | OVERABOVE_TSPATIAL_TSPATIAL | OVERAFTER_TEMPORAL_TEMPORAL | OVERAFTER_TSPATIAL_TSPATIAL | OVERBACK_TSPATIAL_TSPATIAL | OVERBEFORE_TEMPORAL_TEMPORAL | OVERBEFORE_TSPATIAL_TSPATIAL | OVERBELOW_TSPATIAL_TSPATIAL | OVERFRONT_TSPATIAL_TSPATIAL | OVERLAPS_TEMPORAL_TEMPORAL | OVERLAPS_TSPATIAL_TSPATIAL | OVERLEFT_TSPATIAL_TSPATIAL | OVERRIGHT_TSPATIAL_TSPATIAL | RIGHT_TSPATIAL_TSPATIAL | SAME_TEMPORAL_TEMPORAL | SAME_TSPATIAL_TSPATIAL | TBOOL_END_VALUE | TBOOL_START_VALUE | TEMPORAL_CMP | TEMPORAL_DYNTIMEWARP_DISTANCE | TEMPORAL_EQ | TEMPORAL_FRECHET_DISTANCE | TEMPORAL_GE | TEMPORAL_GT | TEMPORAL_HAUSDORFF_DISTANCE | TEMPORAL_LE | TEMPORAL_LT | TEMPORAL_NE | TNPOINT_LENGTH | TBOOL_TO_TINT | TCBUFFER_TO_TFLOAT | TFLOAT_CEIL | TFLOAT_EXP | TFLOAT_FLOOR | TFLOAT_LN | TFLOAT_LOG10 | TFLOAT_RADIANS | TFLOAT_TO_TINT | TINT_TO_TFLOAT | ADJACENT_TNUMBER_TBOX | AFTER_TNUMBER_TBOX | BEFORE_TNUMBER_TBOX | CONTAINED_TNUMBER_TBOX | CONTAINS_TNUMBER_TBOX | LEFT_TNUMBER_TBOX | NAD_TCBUFFER_STBOX | NAD_TFLOAT_TBOX | NAD_TGEO_STBOX | NAD_TINT_TBOX | NAD_TNPOINT_STBOX | NAD_TPOSE_STBOX | OVERAFTER_TNUMBER_TBOX | OVERBEFORE_TNUMBER_TBOX | OVERLAPS_TNUMBER_TBOX | OVERLEFT_TNUMBER_TBOX | OVERRIGHT_TNUMBER_TBOX | RIGHT_TNUMBER_TBOX | SAME_TNUMBER_TBOX | TSPATIAL_EXTENT | TNUMBER_EXTENT | FLOAT_EXTENT | INT_EXTENT | BIGINT_EXTENT | TIMESTAMPTZ_EXTENT | ABOVE_STBOX_TSPATIAL | ABOVE_TSPATIAL_STBOX | ADJACENT_STBOX_TSPATIAL | ADJACENT_TBOX_TNUMBER | ADJACENT_TSPATIAL_STBOX | AFTER_STBOX_TSPATIAL | AFTER_TBOX_TNUMBER | AFTER_TSPATIAL_STBOX | BACK_STBOX_TSPATIAL | BACK_TSPATIAL_STBOX | BEFORE_STBOX_TSPATIAL | BEFORE_TBOX_TNUMBER | BEFORE_TSPATIAL_STBOX | BELOW_STBOX_TSPATIAL | BELOW_TSPATIAL_STBOX | CONTAINED_STBOX_TSPATIAL | CONTAINED_TBOX_TNUMBER | CONTAINED_TSPATIAL_STBOX | CONTAINS_STBOX_TSPATIAL | CONTAINS_TBOX_TNUMBER | CONTAINS_TSPATIAL_STBOX | FRONT_STBOX_TSPATIAL | FRONT_TSPATIAL_STBOX | LEFT_STBOX_TSPATIAL | LEFT_TBOX_TNUMBER | LEFT_TSPATIAL_STBOX | OVERABOVE_STBOX_TSPATIAL | OVERABOVE_TSPATIAL_STBOX | OVERAFTER_STBOX_TSPATIAL | OVERAFTER_TBOX_TNUMBER | OVERAFTER_TSPATIAL_STBOX | OVERBACK_STBOX_TSPATIAL | OVERBACK_TSPATIAL_STBOX | OVERBEFORE_STBOX_TSPATIAL | OVERBEFORE_TBOX_TNUMBER | OVERBEFORE_TSPATIAL_STBOX | OVERBELOW_STBOX_TSPATIAL | OVERBELOW_TSPATIAL_STBOX | OVERFRONT_STBOX_TSPATIAL | OVERFRONT_TSPATIAL_STBOX | OVERLAPS_STBOX_TSPATIAL | OVERLAPS_TBOX_TNUMBER | OVERLAPS_TSPATIAL_STBOX | OVERLEFT_STBOX_TSPATIAL | OVERLEFT_TBOX_TNUMBER | OVERLEFT_TSPATIAL_STBOX | OVERRIGHT_STBOX_TSPATIAL | OVERRIGHT_TBOX_TNUMBER | OVERRIGHT_TSPATIAL_STBOX | RIGHT_STBOX_TSPATIAL | RIGHT_TBOX_TNUMBER | RIGHT_TSPATIAL_STBOX | SAME_STBOX_TSPATIAL | SAME_TBOX_TNUMBER | SAME_TSPATIAL_STBOX | FLOAT_UNION | INT_UNION | BIGINT_UNION | TIMESTAMPTZ_UNION;

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
TEMPORAL_LENGTH: 'TEMPORAL_LENGTH' | 'temporal_length';
PAIR_MEETING: 'PAIR_MEETING' | 'pair_meeting';
CROSS_DISTANCE: 'CROSS_DISTANCE' | 'cross_distance';
TEMPORAL_EINTERSECTS_GEOMETRY: 'TEMPORAL_EINTERSECTS_GEOMETRY' | 'temporal_eintersects_geometry';
TEMPORAL_AINTERSECTS_GEOMETRY: 'TEMPORAL_AINTERSECTS_GEOMETRY' | 'temporal_aintersects_geometry';
TEMPORAL_ECONTAINS_GEOMETRY: 'TEMPORAL_ECONTAINS_GEOMETRY' | 'temporal_econtains_geometry';
EDWITHIN_TGEO_GEO: 'EDWITHIN_TGEO_GEO' | 'edwithin_tgeo_geo';
TGEO_AT_STBOX: 'TGEO_AT_STBOX' | 'tgeo_at_stbox';
/* BEGIN CODEGEN LEXER TOKENS */
TEMPORAL_ADISJOINT_GEOMETRY: 'TEMPORAL_ADISJOINT_GEOMETRY' | 'temporal_adisjoint_geometry';
TEMPORAL_ECONTAINS_TGEOMETRY: 'TEMPORAL_ECONTAINS_TGEOMETRY' | 'temporal_econtains_tgeometry';
TEMPORAL_ECOVERS_TGEOMETRY: 'TEMPORAL_ECOVERS_TGEOMETRY' | 'temporal_ecovers_tgeometry';
TEMPORAL_EDISJOINT_TGEOMETRY: 'TEMPORAL_EDISJOINT_TGEOMETRY' | 'temporal_edisjoint_tgeometry';
TEMPORAL_EINTERSECTS_TGEOMETRY: 'TEMPORAL_EINTERSECTS_TGEOMETRY' | 'temporal_eintersects_tgeometry';
TEMPORAL_ETOUCHES_TGEOMETRY: 'TEMPORAL_ETOUCHES_TGEOMETRY' | 'temporal_etouches_tgeometry';
TEMPORAL_ACONTAINS_TGEOMETRY: 'TEMPORAL_ACONTAINS_TGEOMETRY' | 'temporal_acontains_tgeometry';
TEMPORAL_ADISJOINT_TGEOMETRY: 'TEMPORAL_ADISJOINT_TGEOMETRY' | 'temporal_adisjoint_tgeometry';
TEMPORAL_AINTERSECTS_TGEOMETRY: 'TEMPORAL_AINTERSECTS_TGEOMETRY' | 'temporal_aintersects_tgeometry';
TEMPORAL_ATOUCHES_TGEOMETRY: 'TEMPORAL_ATOUCHES_TGEOMETRY' | 'temporal_atouches_tgeometry';
TEMPORAL_NAD_GEOMETRY: 'TEMPORAL_NAD_GEOMETRY' | 'temporal_nad_geometry';
TEMPORAL_NAD_TGEOMETRY: 'TEMPORAL_NAD_TGEOMETRY' | 'temporal_nad_tgeometry';
TEMPORAL_EDWITHIN_TGEOMETRY: 'TEMPORAL_EDWITHIN_TGEOMETRY' | 'temporal_edwithin_tgeometry';
TEMPORAL_ADWITHIN_GEOMETRY: 'TEMPORAL_ADWITHIN_GEOMETRY' | 'temporal_adwithin_geometry';
TEMPORAL_ADWITHIN_TGEOMETRY: 'TEMPORAL_ADWITHIN_TGEOMETRY' | 'temporal_adwithin_tgeometry';
TEMPORAL_EDISJOINT_GEOMETRY: 'TEMPORAL_EDISJOINT_GEOMETRY' | 'temporal_edisjoint_geometry';
TEMPORAL_ATOUCHES_GEOMETRY: 'TEMPORAL_ATOUCHES_GEOMETRY' | 'temporal_atouches_geometry';
TEMPORAL_ECOVERS_GEOMETRY: 'TEMPORAL_ECOVERS_GEOMETRY' | 'temporal_ecovers_geometry';
TEMPORAL_ACONTAINS_GEOMETRY: 'TEMPORAL_ACONTAINS_GEOMETRY' | 'temporal_acontains_geometry';
TEMPORAL_ETOUCHES_GEOMETRY: 'TEMPORAL_ETOUCHES_GEOMETRY' | 'temporal_etouches_geometry';
TEMPORAL_NAD_FLOAT_SCALAR: 'TEMPORAL_NAD_FLOAT_SCALAR' | 'temporal_nad_float_scalar';
TEMPORAL_NAD_INT_SCALAR: 'TEMPORAL_NAD_INT_SCALAR' | 'temporal_nad_int_scalar';
TEMPORAL_NAD_TFLOAT: 'TEMPORAL_NAD_TFLOAT' | 'temporal_nad_tfloat';
TEMPORAL_NAD_TINT: 'TEMPORAL_NAD_TINT' | 'temporal_nad_tint';
TEMPORAL_AT_GEOMETRY: 'TEMPORAL_AT_GEOMETRY' | 'temporal_at_geometry';
TEMPORAL_MINUS_GEOMETRY: 'TEMPORAL_MINUS_GEOMETRY' | 'temporal_minus_geometry';
TEMPORAL_ECONTAINS_TCBUFFER: 'TEMPORAL_ECONTAINS_TCBUFFER' | 'temporal_econtains_tcbuffer';
TEMPORAL_ECOVERS_TCBUFFER: 'TEMPORAL_ECOVERS_TCBUFFER' | 'temporal_ecovers_tcbuffer';
TEMPORAL_EDISJOINT_TCBUFFER: 'TEMPORAL_EDISJOINT_TCBUFFER' | 'temporal_edisjoint_tcbuffer';
TEMPORAL_EINTERSECTS_TCBUFFER: 'TEMPORAL_EINTERSECTS_TCBUFFER' | 'temporal_eintersects_tcbuffer';
TEMPORAL_ETOUCHES_TCBUFFER: 'TEMPORAL_ETOUCHES_TCBUFFER' | 'temporal_etouches_tcbuffer';
TEMPORAL_ACONTAINS_TCBUFFER: 'TEMPORAL_ACONTAINS_TCBUFFER' | 'temporal_acontains_tcbuffer';
TEMPORAL_ACOVERS_TCBUFFER: 'TEMPORAL_ACOVERS_TCBUFFER' | 'temporal_acovers_tcbuffer';
TEMPORAL_ADISJOINT_TCBUFFER: 'TEMPORAL_ADISJOINT_TCBUFFER' | 'temporal_adisjoint_tcbuffer';
TEMPORAL_AINTERSECTS_TCBUFFER: 'TEMPORAL_AINTERSECTS_TCBUFFER' | 'temporal_aintersects_tcbuffer';
TEMPORAL_ATOUCHES_TCBUFFER: 'TEMPORAL_ATOUCHES_TCBUFFER' | 'temporal_atouches_tcbuffer';
TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER: 'TEMPORAL_ECONTAINS_TCBUFFER_CBUFFER' | 'temporal_econtains_tcbuffer_cbuffer';
TEMPORAL_ECOVERS_TCBUFFER_CBUFFER: 'TEMPORAL_ECOVERS_TCBUFFER_CBUFFER' | 'temporal_ecovers_tcbuffer_cbuffer';
TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER: 'TEMPORAL_EDISJOINT_TCBUFFER_CBUFFER' | 'temporal_edisjoint_tcbuffer_cbuffer';
TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER: 'TEMPORAL_EINTERSECTS_TCBUFFER_CBUFFER' | 'temporal_eintersects_tcbuffer_cbuffer';
TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER: 'TEMPORAL_ETOUCHES_TCBUFFER_CBUFFER' | 'temporal_etouches_tcbuffer_cbuffer';
TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER: 'TEMPORAL_ACONTAINS_TCBUFFER_CBUFFER' | 'temporal_acontains_tcbuffer_cbuffer';
TEMPORAL_ACOVERS_TCBUFFER_CBUFFER: 'TEMPORAL_ACOVERS_TCBUFFER_CBUFFER' | 'temporal_acovers_tcbuffer_cbuffer';
TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER: 'TEMPORAL_ADISJOINT_TCBUFFER_CBUFFER' | 'temporal_adisjoint_tcbuffer_cbuffer';
TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER: 'TEMPORAL_AINTERSECTS_TCBUFFER_CBUFFER' | 'temporal_aintersects_tcbuffer_cbuffer';
TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER: 'TEMPORAL_ATOUCHES_TCBUFFER_CBUFFER' | 'temporal_atouches_tcbuffer_cbuffer';
TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER: 'TEMPORAL_ADISJOINT_TCBUFFER_TCBUFFER' | 'temporal_adisjoint_tcbuffer_tcbuffer';
TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER: 'TEMPORAL_AINTERSECTS_TCBUFFER_TCBUFFER' | 'temporal_aintersects_tcbuffer_tcbuffer';
TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER: 'TEMPORAL_ATOUCHES_TCBUFFER_TCBUFFER' | 'temporal_atouches_tcbuffer_tcbuffer';
TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER: 'TEMPORAL_ECOVERS_TCBUFFER_TCBUFFER' | 'temporal_ecovers_tcbuffer_tcbuffer';
TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER: 'TEMPORAL_EINTERSECTS_TCBUFFER_TCBUFFER' | 'temporal_eintersects_tcbuffer_tcbuffer';
TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER: 'TEMPORAL_ETOUCHES_TCBUFFER_TCBUFFER' | 'temporal_etouches_tcbuffer_tcbuffer';
TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY: 'TEMPORAL_EDWITHIN_TCBUFFER_GEOMETRY' | 'temporal_edwithin_tcbuffer_geometry';
TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY: 'TEMPORAL_ADWITHIN_TCBUFFER_GEOMETRY' | 'temporal_adwithin_tcbuffer_geometry';
TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER: 'TEMPORAL_EDWITHIN_TCBUFFER_CBUFFER' | 'temporal_edwithin_tcbuffer_cbuffer';
TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER: 'TEMPORAL_ADWITHIN_TCBUFFER_CBUFFER' | 'temporal_adwithin_tcbuffer_cbuffer';
TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER: 'TEMPORAL_EDWITHIN_TCBUFFER_TCBUFFER' | 'temporal_edwithin_tcbuffer_tcbuffer';
TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER: 'TEMPORAL_ADWITHIN_TCBUFFER_TCBUFFER' | 'temporal_adwithin_tcbuffer_tcbuffer';
TEMPORAL_ECONTAINS_TPOSE_GEOMETRY: 'TEMPORAL_ECONTAINS_TPOSE_GEOMETRY' | 'temporal_econtains_tpose_geometry';
TEMPORAL_ECOVERS_TPOSE_GEOMETRY: 'TEMPORAL_ECOVERS_TPOSE_GEOMETRY' | 'temporal_ecovers_tpose_geometry';
TEMPORAL_EDISJOINT_TPOSE_GEOMETRY: 'TEMPORAL_EDISJOINT_TPOSE_GEOMETRY' | 'temporal_edisjoint_tpose_geometry';
TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY: 'TEMPORAL_EINTERSECTS_TPOSE_GEOMETRY' | 'temporal_eintersects_tpose_geometry';
TEMPORAL_ETOUCHES_TPOSE_GEOMETRY: 'TEMPORAL_ETOUCHES_TPOSE_GEOMETRY' | 'temporal_etouches_tpose_geometry';
TEMPORAL_ACONTAINS_TPOSE_GEOMETRY: 'TEMPORAL_ACONTAINS_TPOSE_GEOMETRY' | 'temporal_acontains_tpose_geometry';
TEMPORAL_ADISJOINT_TPOSE_GEOMETRY: 'TEMPORAL_ADISJOINT_TPOSE_GEOMETRY' | 'temporal_adisjoint_tpose_geometry';
TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY: 'TEMPORAL_AINTERSECTS_TPOSE_GEOMETRY' | 'temporal_aintersects_tpose_geometry';
TEMPORAL_ATOUCHES_TPOSE_GEOMETRY: 'TEMPORAL_ATOUCHES_TPOSE_GEOMETRY' | 'temporal_atouches_tpose_geometry';
TEMPORAL_ECONTAINS_TPOSE_TPOSE: 'TEMPORAL_ECONTAINS_TPOSE_TPOSE' | 'temporal_econtains_tpose_tpose';
TEMPORAL_ECOVERS_TPOSE_TPOSE: 'TEMPORAL_ECOVERS_TPOSE_TPOSE' | 'temporal_ecovers_tpose_tpose';
TEMPORAL_EDISJOINT_TPOSE_TPOSE: 'TEMPORAL_EDISJOINT_TPOSE_TPOSE' | 'temporal_edisjoint_tpose_tpose';
TEMPORAL_EINTERSECTS_TPOSE_TPOSE: 'TEMPORAL_EINTERSECTS_TPOSE_TPOSE' | 'temporal_eintersects_tpose_tpose';
TEMPORAL_ETOUCHES_TPOSE_TPOSE: 'TEMPORAL_ETOUCHES_TPOSE_TPOSE' | 'temporal_etouches_tpose_tpose';
TEMPORAL_ACONTAINS_TPOSE_TPOSE: 'TEMPORAL_ACONTAINS_TPOSE_TPOSE' | 'temporal_acontains_tpose_tpose';
TEMPORAL_ADISJOINT_TPOSE_TPOSE: 'TEMPORAL_ADISJOINT_TPOSE_TPOSE' | 'temporal_adisjoint_tpose_tpose';
TEMPORAL_AINTERSECTS_TPOSE_TPOSE: 'TEMPORAL_AINTERSECTS_TPOSE_TPOSE' | 'temporal_aintersects_tpose_tpose';
TEMPORAL_ATOUCHES_TPOSE_TPOSE: 'TEMPORAL_ATOUCHES_TPOSE_TPOSE' | 'temporal_atouches_tpose_tpose';
TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY: 'TEMPORAL_ECONTAINS_TNPOINT_GEOMETRY' | 'temporal_econtains_tnpoint_geometry';
TEMPORAL_ECONTAINS_TNPOINT_TNPOINT: 'TEMPORAL_ECONTAINS_TNPOINT_TNPOINT' | 'temporal_econtains_tnpoint_tnpoint';
TEMPORAL_ECOVERS_TNPOINT_GEOMETRY: 'TEMPORAL_ECOVERS_TNPOINT_GEOMETRY' | 'temporal_ecovers_tnpoint_geometry';
TEMPORAL_ECOVERS_TNPOINT_TNPOINT: 'TEMPORAL_ECOVERS_TNPOINT_TNPOINT' | 'temporal_ecovers_tnpoint_tnpoint';
TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY: 'TEMPORAL_EDISJOINT_TNPOINT_GEOMETRY' | 'temporal_edisjoint_tnpoint_geometry';
TEMPORAL_EDISJOINT_TNPOINT_TNPOINT: 'TEMPORAL_EDISJOINT_TNPOINT_TNPOINT' | 'temporal_edisjoint_tnpoint_tnpoint';
TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY: 'TEMPORAL_EINTERSECTS_TNPOINT_GEOMETRY' | 'temporal_eintersects_tnpoint_geometry';
TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT: 'TEMPORAL_EINTERSECTS_TNPOINT_TNPOINT' | 'temporal_eintersects_tnpoint_tnpoint';
TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY: 'TEMPORAL_ETOUCHES_TNPOINT_GEOMETRY' | 'temporal_etouches_tnpoint_geometry';
TEMPORAL_ETOUCHES_TNPOINT_TNPOINT: 'TEMPORAL_ETOUCHES_TNPOINT_TNPOINT' | 'temporal_etouches_tnpoint_tnpoint';
TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY: 'TEMPORAL_ACONTAINS_TNPOINT_GEOMETRY' | 'temporal_acontains_tnpoint_geometry';
TEMPORAL_ACONTAINS_TNPOINT_TNPOINT: 'TEMPORAL_ACONTAINS_TNPOINT_TNPOINT' | 'temporal_acontains_tnpoint_tnpoint';
TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY: 'TEMPORAL_ADISJOINT_TNPOINT_GEOMETRY' | 'temporal_adisjoint_tnpoint_geometry';
TEMPORAL_ADISJOINT_TNPOINT_TNPOINT: 'TEMPORAL_ADISJOINT_TNPOINT_TNPOINT' | 'temporal_adisjoint_tnpoint_tnpoint';
TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY: 'TEMPORAL_AINTERSECTS_TNPOINT_GEOMETRY' | 'temporal_aintersects_tnpoint_geometry';
TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT: 'TEMPORAL_AINTERSECTS_TNPOINT_TNPOINT' | 'temporal_aintersects_tnpoint_tnpoint';
TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY: 'TEMPORAL_ATOUCHES_TNPOINT_GEOMETRY' | 'temporal_atouches_tnpoint_geometry';
TEMPORAL_ATOUCHES_TNPOINT_TNPOINT: 'TEMPORAL_ATOUCHES_TNPOINT_TNPOINT' | 'temporal_atouches_tnpoint_tnpoint';
TEMPORAL_NAD_TPOSE_GEOMETRY: 'TEMPORAL_NAD_TPOSE_GEOMETRY' | 'temporal_nad_tpose_geometry';
TEMPORAL_NAD_TPOSE_TPOSE: 'TEMPORAL_NAD_TPOSE_TPOSE' | 'temporal_nad_tpose_tpose';
TEMPORAL_NAD_TNPOINT_GEOMETRY: 'TEMPORAL_NAD_TNPOINT_GEOMETRY' | 'temporal_nad_tnpoint_geometry';
TEMPORAL_NAD_TNPOINT_TNPOINT: 'TEMPORAL_NAD_TNPOINT_TNPOINT' | 'temporal_nad_tnpoint_tnpoint';
TEMPORAL_EDWITHIN_TPOSE_GEOMETRY: 'TEMPORAL_EDWITHIN_TPOSE_GEOMETRY' | 'temporal_edwithin_tpose_geometry';
TEMPORAL_EDWITHIN_TPOSE_TPOSE: 'TEMPORAL_EDWITHIN_TPOSE_TPOSE' | 'temporal_edwithin_tpose_tpose';
TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY: 'TEMPORAL_EDWITHIN_TNPOINT_GEOMETRY' | 'temporal_edwithin_tnpoint_geometry';
TEMPORAL_EDWITHIN_TNPOINT_TNPOINT: 'TEMPORAL_EDWITHIN_TNPOINT_TNPOINT' | 'temporal_edwithin_tnpoint_tnpoint';
TEMPORAL_ADWITHIN_TPOSE_GEOMETRY: 'TEMPORAL_ADWITHIN_TPOSE_GEOMETRY' | 'temporal_adwithin_tpose_geometry';
TEMPORAL_ADWITHIN_TPOSE_TPOSE: 'TEMPORAL_ADWITHIN_TPOSE_TPOSE' | 'temporal_adwithin_tpose_tpose';
TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY: 'TEMPORAL_ADWITHIN_TNPOINT_GEOMETRY' | 'temporal_adwithin_tnpoint_geometry';
TEMPORAL_ADWITHIN_TNPOINT_TNPOINT: 'TEMPORAL_ADWITHIN_TNPOINT_TNPOINT' | 'temporal_adwithin_tnpoint_tnpoint';
TEMPORAL_NAD_TCBUFFER: 'TEMPORAL_NAD_TCBUFFER' | 'temporal_nad_tcbuffer';
TEMPORAL_NAD_TCBUFFER_CBUFFER: 'TEMPORAL_NAD_TCBUFFER_CBUFFER' | 'temporal_nad_tcbuffer_cbuffer';
TEMPORAL_NAD_TCBUFFER_TCBUFFER: 'TEMPORAL_NAD_TCBUFFER_TCBUFFER' | 'temporal_nad_tcbuffer_tcbuffer';
ALWAYS_EQ_TFLOAT_FLOAT: 'ALWAYS_EQ_TFLOAT_FLOAT' | 'always_eq_tfloat_float';
ALWAYS_EQ_TINT_INT: 'ALWAYS_EQ_TINT_INT' | 'always_eq_tint_int';
ALWAYS_GE_TFLOAT_FLOAT: 'ALWAYS_GE_TFLOAT_FLOAT' | 'always_ge_tfloat_float';
ALWAYS_GE_TINT_INT: 'ALWAYS_GE_TINT_INT' | 'always_ge_tint_int';
ALWAYS_GT_TFLOAT_FLOAT: 'ALWAYS_GT_TFLOAT_FLOAT' | 'always_gt_tfloat_float';
ALWAYS_GT_TINT_INT: 'ALWAYS_GT_TINT_INT' | 'always_gt_tint_int';
ALWAYS_LE_TFLOAT_FLOAT: 'ALWAYS_LE_TFLOAT_FLOAT' | 'always_le_tfloat_float';
ALWAYS_LE_TINT_INT: 'ALWAYS_LE_TINT_INT' | 'always_le_tint_int';
ALWAYS_LT_TFLOAT_FLOAT: 'ALWAYS_LT_TFLOAT_FLOAT' | 'always_lt_tfloat_float';
ALWAYS_LT_TINT_INT: 'ALWAYS_LT_TINT_INT' | 'always_lt_tint_int';
ALWAYS_NE_TFLOAT_FLOAT: 'ALWAYS_NE_TFLOAT_FLOAT' | 'always_ne_tfloat_float';
ALWAYS_NE_TINT_INT: 'ALWAYS_NE_TINT_INT' | 'always_ne_tint_int';
EVER_EQ_TFLOAT_FLOAT: 'EVER_EQ_TFLOAT_FLOAT' | 'ever_eq_tfloat_float';
EVER_EQ_TINT_INT: 'EVER_EQ_TINT_INT' | 'ever_eq_tint_int';
EVER_GE_TFLOAT_FLOAT: 'EVER_GE_TFLOAT_FLOAT' | 'ever_ge_tfloat_float';
EVER_GE_TINT_INT: 'EVER_GE_TINT_INT' | 'ever_ge_tint_int';
EVER_GT_TFLOAT_FLOAT: 'EVER_GT_TFLOAT_FLOAT' | 'ever_gt_tfloat_float';
EVER_GT_TINT_INT: 'EVER_GT_TINT_INT' | 'ever_gt_tint_int';
EVER_LE_TFLOAT_FLOAT: 'EVER_LE_TFLOAT_FLOAT' | 'ever_le_tfloat_float';
EVER_LE_TINT_INT: 'EVER_LE_TINT_INT' | 'ever_le_tint_int';
EVER_LT_TFLOAT_FLOAT: 'EVER_LT_TFLOAT_FLOAT' | 'ever_lt_tfloat_float';
EVER_LT_TINT_INT: 'EVER_LT_TINT_INT' | 'ever_lt_tint_int';
EVER_NE_TFLOAT_FLOAT: 'EVER_NE_TFLOAT_FLOAT' | 'ever_ne_tfloat_float';
EVER_NE_TINT_INT: 'EVER_NE_TINT_INT' | 'ever_ne_tint_int';
ALWAYS_EQ_FLOAT_TFLOAT: 'ALWAYS_EQ_FLOAT_TFLOAT' | 'always_eq_float_tfloat';
ALWAYS_EQ_INT_TINT: 'ALWAYS_EQ_INT_TINT' | 'always_eq_int_tint';
ALWAYS_EQ_TEMPORAL_TEMPORAL: 'ALWAYS_EQ_TEMPORAL_TEMPORAL' | 'always_eq_temporal_temporal';
ALWAYS_GE_FLOAT_TFLOAT: 'ALWAYS_GE_FLOAT_TFLOAT' | 'always_ge_float_tfloat';
ALWAYS_GE_INT_TINT: 'ALWAYS_GE_INT_TINT' | 'always_ge_int_tint';
ALWAYS_GE_TEMPORAL_TEMPORAL: 'ALWAYS_GE_TEMPORAL_TEMPORAL' | 'always_ge_temporal_temporal';
ALWAYS_GT_FLOAT_TFLOAT: 'ALWAYS_GT_FLOAT_TFLOAT' | 'always_gt_float_tfloat';
ALWAYS_GT_INT_TINT: 'ALWAYS_GT_INT_TINT' | 'always_gt_int_tint';
ALWAYS_GT_TEMPORAL_TEMPORAL: 'ALWAYS_GT_TEMPORAL_TEMPORAL' | 'always_gt_temporal_temporal';
ALWAYS_LE_FLOAT_TFLOAT: 'ALWAYS_LE_FLOAT_TFLOAT' | 'always_le_float_tfloat';
ALWAYS_LE_INT_TINT: 'ALWAYS_LE_INT_TINT' | 'always_le_int_tint';
ALWAYS_LE_TEMPORAL_TEMPORAL: 'ALWAYS_LE_TEMPORAL_TEMPORAL' | 'always_le_temporal_temporal';
ALWAYS_LT_FLOAT_TFLOAT: 'ALWAYS_LT_FLOAT_TFLOAT' | 'always_lt_float_tfloat';
ALWAYS_LT_INT_TINT: 'ALWAYS_LT_INT_TINT' | 'always_lt_int_tint';
ALWAYS_LT_TEMPORAL_TEMPORAL: 'ALWAYS_LT_TEMPORAL_TEMPORAL' | 'always_lt_temporal_temporal';
ALWAYS_NE_FLOAT_TFLOAT: 'ALWAYS_NE_FLOAT_TFLOAT' | 'always_ne_float_tfloat';
ALWAYS_NE_INT_TINT: 'ALWAYS_NE_INT_TINT' | 'always_ne_int_tint';
ALWAYS_NE_TEMPORAL_TEMPORAL: 'ALWAYS_NE_TEMPORAL_TEMPORAL' | 'always_ne_temporal_temporal';
EVER_EQ_FLOAT_TFLOAT: 'EVER_EQ_FLOAT_TFLOAT' | 'ever_eq_float_tfloat';
EVER_EQ_INT_TINT: 'EVER_EQ_INT_TINT' | 'ever_eq_int_tint';
EVER_EQ_TEMPORAL_TEMPORAL: 'EVER_EQ_TEMPORAL_TEMPORAL' | 'ever_eq_temporal_temporal';
EVER_GE_FLOAT_TFLOAT: 'EVER_GE_FLOAT_TFLOAT' | 'ever_ge_float_tfloat';
EVER_GE_INT_TINT: 'EVER_GE_INT_TINT' | 'ever_ge_int_tint';
EVER_GE_TEMPORAL_TEMPORAL: 'EVER_GE_TEMPORAL_TEMPORAL' | 'ever_ge_temporal_temporal';
EVER_GT_FLOAT_TFLOAT: 'EVER_GT_FLOAT_TFLOAT' | 'ever_gt_float_tfloat';
EVER_GT_INT_TINT: 'EVER_GT_INT_TINT' | 'ever_gt_int_tint';
EVER_GT_TEMPORAL_TEMPORAL: 'EVER_GT_TEMPORAL_TEMPORAL' | 'ever_gt_temporal_temporal';
EVER_LE_FLOAT_TFLOAT: 'EVER_LE_FLOAT_TFLOAT' | 'ever_le_float_tfloat';
EVER_LE_INT_TINT: 'EVER_LE_INT_TINT' | 'ever_le_int_tint';
EVER_LE_TEMPORAL_TEMPORAL: 'EVER_LE_TEMPORAL_TEMPORAL' | 'ever_le_temporal_temporal';
EVER_LT_FLOAT_TFLOAT: 'EVER_LT_FLOAT_TFLOAT' | 'ever_lt_float_tfloat';
EVER_LT_INT_TINT: 'EVER_LT_INT_TINT' | 'ever_lt_int_tint';
EVER_LT_TEMPORAL_TEMPORAL: 'EVER_LT_TEMPORAL_TEMPORAL' | 'ever_lt_temporal_temporal';
EVER_NE_FLOAT_TFLOAT: 'EVER_NE_FLOAT_TFLOAT' | 'ever_ne_float_tfloat';
EVER_NE_INT_TINT: 'EVER_NE_INT_TINT' | 'ever_ne_int_tint';
EVER_NE_TEMPORAL_TEMPORAL: 'EVER_NE_TEMPORAL_TEMPORAL' | 'ever_ne_temporal_temporal';
ALWAYS_EQ_TCBUFFER_CBUFFER: 'ALWAYS_EQ_TCBUFFER_CBUFFER' | 'always_eq_tcbuffer_cbuffer';
ALWAYS_EQ_TCBUFFER_TCBUFFER: 'ALWAYS_EQ_TCBUFFER_TCBUFFER' | 'always_eq_tcbuffer_tcbuffer';
ALWAYS_EQ_TGEO_GEO: 'ALWAYS_EQ_TGEO_GEO' | 'always_eq_tgeo_geo';
ALWAYS_EQ_TGEO_TGEO: 'ALWAYS_EQ_TGEO_TGEO' | 'always_eq_tgeo_tgeo';
ALWAYS_NE_TCBUFFER_CBUFFER: 'ALWAYS_NE_TCBUFFER_CBUFFER' | 'always_ne_tcbuffer_cbuffer';
ALWAYS_NE_TCBUFFER_TCBUFFER: 'ALWAYS_NE_TCBUFFER_TCBUFFER' | 'always_ne_tcbuffer_tcbuffer';
ALWAYS_NE_TGEO_GEO: 'ALWAYS_NE_TGEO_GEO' | 'always_ne_tgeo_geo';
ALWAYS_NE_TGEO_TGEO: 'ALWAYS_NE_TGEO_TGEO' | 'always_ne_tgeo_tgeo';
ATOUCHES_TPOINT_GEO: 'ATOUCHES_TPOINT_GEO' | 'atouches_tpoint_geo';
ETOUCHES_TPOINT_GEO: 'ETOUCHES_TPOINT_GEO' | 'etouches_tpoint_geo';
EVER_EQ_TCBUFFER_CBUFFER: 'EVER_EQ_TCBUFFER_CBUFFER' | 'ever_eq_tcbuffer_cbuffer';
EVER_EQ_TCBUFFER_TCBUFFER: 'EVER_EQ_TCBUFFER_TCBUFFER' | 'ever_eq_tcbuffer_tcbuffer';
EVER_EQ_TGEO_GEO: 'EVER_EQ_TGEO_GEO' | 'ever_eq_tgeo_geo';
EVER_EQ_TGEO_TGEO: 'EVER_EQ_TGEO_TGEO' | 'ever_eq_tgeo_tgeo';
EVER_NE_TCBUFFER_CBUFFER: 'EVER_NE_TCBUFFER_CBUFFER' | 'ever_ne_tcbuffer_cbuffer';
EVER_NE_TCBUFFER_TCBUFFER: 'EVER_NE_TCBUFFER_TCBUFFER' | 'ever_ne_tcbuffer_tcbuffer';
EVER_NE_TGEO_GEO: 'EVER_NE_TGEO_GEO' | 'ever_ne_tgeo_geo';
EVER_NE_TGEO_TGEO: 'EVER_NE_TGEO_TGEO' | 'ever_ne_tgeo_tgeo';
ABOVE_TSPATIAL_TSPATIAL: 'ABOVE_TSPATIAL_TSPATIAL' | 'above_tspatial_tspatial';
ADJACENT_TEMPORAL_TEMPORAL: 'ADJACENT_TEMPORAL_TEMPORAL' | 'adjacent_temporal_temporal';
ADJACENT_TSPATIAL_TSPATIAL: 'ADJACENT_TSPATIAL_TSPATIAL' | 'adjacent_tspatial_tspatial';
AFTER_TEMPORAL_TEMPORAL: 'AFTER_TEMPORAL_TEMPORAL' | 'after_temporal_temporal';
AFTER_TSPATIAL_TSPATIAL: 'AFTER_TSPATIAL_TSPATIAL' | 'after_tspatial_tspatial';
ALWAYS_EQ_TBOOL_BOOL: 'ALWAYS_EQ_TBOOL_BOOL' | 'always_eq_tbool_bool';
ALWAYS_NE_TBOOL_BOOL: 'ALWAYS_NE_TBOOL_BOOL' | 'always_ne_tbool_bool';
BACK_TSPATIAL_TSPATIAL: 'BACK_TSPATIAL_TSPATIAL' | 'back_tspatial_tspatial';
BEFORE_TEMPORAL_TEMPORAL: 'BEFORE_TEMPORAL_TEMPORAL' | 'before_temporal_temporal';
BEFORE_TSPATIAL_TSPATIAL: 'BEFORE_TSPATIAL_TSPATIAL' | 'before_tspatial_tspatial';
BELOW_TSPATIAL_TSPATIAL: 'BELOW_TSPATIAL_TSPATIAL' | 'below_tspatial_tspatial';
CONTAINED_TEMPORAL_TEMPORAL: 'CONTAINED_TEMPORAL_TEMPORAL' | 'contained_temporal_temporal';
CONTAINED_TSPATIAL_TSPATIAL: 'CONTAINED_TSPATIAL_TSPATIAL' | 'contained_tspatial_tspatial';
CONTAINS_TEMPORAL_TEMPORAL: 'CONTAINS_TEMPORAL_TEMPORAL' | 'contains_temporal_temporal';
CONTAINS_TSPATIAL_TSPATIAL: 'CONTAINS_TSPATIAL_TSPATIAL' | 'contains_tspatial_tspatial';
EVER_EQ_TBOOL_BOOL: 'EVER_EQ_TBOOL_BOOL' | 'ever_eq_tbool_bool';
EVER_NE_TBOOL_BOOL: 'EVER_NE_TBOOL_BOOL' | 'ever_ne_tbool_bool';
FRONT_TSPATIAL_TSPATIAL: 'FRONT_TSPATIAL_TSPATIAL' | 'front_tspatial_tspatial';
LEFT_TSPATIAL_TSPATIAL: 'LEFT_TSPATIAL_TSPATIAL' | 'left_tspatial_tspatial';
NAD_TNPOINT_GEO: 'NAD_TNPOINT_GEO' | 'nad_tnpoint_geo';
NAD_TPOSE_GEO: 'NAD_TPOSE_GEO' | 'nad_tpose_geo';
OVERABOVE_TSPATIAL_TSPATIAL: 'OVERABOVE_TSPATIAL_TSPATIAL' | 'overabove_tspatial_tspatial';
OVERAFTER_TEMPORAL_TEMPORAL: 'OVERAFTER_TEMPORAL_TEMPORAL' | 'overafter_temporal_temporal';
OVERAFTER_TSPATIAL_TSPATIAL: 'OVERAFTER_TSPATIAL_TSPATIAL' | 'overafter_tspatial_tspatial';
OVERBACK_TSPATIAL_TSPATIAL: 'OVERBACK_TSPATIAL_TSPATIAL' | 'overback_tspatial_tspatial';
OVERBEFORE_TEMPORAL_TEMPORAL: 'OVERBEFORE_TEMPORAL_TEMPORAL' | 'overbefore_temporal_temporal';
OVERBEFORE_TSPATIAL_TSPATIAL: 'OVERBEFORE_TSPATIAL_TSPATIAL' | 'overbefore_tspatial_tspatial';
OVERBELOW_TSPATIAL_TSPATIAL: 'OVERBELOW_TSPATIAL_TSPATIAL' | 'overbelow_tspatial_tspatial';
OVERFRONT_TSPATIAL_TSPATIAL: 'OVERFRONT_TSPATIAL_TSPATIAL' | 'overfront_tspatial_tspatial';
OVERLAPS_TEMPORAL_TEMPORAL: 'OVERLAPS_TEMPORAL_TEMPORAL' | 'overlaps_temporal_temporal';
OVERLAPS_TSPATIAL_TSPATIAL: 'OVERLAPS_TSPATIAL_TSPATIAL' | 'overlaps_tspatial_tspatial';
OVERLEFT_TSPATIAL_TSPATIAL: 'OVERLEFT_TSPATIAL_TSPATIAL' | 'overleft_tspatial_tspatial';
OVERRIGHT_TSPATIAL_TSPATIAL: 'OVERRIGHT_TSPATIAL_TSPATIAL' | 'overright_tspatial_tspatial';
RIGHT_TSPATIAL_TSPATIAL: 'RIGHT_TSPATIAL_TSPATIAL' | 'right_tspatial_tspatial';
SAME_TEMPORAL_TEMPORAL: 'SAME_TEMPORAL_TEMPORAL' | 'same_temporal_temporal';
SAME_TSPATIAL_TSPATIAL: 'SAME_TSPATIAL_TSPATIAL' | 'same_tspatial_tspatial';
TBOOL_END_VALUE: 'TBOOL_END_VALUE' | 'tbool_end_value';
TBOOL_START_VALUE: 'TBOOL_START_VALUE' | 'tbool_start_value';
TEMPORAL_CMP: 'TEMPORAL_CMP' | 'temporal_cmp';
TEMPORAL_DYNTIMEWARP_DISTANCE: 'TEMPORAL_DYNTIMEWARP_DISTANCE' | 'temporal_dyntimewarp_distance';
TEMPORAL_EQ: 'TEMPORAL_EQ' | 'temporal_eq';
TEMPORAL_FRECHET_DISTANCE: 'TEMPORAL_FRECHET_DISTANCE' | 'temporal_frechet_distance';
TEMPORAL_GE: 'TEMPORAL_GE' | 'temporal_ge';
TEMPORAL_GT: 'TEMPORAL_GT' | 'temporal_gt';
TEMPORAL_HAUSDORFF_DISTANCE: 'TEMPORAL_HAUSDORFF_DISTANCE' | 'temporal_hausdorff_distance';
TEMPORAL_LE: 'TEMPORAL_LE' | 'temporal_le';
TEMPORAL_LT: 'TEMPORAL_LT' | 'temporal_lt';
TEMPORAL_NE: 'TEMPORAL_NE' | 'temporal_ne';
TNPOINT_LENGTH: 'TNPOINT_LENGTH' | 'tnpoint_length';
TBOOL_TO_TINT: 'TBOOL_TO_TINT' | 'tbool_to_tint';
TCBUFFER_TO_TFLOAT: 'TCBUFFER_TO_TFLOAT' | 'tcbuffer_to_tfloat';
TFLOAT_CEIL: 'TFLOAT_CEIL' | 'tfloat_ceil';
TFLOAT_EXP: 'TFLOAT_EXP' | 'tfloat_exp';
TFLOAT_FLOOR: 'TFLOAT_FLOOR' | 'tfloat_floor';
TFLOAT_LN: 'TFLOAT_LN' | 'tfloat_ln';
TFLOAT_LOG10: 'TFLOAT_LOG10' | 'tfloat_log10';
TFLOAT_RADIANS: 'TFLOAT_RADIANS' | 'tfloat_radians';
TFLOAT_TO_TINT: 'TFLOAT_TO_TINT' | 'tfloat_to_tint';
TINT_TO_TFLOAT: 'TINT_TO_TFLOAT' | 'tint_to_tfloat';
ADJACENT_TNUMBER_TBOX: 'ADJACENT_TNUMBER_TBOX' | 'adjacent_tnumber_tbox';
AFTER_TNUMBER_TBOX: 'AFTER_TNUMBER_TBOX' | 'after_tnumber_tbox';
BEFORE_TNUMBER_TBOX: 'BEFORE_TNUMBER_TBOX' | 'before_tnumber_tbox';
CONTAINED_TNUMBER_TBOX: 'CONTAINED_TNUMBER_TBOX' | 'contained_tnumber_tbox';
CONTAINS_TNUMBER_TBOX: 'CONTAINS_TNUMBER_TBOX' | 'contains_tnumber_tbox';
LEFT_TNUMBER_TBOX: 'LEFT_TNUMBER_TBOX' | 'left_tnumber_tbox';
NAD_TCBUFFER_STBOX: 'NAD_TCBUFFER_STBOX' | 'nad_tcbuffer_stbox';
NAD_TFLOAT_TBOX: 'NAD_TFLOAT_TBOX' | 'nad_tfloat_tbox';
NAD_TGEO_STBOX: 'NAD_TGEO_STBOX' | 'nad_tgeo_stbox';
NAD_TINT_TBOX: 'NAD_TINT_TBOX' | 'nad_tint_tbox';
NAD_TNPOINT_STBOX: 'NAD_TNPOINT_STBOX' | 'nad_tnpoint_stbox';
NAD_TPOSE_STBOX: 'NAD_TPOSE_STBOX' | 'nad_tpose_stbox';
OVERAFTER_TNUMBER_TBOX: 'OVERAFTER_TNUMBER_TBOX' | 'overafter_tnumber_tbox';
OVERBEFORE_TNUMBER_TBOX: 'OVERBEFORE_TNUMBER_TBOX' | 'overbefore_tnumber_tbox';
OVERLAPS_TNUMBER_TBOX: 'OVERLAPS_TNUMBER_TBOX' | 'overlaps_tnumber_tbox';
OVERLEFT_TNUMBER_TBOX: 'OVERLEFT_TNUMBER_TBOX' | 'overleft_tnumber_tbox';
OVERRIGHT_TNUMBER_TBOX: 'OVERRIGHT_TNUMBER_TBOX' | 'overright_tnumber_tbox';
RIGHT_TNUMBER_TBOX: 'RIGHT_TNUMBER_TBOX' | 'right_tnumber_tbox';
SAME_TNUMBER_TBOX: 'SAME_TNUMBER_TBOX' | 'same_tnumber_tbox';
ABOVE_STBOX_TSPATIAL: 'ABOVE_STBOX_TSPATIAL' | 'above_stbox_tspatial';
ABOVE_TSPATIAL_STBOX: 'ABOVE_TSPATIAL_STBOX' | 'above_tspatial_stbox';
ADJACENT_STBOX_TSPATIAL: 'ADJACENT_STBOX_TSPATIAL' | 'adjacent_stbox_tspatial';
ADJACENT_TBOX_TNUMBER: 'ADJACENT_TBOX_TNUMBER' | 'adjacent_tbox_tnumber';
ADJACENT_TSPATIAL_STBOX: 'ADJACENT_TSPATIAL_STBOX' | 'adjacent_tspatial_stbox';
AFTER_STBOX_TSPATIAL: 'AFTER_STBOX_TSPATIAL' | 'after_stbox_tspatial';
AFTER_TBOX_TNUMBER: 'AFTER_TBOX_TNUMBER' | 'after_tbox_tnumber';
AFTER_TSPATIAL_STBOX: 'AFTER_TSPATIAL_STBOX' | 'after_tspatial_stbox';
BACK_STBOX_TSPATIAL: 'BACK_STBOX_TSPATIAL' | 'back_stbox_tspatial';
BACK_TSPATIAL_STBOX: 'BACK_TSPATIAL_STBOX' | 'back_tspatial_stbox';
BEFORE_STBOX_TSPATIAL: 'BEFORE_STBOX_TSPATIAL' | 'before_stbox_tspatial';
BEFORE_TBOX_TNUMBER: 'BEFORE_TBOX_TNUMBER' | 'before_tbox_tnumber';
BEFORE_TSPATIAL_STBOX: 'BEFORE_TSPATIAL_STBOX' | 'before_tspatial_stbox';
BELOW_STBOX_TSPATIAL: 'BELOW_STBOX_TSPATIAL' | 'below_stbox_tspatial';
BELOW_TSPATIAL_STBOX: 'BELOW_TSPATIAL_STBOX' | 'below_tspatial_stbox';
CONTAINED_STBOX_TSPATIAL: 'CONTAINED_STBOX_TSPATIAL' | 'contained_stbox_tspatial';
CONTAINED_TBOX_TNUMBER: 'CONTAINED_TBOX_TNUMBER' | 'contained_tbox_tnumber';
CONTAINED_TSPATIAL_STBOX: 'CONTAINED_TSPATIAL_STBOX' | 'contained_tspatial_stbox';
CONTAINS_STBOX_TSPATIAL: 'CONTAINS_STBOX_TSPATIAL' | 'contains_stbox_tspatial';
CONTAINS_TBOX_TNUMBER: 'CONTAINS_TBOX_TNUMBER' | 'contains_tbox_tnumber';
CONTAINS_TSPATIAL_STBOX: 'CONTAINS_TSPATIAL_STBOX' | 'contains_tspatial_stbox';
FRONT_STBOX_TSPATIAL: 'FRONT_STBOX_TSPATIAL' | 'front_stbox_tspatial';
FRONT_TSPATIAL_STBOX: 'FRONT_TSPATIAL_STBOX' | 'front_tspatial_stbox';
LEFT_STBOX_TSPATIAL: 'LEFT_STBOX_TSPATIAL' | 'left_stbox_tspatial';
LEFT_TBOX_TNUMBER: 'LEFT_TBOX_TNUMBER' | 'left_tbox_tnumber';
LEFT_TSPATIAL_STBOX: 'LEFT_TSPATIAL_STBOX' | 'left_tspatial_stbox';
OVERABOVE_STBOX_TSPATIAL: 'OVERABOVE_STBOX_TSPATIAL' | 'overabove_stbox_tspatial';
OVERABOVE_TSPATIAL_STBOX: 'OVERABOVE_TSPATIAL_STBOX' | 'overabove_tspatial_stbox';
OVERAFTER_STBOX_TSPATIAL: 'OVERAFTER_STBOX_TSPATIAL' | 'overafter_stbox_tspatial';
OVERAFTER_TBOX_TNUMBER: 'OVERAFTER_TBOX_TNUMBER' | 'overafter_tbox_tnumber';
OVERAFTER_TSPATIAL_STBOX: 'OVERAFTER_TSPATIAL_STBOX' | 'overafter_tspatial_stbox';
OVERBACK_STBOX_TSPATIAL: 'OVERBACK_STBOX_TSPATIAL' | 'overback_stbox_tspatial';
OVERBACK_TSPATIAL_STBOX: 'OVERBACK_TSPATIAL_STBOX' | 'overback_tspatial_stbox';
OVERBEFORE_STBOX_TSPATIAL: 'OVERBEFORE_STBOX_TSPATIAL' | 'overbefore_stbox_tspatial';
OVERBEFORE_TBOX_TNUMBER: 'OVERBEFORE_TBOX_TNUMBER' | 'overbefore_tbox_tnumber';
OVERBEFORE_TSPATIAL_STBOX: 'OVERBEFORE_TSPATIAL_STBOX' | 'overbefore_tspatial_stbox';
OVERBELOW_STBOX_TSPATIAL: 'OVERBELOW_STBOX_TSPATIAL' | 'overbelow_stbox_tspatial';
OVERBELOW_TSPATIAL_STBOX: 'OVERBELOW_TSPATIAL_STBOX' | 'overbelow_tspatial_stbox';
OVERFRONT_STBOX_TSPATIAL: 'OVERFRONT_STBOX_TSPATIAL' | 'overfront_stbox_tspatial';
OVERFRONT_TSPATIAL_STBOX: 'OVERFRONT_TSPATIAL_STBOX' | 'overfront_tspatial_stbox';
OVERLAPS_STBOX_TSPATIAL: 'OVERLAPS_STBOX_TSPATIAL' | 'overlaps_stbox_tspatial';
OVERLAPS_TBOX_TNUMBER: 'OVERLAPS_TBOX_TNUMBER' | 'overlaps_tbox_tnumber';
OVERLAPS_TSPATIAL_STBOX: 'OVERLAPS_TSPATIAL_STBOX' | 'overlaps_tspatial_stbox';
OVERLEFT_STBOX_TSPATIAL: 'OVERLEFT_STBOX_TSPATIAL' | 'overleft_stbox_tspatial';
OVERLEFT_TBOX_TNUMBER: 'OVERLEFT_TBOX_TNUMBER' | 'overleft_tbox_tnumber';
OVERLEFT_TSPATIAL_STBOX: 'OVERLEFT_TSPATIAL_STBOX' | 'overleft_tspatial_stbox';
OVERRIGHT_STBOX_TSPATIAL: 'OVERRIGHT_STBOX_TSPATIAL' | 'overright_stbox_tspatial';
OVERRIGHT_TBOX_TNUMBER: 'OVERRIGHT_TBOX_TNUMBER' | 'overright_tbox_tnumber';
OVERRIGHT_TSPATIAL_STBOX: 'OVERRIGHT_TSPATIAL_STBOX' | 'overright_tspatial_stbox';
RIGHT_STBOX_TSPATIAL: 'RIGHT_STBOX_TSPATIAL' | 'right_stbox_tspatial';
RIGHT_TBOX_TNUMBER: 'RIGHT_TBOX_TNUMBER' | 'right_tbox_tnumber';
RIGHT_TSPATIAL_STBOX: 'RIGHT_TSPATIAL_STBOX' | 'right_tspatial_stbox';
SAME_STBOX_TSPATIAL: 'SAME_STBOX_TSPATIAL' | 'same_stbox_tspatial';
SAME_TBOX_TNUMBER: 'SAME_TBOX_TNUMBER' | 'same_tbox_tnumber';
SAME_TSPATIAL_STBOX: 'SAME_TSPATIAL_STBOX' | 'same_tspatial_stbox';
/* END CODEGEN LEXER TOKENS */
/* BEGIN CODEGEN AGGREGATION LEXER TOKENS */
TEMPORAL_NUM_INSTANTS: 'TEMPORAL_NUM_INSTANTS' | 'temporal_num_instants';
TEMPORAL_NUM_SEQUENCES: 'TEMPORAL_NUM_SEQUENCES' | 'temporal_num_sequences';
TEMPORAL_NUM_TIMESTAMPS: 'TEMPORAL_NUM_TIMESTAMPS' | 'temporal_num_timestamps';
TEMPORAL_TFLOAT_START_VALUE: 'TEMPORAL_TFLOAT_START_VALUE' | 'temporal_tfloat_start_value';
TEMPORAL_TFLOAT_END_VALUE: 'TEMPORAL_TFLOAT_END_VALUE' | 'temporal_tfloat_end_value';
TEMPORAL_TFLOAT_MIN_VALUE: 'TEMPORAL_TFLOAT_MIN_VALUE' | 'temporal_tfloat_min_value';
TEMPORAL_TFLOAT_MAX_VALUE: 'TEMPORAL_TFLOAT_MAX_VALUE' | 'temporal_tfloat_max_value';
TEMPORAL_TNUMBER_INTEGRAL: 'TEMPORAL_TNUMBER_INTEGRAL' | 'temporal_tnumber_integral';
TEMPORAL_TINT_START_VALUE: 'TEMPORAL_TINT_START_VALUE' | 'temporal_tint_start_value';
TEMPORAL_TINT_END_VALUE: 'TEMPORAL_TINT_END_VALUE' | 'temporal_tint_end_value';
TEMPORAL_TINT_MIN_VALUE: 'TEMPORAL_TINT_MIN_VALUE' | 'temporal_tint_min_value';
TEMPORAL_TINT_MAX_VALUE: 'TEMPORAL_TINT_MAX_VALUE' | 'temporal_tint_max_value';
TEMPORAL_TFLOAT_AVG_VALUE: 'TEMPORAL_TFLOAT_AVG_VALUE' | 'temporal_tfloat_avg_value';
TEMPORAL_TNUMBER_TWAVG: 'TEMPORAL_TNUMBER_TWAVG' | 'temporal_tnumber_twavg';
TEMPORAL_TINT_AVG_VALUE: 'TEMPORAL_TINT_AVG_VALUE' | 'temporal_tint_avg_value';
TEMPORAL_START_TIMESTAMP: 'TEMPORAL_START_TIMESTAMP' | 'temporal_start_timestamp';
TEMPORAL_END_TIMESTAMP: 'TEMPORAL_END_TIMESTAMP' | 'temporal_end_timestamp';
TEMPORAL_LOWER_INC: 'TEMPORAL_LOWER_INC' | 'temporal_lower_inc';
TEMPORAL_UPPER_INC: 'TEMPORAL_UPPER_INC' | 'temporal_upper_inc';
TEMPORAL_TPOINT_IS_SIMPLE: 'TEMPORAL_TPOINT_IS_SIMPLE' | 'temporal_tpoint_is_simple';
TSPATIAL_EXTENT: 'TSPATIAL_EXTENT' | 'tspatial_extent';
TNUMBER_EXTENT: 'TNUMBER_EXTENT' | 'tnumber_extent';
FLOAT_EXTENT: 'FLOAT_EXTENT' | 'float_extent';
INT_EXTENT: 'INT_EXTENT' | 'int_extent';
BIGINT_EXTENT: 'BIGINT_EXTENT' | 'bigint_extent';
TIMESTAMPTZ_EXTENT: 'TIMESTAMPTZ_EXTENT' | 'timestamptz_extent';
FLOAT_UNION: 'FLOAT_UNION' | 'float_union';
INT_UNION: 'INT_UNION' | 'int_union';
BIGINT_UNION: 'BIGINT_UNION' | 'bigint_union';
TIMESTAMPTZ_UNION: 'TIMESTAMPTZ_UNION' | 'timestamptz_union';
/* END CODEGEN AGGREGATION LEXER TOKENS */
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
