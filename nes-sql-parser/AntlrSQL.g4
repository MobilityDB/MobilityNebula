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

functionName:  IDENTIFIER | AVG | MAX | MIN | SUM | COUNT | MEDIAN | ARRAY_AGG | VAR | TEMPORAL_SEQUENCE | TEMPORAL_EINTERSECTS_GEOMETRY | TEMPORAL_AINTERSECTS_GEOMETRY | TEMPORAL_ECONTAINS_GEOMETRY | ALWAYS_EQ_BIGINT_TBIGINT | ALWAYS_GE_BIGINT_TBIGINT | ALWAYS_GT_BIGINT_TBIGINT | ALWAYS_LE_BIGINT_TBIGINT | ALWAYS_LT_BIGINT_TBIGINT | ALWAYS_NE_BIGINT_TBIGINT | ALWAYS_EQ_TBIGINT_BIGINT | ALWAYS_GE_TBIGINT_BIGINT | ALWAYS_GT_TBIGINT_BIGINT | ALWAYS_LE_TBIGINT_BIGINT | ALWAYS_LT_TBIGINT_BIGINT | ALWAYS_NE_TBIGINT_BIGINT | ALWAYS_EQ_TBIGINT_TBIGINT | ALWAYS_GE_TBIGINT_TBIGINT | ALWAYS_GT_TBIGINT_TBIGINT | ALWAYS_LE_TBIGINT_TBIGINT | ALWAYS_LT_TBIGINT_TBIGINT | ALWAYS_NE_TBIGINT_TBIGINT | ALWAYS_EQ_BOOL_TBOOL | ALWAYS_NE_BOOL_TBOOL | ALWAYS_EQ_FLOAT_TFLOAT | ALWAYS_GE_FLOAT_TFLOAT | ALWAYS_GT_FLOAT_TFLOAT | ALWAYS_LE_FLOAT_TFLOAT | ALWAYS_LT_FLOAT_TFLOAT | ALWAYS_NE_FLOAT_TFLOAT | ALWAYS_EQ_TBOOL_BOOL | ALWAYS_NE_TBOOL_BOOL | ALWAYS_EQ_TEMPORAL_TEMPORAL | ALWAYS_GE_TEMPORAL_TEMPORAL | ALWAYS_GT_TEMPORAL_TEMPORAL | ALWAYS_LE_TEMPORAL_TEMPORAL | ALWAYS_LT_TEMPORAL_TEMPORAL | ALWAYS_NE_TEMPORAL_TEMPORAL | ALWAYS_EQ_TFLOAT_FLOAT | ALWAYS_GE_TFLOAT_FLOAT | ALWAYS_GT_TFLOAT_FLOAT | ALWAYS_LE_TFLOAT_FLOAT | ALWAYS_LT_TFLOAT_FLOAT | ALWAYS_NE_TFLOAT_FLOAT | ALWAYS_EQ_TFLOAT_TFLOAT | ALWAYS_GE_TFLOAT_TFLOAT | ALWAYS_GT_TFLOAT_TFLOAT | ALWAYS_LE_TFLOAT_TFLOAT | ALWAYS_LT_TFLOAT_TFLOAT | ALWAYS_NE_TFLOAT_TFLOAT | ALWAYS_EQ_INT_TINT | ALWAYS_GE_INT_TINT | ALWAYS_GT_INT_TINT | ALWAYS_LE_INT_TINT | ALWAYS_LT_INT_TINT | ALWAYS_NE_INT_TINT | ALWAYS_EQ_TINT_INT | ALWAYS_GE_TINT_INT | ALWAYS_GT_TINT_INT | ALWAYS_LE_TINT_INT | ALWAYS_LT_TINT_INT | ALWAYS_NE_TINT_INT | ALWAYS_EQ_TINT_TINT | ALWAYS_GE_TINT_TINT | ALWAYS_GT_TINT_TINT | ALWAYS_LE_TINT_TINT | ALWAYS_LT_TINT_TINT | ALWAYS_NE_TINT_TINT | EDWITHIN_TGEO_GEO | EVER_EQ_BIGINT_TBIGINT | EVER_GE_BIGINT_TBIGINT | EVER_GT_BIGINT_TBIGINT | EVER_LE_BIGINT_TBIGINT | EVER_LT_BIGINT_TBIGINT | EVER_NE_BIGINT_TBIGINT | EVER_EQ_TBIGINT_BIGINT | EVER_GE_TBIGINT_BIGINT | EVER_GT_TBIGINT_BIGINT | EVER_LE_TBIGINT_BIGINT | EVER_LT_TBIGINT_BIGINT | EVER_NE_TBIGINT_BIGINT | EVER_EQ_TBIGINT_TBIGINT | EVER_GE_TBIGINT_TBIGINT | EVER_GT_TBIGINT_TBIGINT | EVER_LE_TBIGINT_TBIGINT | EVER_LT_TBIGINT_TBIGINT | EVER_NE_TBIGINT_TBIGINT | EVER_EQ_BOOL_TBOOL | EVER_NE_BOOL_TBOOL | EVER_EQ_FLOAT_TFLOAT | EVER_GE_FLOAT_TFLOAT | EVER_GT_FLOAT_TFLOAT | EVER_LE_FLOAT_TFLOAT | EVER_LT_FLOAT_TFLOAT | EVER_NE_FLOAT_TFLOAT | EVER_EQ_TBOOL_BOOL | EVER_NE_TBOOL_BOOL | EVER_EQ_TEMPORAL_TEMPORAL | EVER_GE_TEMPORAL_TEMPORAL | EVER_GT_TEMPORAL_TEMPORAL | EVER_LE_TEMPORAL_TEMPORAL | EVER_LT_TEMPORAL_TEMPORAL | EVER_NE_TEMPORAL_TEMPORAL | EVER_EQ_TFLOAT_FLOAT | EVER_GE_TFLOAT_FLOAT | EVER_GT_TFLOAT_FLOAT | EVER_LE_TFLOAT_FLOAT | EVER_LT_TFLOAT_FLOAT | EVER_NE_TFLOAT_FLOAT | EVER_EQ_TFLOAT_TFLOAT | EVER_GE_TFLOAT_TFLOAT | EVER_GT_TFLOAT_TFLOAT | EVER_LE_TFLOAT_TFLOAT | EVER_LT_TFLOAT_TFLOAT | EVER_NE_TFLOAT_TFLOAT | EVER_EQ_INT_TINT | EVER_GE_INT_TINT | EVER_GT_INT_TINT | EVER_LE_INT_TINT | EVER_LT_INT_TINT | EVER_NE_INT_TINT | EVER_EQ_TINT_INT | EVER_GE_TINT_INT | EVER_GT_TINT_INT | EVER_LE_TINT_INT | EVER_LT_TINT_INT | EVER_NE_TINT_INT | EVER_EQ_TINT_TINT | EVER_GE_TINT_TINT | EVER_GT_TINT_TINT | EVER_LE_TINT_TINT | EVER_LT_TINT_TINT | EVER_NE_TINT_TINT | TGEO_AT_STBOX | ADD_BIGINT_TBIGINT | ADD_FLOAT_TFLOAT | ADD_INT_TINT | ADD_TBIGINT_BIGINT | ADD_TFLOAT_FLOAT | ADD_TINT_INT | ADD_TNUMBER_TNUMBER | DIV_BIGINT_TBIGINT | DIV_FLOAT_TFLOAT | DIV_INT_TINT | DIV_TBIGINT_BIGINT | DIV_TFLOAT_FLOAT | DIV_TINT_INT | DIV_TNUMBER_TNUMBER | MUL_BIGINT_TBIGINT | MUL_FLOAT_TFLOAT | MUL_INT_TINT | MUL_TBIGINT_BIGINT | MUL_TFLOAT_FLOAT | MUL_TINT_INT | MUL_TNUMBER_TNUMBER | SUB_BIGINT_TBIGINT | SUB_FLOAT_TFLOAT | SUB_INT_TINT | SUB_TBIGINT_BIGINT | SUB_TFLOAT_FLOAT | SUB_TINT_INT | SUB_TNUMBER_TNUMBER | TDISTANCE_TFLOAT_FLOAT | TDISTANCE_TINT_INT | TDISTANCE_TNUMBER_TNUMBER | TEMPORAL_ROUND | TFLOAT_CEIL | TFLOAT_COS | TFLOAT_DEGREES | TFLOAT_EXP | TFLOAT_FLOOR | TFLOAT_LN | TFLOAT_LOG10 | TFLOAT_RADIANS | TFLOAT_SCALE_VALUE | TFLOAT_SHIFT_SCALE_VALUE | TBIGINT_SCALE_VALUE | TBIGINT_SHIFT_SCALE_VALUE | TBIGINT_SHIFT_VALUE | TBIGINT_TO_TFLOAT | TBIGINT_TO_TINT | TFLOAT_SHIFT_VALUE | TFLOAT_SIN | TFLOAT_TAN | TFLOAT_TO_TBIGINT | TFLOAT_TO_TINT | TINT_SCALE_VALUE | TINT_SHIFT_SCALE_VALUE | TINT_SHIFT_VALUE | TINT_TO_TBIGINT | TINT_TO_TFLOAT | TNUMBER_ABS | TAND_BOOL_TBOOL | TAND_TBOOL_BOOL | TAND_TBOOL_TBOOL | TNOT_TBOOL | TOR_BOOL_TBOOL | TOR_TBOOL_BOOL | TOR_TBOOL_TBOOL | TEQ_BOOL_TBOOL | TEQ_FLOAT_TFLOAT | TEQ_INT_TINT | TEQ_TBOOL_BOOL | TEQ_TEMPORAL_TEMPORAL | TEQ_TFLOAT_FLOAT | TEQ_TINT_INT | TNE_TBOOL_BOOL | TNE_BOOL_TBOOL | TNE_TFLOAT_FLOAT | TNE_FLOAT_TFLOAT | TNE_TINT_INT | TNE_INT_TINT | TNE_TEMPORAL_TEMPORAL | TGE_TFLOAT_FLOAT | TGE_FLOAT_TFLOAT | TGE_TINT_INT | TGE_INT_TINT | TGE_TEMPORAL_TEMPORAL | TGT_TFLOAT_FLOAT | TGT_FLOAT_TFLOAT | TGT_TINT_INT | TGT_INT_TINT | TGT_TEMPORAL_TEMPORAL | TLE_TFLOAT_FLOAT | TLE_FLOAT_TFLOAT | TLE_TINT_INT | TLE_INT_TINT | TLE_TEMPORAL_TEMPORAL | TLT_TFLOAT_FLOAT | TLT_FLOAT_TFLOAT | TLT_TINT_INT | TLT_INT_TINT | TLT_TEMPORAL_TEMPORAL | TBOOL_TO_TINT | EVER_EQ_TTEXT_TEXT | EVER_NE_TTEXT_TEXT | EVER_GE_TTEXT_TEXT | EVER_GT_TTEXT_TEXT | EVER_LE_TTEXT_TEXT | EVER_LT_TTEXT_TEXT | EVER_EQ_TEXT_TTEXT | EVER_NE_TEXT_TTEXT | EVER_GE_TEXT_TTEXT | EVER_GT_TEXT_TTEXT | EVER_LE_TEXT_TTEXT | EVER_LT_TEXT_TTEXT | ALWAYS_EQ_TTEXT_TEXT | ALWAYS_NE_TTEXT_TEXT | ALWAYS_GE_TTEXT_TEXT | ALWAYS_GT_TTEXT_TEXT | ALWAYS_LE_TTEXT_TEXT | ALWAYS_LT_TTEXT_TEXT | ALWAYS_EQ_TEXT_TTEXT | ALWAYS_NE_TEXT_TTEXT | ALWAYS_GE_TEXT_TTEXT | ALWAYS_GT_TEXT_TTEXT | ALWAYS_LE_TEXT_TTEXT | ALWAYS_LT_TEXT_TTEXT | TEQ_TTEXT_TEXT | TNE_TTEXT_TEXT | TEQ_TEXT_TTEXT | TNE_TEXT_TTEXT | TGE_TTEXT_TEXT | TGE_TEXT_TTEXT | TGT_TTEXT_TEXT | TGT_TEXT_TTEXT | TLE_TTEXT_TEXT | TLE_TEXT_TTEXT | TLT_TTEXT_TEXT | TLT_TEXT_TTEXT | TTEXT_UPPER | TTEXT_LOWER | TTEXT_INITCAP | TEXTCAT_TTEXT_TEXT | TEXTCAT_TEXT_TTEXT | TEXTCAT_TTEXT_TTEXT | GEOM_LENGTH | GEOM_PERIMETER | GEOM_AZIMUTH | GEOG_AREA | GEOG_LENGTH | GEOG_PERIMETER | GEOM_IS_EMPTY | AINTERSECTS_TGEO_GEO | ACOVERS_TGEO_GEO | ADISJOINT_TGEO_GEO | ADWITHIN_TGEO_GEO | EINTERSECTS_TGEO_GEO | ETOUCHES_TGEO_GEO | ECONTAINS_TGEO_GEO | ACONTAINS_TGEO_GEO | ATOUCHES_TGEO_GEO | GEO_NUM_POINTS | GEO_NUM_GEOS | GEO_SRID | GEO_IS_UNITARY | GEO_EQUALS | GEO_SAME | GEOG_DISTANCE | NAD_TGEO_GEO | EVER_EQ_TGEO_GEO | EVER_NE_TGEO_GEO | ALWAYS_EQ_TGEO_GEO | ALWAYS_NE_TGEO_GEO | GEOG_INTERSECTS | GEOG_DWITHIN | ACOVERS_GEO_TGEO | GEOM_INTERSECTS | GEOM_DWITHIN | H3_GS_POINT_TO_CELL | EVER_EQ_TH3INDEX_H3INDEX | EVER_NE_TH3INDEX_H3INDEX | ALWAYS_EQ_TH3INDEX_H3INDEX | ALWAYS_NE_TH3INDEX_H3INDEX | TH3INDEX_GET_RESOLUTION | TH3INDEX_GET_BASE_CELL_NUMBER | TH3INDEX_IS_VALID_CELL | TH3INDEX_IS_PENTAGON | TH3INDEX_CELL_TO_PARENT_NEXT | TH3INDEX_CELL_TO_CENTER_CHILD_NEXT | TH3INDEX_CELL_TO_PARENT | TH3INDEX_CELL_TO_CENTER_CHILD | TH3INDEX_CELL_TO_CHILD_POS | TH3INDEX_ARE_NEIGHBOR_CELLS | TH3INDEX_GRID_DISTANCE | EINTERSECTS_TCBUFFER_GEO | AINTERSECTS_TCBUFFER_GEO | ECOVERS_TCBUFFER_GEO | ACOVERS_TCBUFFER_GEO | EDISJOINT_TCBUFFER_GEO | ADISJOINT_TCBUFFER_GEO | ETOUCHES_TCBUFFER_GEO | ATOUCHES_TCBUFFER_GEO | ECONTAINS_TCBUFFER_GEO | ACONTAINS_TCBUFFER_GEO | NAD_TCBUFFER_GEO | EDWITHIN_TCBUFFER_GEO | ADWITHIN_TCBUFFER_GEO | EINTERSECTS_TCBUFFER_CBUFFER | AINTERSECTS_TCBUFFER_CBUFFER | ECOVERS_TCBUFFER_CBUFFER | ACOVERS_TCBUFFER_CBUFFER | EDISJOINT_TCBUFFER_CBUFFER | ADISJOINT_TCBUFFER_CBUFFER | ETOUCHES_TCBUFFER_CBUFFER | ATOUCHES_TCBUFFER_CBUFFER | ECONTAINS_TCBUFFER_CBUFFER | ACONTAINS_TCBUFFER_CBUFFER | EVER_EQ_TCBUFFER_CBUFFER | ALWAYS_EQ_TCBUFFER_CBUFFER | EVER_NE_TCBUFFER_CBUFFER | ALWAYS_NE_TCBUFFER_CBUFFER | NAD_TCBUFFER_CBUFFER | EVER_EQ_TCBUFFER_TCBUFFER | ALWAYS_EQ_TCBUFFER_TCBUFFER | EVER_NE_TCBUFFER_TCBUFFER | ALWAYS_NE_TCBUFFER_TCBUFFER | EINTERSECTS_TCBUFFER_TCBUFFER | AINTERSECTS_TCBUFFER_TCBUFFER | ECOVERS_TCBUFFER_TCBUFFER | ACOVERS_TCBUFFER_TCBUFFER | ADISJOINT_TCBUFFER_TCBUFFER | ETOUCHES_TCBUFFER_TCBUFFER | ATOUCHES_TCBUFFER_TCBUFFER | NAD_TCBUFFER_TCBUFFER | EDWITHIN_TCBUFFER_TCBUFFER | ADWITHIN_TCBUFFER_TCBUFFER | MINDISTANCE_TCBUFFER_TCBUFFER | NAD_TNPOINT_GEO | EVER_EQ_TNPOINT_NPOINT | ALWAYS_EQ_TNPOINT_NPOINT | EVER_NE_TNPOINT_NPOINT | ALWAYS_NE_TNPOINT_NPOINT | NAD_TNPOINT_NPOINT | EVER_EQ_NPOINT_TNPOINT | ALWAYS_EQ_NPOINT_TNPOINT | EVER_NE_NPOINT_TNPOINT | ALWAYS_NE_NPOINT_TNPOINT | EVER_EQ_TNPOINT_TNPOINT | ALWAYS_EQ_TNPOINT_TNPOINT | EVER_NE_TNPOINT_TNPOINT | ALWAYS_NE_TNPOINT_TNPOINT | NAD_TNPOINT_TNPOINT | NAD_TPOSE_GEO | EVER_EQ_TPOSE_POSE | ALWAYS_EQ_TPOSE_POSE | EVER_NE_TPOSE_POSE | ALWAYS_NE_TPOSE_POSE | NAD_TPOSE_POSE | EVER_EQ_POSE_TPOSE | ALWAYS_EQ_POSE_TPOSE | EVER_NE_POSE_TPOSE | ALWAYS_NE_POSE_TPOSE | EVER_EQ_TPOSE_TPOSE | ALWAYS_EQ_TPOSE_TPOSE | EVER_NE_TPOSE_TPOSE | ALWAYS_NE_TPOSE_TPOSE | NAD_TPOSE_TPOSE | EVER_EQ_TRGEOMETRY_GEO | ALWAYS_EQ_TRGEOMETRY_GEO | EVER_NE_TRGEOMETRY_GEO | ALWAYS_NE_TRGEOMETRY_GEO | NAD_TRGEOMETRY_GEO | EVER_EQ_GEO_TRGEOMETRY | ALWAYS_EQ_GEO_TRGEOMETRY | EVER_NE_GEO_TRGEOMETRY | ALWAYS_NE_GEO_TRGEOMETRY | EVER_EQ_TRGEOMETRY_TRGEOMETRY | ALWAYS_EQ_TRGEOMETRY_TRGEOMETRY | EVER_NE_TRGEOMETRY_TRGEOMETRY | ALWAYS_NE_TRGEOMETRY_TRGEOMETRY | NAD_TRGEOMETRY_TRGEOMETRY | EINTERSECTS_TPCPOINT_GEO | NAD_TPCPOINT_GEO | QUADBIN_POINT_TO_CELL | QUADBIN_IS_VALID_CELL | QUADBIN_GET_RESOLUTION | QUADBIN_CELL_AREA | QUADBIN_CELL_TO_QUADKEY | QUADBIN_CELL_TO_PARENT | QUADBIN_TILE_TO_CELL | EVER_EQ_TJSONB_JSONB | ALWAYS_EQ_TJSONB_JSONB | EVER_NE_TJSONB_JSONB | ALWAYS_NE_TJSONB_JSONB | EVER_EQ_TJSONB_TJSONB | ALWAYS_EQ_TJSONB_TJSONB | EVER_NE_TJSONB_TJSONB | ALWAYS_NE_TJSONB_TJSONB | GEOM_BOUNDARY | GEOM_CENTROID | GEOM_CONVEX_HULL | GEO_REVERSE | GEO_POINTS | GEOM_UNARY_UNION | GEOM_DIFFERENCE2D | GEOM_INTERSECTION2D | GEOM_SHORTESTLINE2D | GEOM_SHORTESTLINE3D | GEO_SET_SRID | GEO_TRANSFORM | GEO_ROUND | GEOM_BUFFER | LINE_NUMPOINTS | LINE_LOCATE_POINT | LINE_INTERPOLATE_POINT | LINE_SUBSTRING | GEOM_POINT_MAKE2D | GEOM_POINT_MAKE3DZ | GEOG_POINT_MAKE2D | GEOG_POINT_MAKE3DZ | GEOM_TO_GEOG | GEOG_TO_GEOM | GEOG_CENTROID | GEO_GEO_N | LINE_POINT_N | GEOM_INTERSECTION2D_COLL | GEO_AS_GEOJSON | GEO_AS_HEXEWKB | GEO_AS_EWKT | GEO_FROM_GEOJSON | GEOM_FROM_HEXEWKB | GEO_TRANSFORM_PIPELINE | GEOM_MIN_BOUNDING_CENTER | GEOM_MIN_BOUNDING_RADIUS | EVER_EQ_TGEO_TGEO | EVER_NE_TGEO_TGEO | ALWAYS_EQ_TGEO_TGEO | ALWAYS_NE_TGEO_TGEO | EINTERSECTS_TGEO_TGEO | ECOVERS_TGEO_TGEO | EDISJOINT_TGEO_TGEO | ECONTAINS_TGEO_TGEO | ETOUCHES_TGEO_TGEO | AINTERSECTS_TGEO_TGEO | ACOVERS_TGEO_TGEO | ADISJOINT_TGEO_TGEO | ACONTAINS_TGEO_TGEO | ATOUCHES_TGEO_TGEO | EDWITHIN_TGEO_TGEO | ADWITHIN_TGEO_TGEO | H3INDEX_EQ | H3INDEX_NE | H3INDEX_LT | H3INDEX_LE | H3INDEX_GT | H3INDEX_GE | H3INDEX_CMP | H3INDEX_OUT | H3INDEX_IN | JSON_ARRAY_LENGTH | JSON_TYPEOF | JSON_OBJECT_FIELD_TEXT | JSON_ARRAY_ELEMENT_TEXT | JSONB_ARRAY_LENGTH | JSONB_EQ | JSONB_NE | JSONB_LT | JSONB_LE | JSONB_GT | JSONB_GE | JSONB_CMP | JSONB_CONTAINS | JSONB_CONTAINED | JSONB_EXISTS | JSONB_TO_CSTRING | JSONB_PRETTY | JSONB_OBJECT_FIELD_TEXT | JSONB_ARRAY_ELEMENT_TEXT | INTSPAN_MAKE | FLOATSPAN_MAKE | INTSPAN_LOWER | INTSPAN_UPPER | INTSPAN_WIDTH | INTSPAN_LOWER_INC | INTSPAN_UPPER_INC | FLOATSPAN_LOWER | FLOATSPAN_UPPER | FLOATSPAN_WIDTH | FLOATSPAN_LOWER_INC | FLOATSPAN_UPPER_INC | CONTAINED_INT_SPAN | CONTAINED_FLOAT_SPAN | CONTAINS_SPAN_INT | CONTAINS_SPAN_FLOAT | CONTAINED_SPAN_SPAN | CONTAINS_SPAN_SPAN | CONTAINED_FLOATSPAN_SPAN | CONTAINS_FLOATSPAN_SPAN | TEXT_UPPER | TEXT_LOWER | TEXT_INITCAP | QUADBIN_EQ | QUADBIN_NE | QUADBIN_LT | QUADBIN_LE | QUADBIN_GT | QUADBIN_GE | QUADBIN_CMP | EVER_EQ_QUADBIN_TQUADBIN | EVER_NE_QUADBIN_TQUADBIN | ALWAYS_EQ_QUADBIN_TQUADBIN | ALWAYS_NE_QUADBIN_TQUADBIN | EVER_EQ_TQUADBIN_QUADBIN | EVER_NE_TQUADBIN_QUADBIN | ALWAYS_EQ_TQUADBIN_QUADBIN | ALWAYS_NE_TQUADBIN_QUADBIN | EVER_EQ_TQUADBIN_TQUADBIN | EVER_NE_TQUADBIN_TQUADBIN | ALWAYS_EQ_TQUADBIN_TQUADBIN | ALWAYS_NE_TQUADBIN_TQUADBIN | GEOM_RELATE_PATTERN | GEOM_INTERSECTS2D | GEOM_DWITHIN2D | GEOM_CONTAINS | GEOM_DISJOINT2D | GEOM_COVERS | GEOM_TOUCHES | GEOM_INTERSECTS3D | GEOM_DWITHIN3D | GEOM_DISTANCE2D | EINTERSECTS_TRGEOMETRY_GEO | AINTERSECTS_TRGEOMETRY_GEO | EDISJOINT_TRGEOMETRY_GEO | ADISJOINT_TRGEOMETRY_GEO | ETOUCHES_TRGEOMETRY_GEO | ATOUCHES_TRGEOMETRY_GEO | ECOVERS_TRGEOMETRY_GEO | ACOVERS_TRGEOMETRY_GEO | EDWITHIN_TRGEOMETRY_GEO | ADWITHIN_TRGEOMETRY_GEO | ECONTAINS_GEO_TRGEOMETRY | ACONTAINS_GEO_TRGEOMETRY | ECOVERS_GEO_TRGEOMETRY | ACOVERS_GEO_TRGEOMETRY | GEOM_DISTANCE3D;

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
ALWAYS_EQ_BIGINT_TBIGINT: 'ALWAYS_EQ_BIGINT_TBIGINT' | 'always_eq_bigint_tbigint';
ALWAYS_GE_BIGINT_TBIGINT: 'ALWAYS_GE_BIGINT_TBIGINT' | 'always_ge_bigint_tbigint';
ALWAYS_GT_BIGINT_TBIGINT: 'ALWAYS_GT_BIGINT_TBIGINT' | 'always_gt_bigint_tbigint';
ALWAYS_LE_BIGINT_TBIGINT: 'ALWAYS_LE_BIGINT_TBIGINT' | 'always_le_bigint_tbigint';
ALWAYS_LT_BIGINT_TBIGINT: 'ALWAYS_LT_BIGINT_TBIGINT' | 'always_lt_bigint_tbigint';
ALWAYS_NE_BIGINT_TBIGINT: 'ALWAYS_NE_BIGINT_TBIGINT' | 'always_ne_bigint_tbigint';
ALWAYS_EQ_BOOL_TBOOL: 'ALWAYS_EQ_BOOL_TBOOL' | 'always_eq_bool_tbool';
ALWAYS_NE_BOOL_TBOOL: 'ALWAYS_NE_BOOL_TBOOL' | 'always_ne_bool_tbool';
ALWAYS_EQ_TBIGINT_BIGINT: 'ALWAYS_EQ_TBIGINT_BIGINT' | 'always_eq_tbigint_bigint';
ALWAYS_GE_TBIGINT_BIGINT: 'ALWAYS_GE_TBIGINT_BIGINT' | 'always_ge_tbigint_bigint';
ALWAYS_GT_TBIGINT_BIGINT: 'ALWAYS_GT_TBIGINT_BIGINT' | 'always_gt_tbigint_bigint';
ALWAYS_LE_TBIGINT_BIGINT: 'ALWAYS_LE_TBIGINT_BIGINT' | 'always_le_tbigint_bigint';
ALWAYS_LT_TBIGINT_BIGINT: 'ALWAYS_LT_TBIGINT_BIGINT' | 'always_lt_tbigint_bigint';
ALWAYS_NE_TBIGINT_BIGINT: 'ALWAYS_NE_TBIGINT_BIGINT' | 'always_ne_tbigint_bigint';
ALWAYS_EQ_TBIGINT_TBIGINT: 'ALWAYS_EQ_TBIGINT_TBIGINT' | 'always_eq_tbigint_tbigint';
ALWAYS_GE_TBIGINT_TBIGINT: 'ALWAYS_GE_TBIGINT_TBIGINT' | 'always_ge_tbigint_tbigint';
ALWAYS_GT_TBIGINT_TBIGINT: 'ALWAYS_GT_TBIGINT_TBIGINT' | 'always_gt_tbigint_tbigint';
ALWAYS_LE_TBIGINT_TBIGINT: 'ALWAYS_LE_TBIGINT_TBIGINT' | 'always_le_tbigint_tbigint';
ALWAYS_LT_TBIGINT_TBIGINT: 'ALWAYS_LT_TBIGINT_TBIGINT' | 'always_lt_tbigint_tbigint';
ALWAYS_NE_TBIGINT_TBIGINT: 'ALWAYS_NE_TBIGINT_TBIGINT' | 'always_ne_tbigint_tbigint';
ALWAYS_EQ_TBOOL_BOOL: 'ALWAYS_EQ_TBOOL_BOOL' | 'always_eq_tbool_bool';
ALWAYS_NE_TBOOL_BOOL: 'ALWAYS_NE_TBOOL_BOOL' | 'always_ne_tbool_bool';
ALWAYS_EQ_TEMPORAL_TEMPORAL: 'ALWAYS_EQ_TEMPORAL_TEMPORAL' | 'always_eq_temporal_temporal';
ALWAYS_GE_TEMPORAL_TEMPORAL: 'ALWAYS_GE_TEMPORAL_TEMPORAL' | 'always_ge_temporal_temporal';
ALWAYS_GT_TEMPORAL_TEMPORAL: 'ALWAYS_GT_TEMPORAL_TEMPORAL' | 'always_gt_temporal_temporal';
ALWAYS_LE_TEMPORAL_TEMPORAL: 'ALWAYS_LE_TEMPORAL_TEMPORAL' | 'always_le_temporal_temporal';
ALWAYS_LT_TEMPORAL_TEMPORAL: 'ALWAYS_LT_TEMPORAL_TEMPORAL' | 'always_lt_temporal_temporal';
ALWAYS_NE_TEMPORAL_TEMPORAL: 'ALWAYS_NE_TEMPORAL_TEMPORAL' | 'always_ne_temporal_temporal';
ALWAYS_EQ_FLOAT_TFLOAT: 'ALWAYS_EQ_FLOAT_TFLOAT' | 'always_eq_float_tfloat';
ALWAYS_GE_FLOAT_TFLOAT: 'ALWAYS_GE_FLOAT_TFLOAT' | 'always_ge_float_tfloat';
ALWAYS_GT_FLOAT_TFLOAT: 'ALWAYS_GT_FLOAT_TFLOAT' | 'always_gt_float_tfloat';
ALWAYS_LE_FLOAT_TFLOAT: 'ALWAYS_LE_FLOAT_TFLOAT' | 'always_le_float_tfloat';
ALWAYS_LT_FLOAT_TFLOAT: 'ALWAYS_LT_FLOAT_TFLOAT' | 'always_lt_float_tfloat';
ALWAYS_NE_FLOAT_TFLOAT: 'ALWAYS_NE_FLOAT_TFLOAT' | 'always_ne_float_tfloat';
ALWAYS_EQ_TFLOAT_FLOAT: 'ALWAYS_EQ_TFLOAT_FLOAT' | 'always_eq_tfloat_float';
ALWAYS_GE_TFLOAT_FLOAT: 'ALWAYS_GE_TFLOAT_FLOAT' | 'always_ge_tfloat_float';
ALWAYS_GT_TFLOAT_FLOAT: 'ALWAYS_GT_TFLOAT_FLOAT' | 'always_gt_tfloat_float';
ALWAYS_LE_TFLOAT_FLOAT: 'ALWAYS_LE_TFLOAT_FLOAT' | 'always_le_tfloat_float';
ALWAYS_LT_TFLOAT_FLOAT: 'ALWAYS_LT_TFLOAT_FLOAT' | 'always_lt_tfloat_float';
ALWAYS_NE_TFLOAT_FLOAT: 'ALWAYS_NE_TFLOAT_FLOAT' | 'always_ne_tfloat_float';
ALWAYS_EQ_TFLOAT_TFLOAT: 'ALWAYS_EQ_TFLOAT_TFLOAT' | 'always_eq_tfloat_tfloat';
ALWAYS_GE_TFLOAT_TFLOAT: 'ALWAYS_GE_TFLOAT_TFLOAT' | 'always_ge_tfloat_tfloat';
ALWAYS_GT_TFLOAT_TFLOAT: 'ALWAYS_GT_TFLOAT_TFLOAT' | 'always_gt_tfloat_tfloat';
ALWAYS_LE_TFLOAT_TFLOAT: 'ALWAYS_LE_TFLOAT_TFLOAT' | 'always_le_tfloat_tfloat';
ALWAYS_LT_TFLOAT_TFLOAT: 'ALWAYS_LT_TFLOAT_TFLOAT' | 'always_lt_tfloat_tfloat';
ALWAYS_NE_TFLOAT_TFLOAT: 'ALWAYS_NE_TFLOAT_TFLOAT' | 'always_ne_tfloat_tfloat';
ALWAYS_EQ_INT_TINT: 'ALWAYS_EQ_INT_TINT' | 'always_eq_int_tint';
ALWAYS_GE_INT_TINT: 'ALWAYS_GE_INT_TINT' | 'always_ge_int_tint';
ALWAYS_GT_INT_TINT: 'ALWAYS_GT_INT_TINT' | 'always_gt_int_tint';
ALWAYS_LE_INT_TINT: 'ALWAYS_LE_INT_TINT' | 'always_le_int_tint';
ALWAYS_LT_INT_TINT: 'ALWAYS_LT_INT_TINT' | 'always_lt_int_tint';
ALWAYS_NE_INT_TINT: 'ALWAYS_NE_INT_TINT' | 'always_ne_int_tint';
ALWAYS_EQ_TINT_INT: 'ALWAYS_EQ_TINT_INT' | 'always_eq_tint_int';
ALWAYS_GE_TINT_INT: 'ALWAYS_GE_TINT_INT' | 'always_ge_tint_int';
ALWAYS_GT_TINT_INT: 'ALWAYS_GT_TINT_INT' | 'always_gt_tint_int';
ALWAYS_LE_TINT_INT: 'ALWAYS_LE_TINT_INT' | 'always_le_tint_int';
ALWAYS_LT_TINT_INT: 'ALWAYS_LT_TINT_INT' | 'always_lt_tint_int';
ALWAYS_NE_TINT_INT: 'ALWAYS_NE_TINT_INT' | 'always_ne_tint_int';
ALWAYS_EQ_TINT_TINT: 'ALWAYS_EQ_TINT_TINT' | 'always_eq_tint_tint';
ALWAYS_GE_TINT_TINT: 'ALWAYS_GE_TINT_TINT' | 'always_ge_tint_tint';
ALWAYS_GT_TINT_TINT: 'ALWAYS_GT_TINT_TINT' | 'always_gt_tint_tint';
ALWAYS_LE_TINT_TINT: 'ALWAYS_LE_TINT_TINT' | 'always_le_tint_tint';
ALWAYS_LT_TINT_TINT: 'ALWAYS_LT_TINT_TINT' | 'always_lt_tint_tint';
ALWAYS_NE_TINT_TINT: 'ALWAYS_NE_TINT_TINT' | 'always_ne_tint_tint';
EDWITHIN_TGEO_GEO: 'EDWITHIN_TGEO_GEO' | 'edwithin_tgeo_geo';
EVER_EQ_BIGINT_TBIGINT: 'EVER_EQ_BIGINT_TBIGINT' | 'ever_eq_bigint_tbigint';
EVER_GE_BIGINT_TBIGINT: 'EVER_GE_BIGINT_TBIGINT' | 'ever_ge_bigint_tbigint';
EVER_GT_BIGINT_TBIGINT: 'EVER_GT_BIGINT_TBIGINT' | 'ever_gt_bigint_tbigint';
EVER_LE_BIGINT_TBIGINT: 'EVER_LE_BIGINT_TBIGINT' | 'ever_le_bigint_tbigint';
EVER_LT_BIGINT_TBIGINT: 'EVER_LT_BIGINT_TBIGINT' | 'ever_lt_bigint_tbigint';
EVER_NE_BIGINT_TBIGINT: 'EVER_NE_BIGINT_TBIGINT' | 'ever_ne_bigint_tbigint';
EVER_EQ_BOOL_TBOOL: 'EVER_EQ_BOOL_TBOOL' | 'ever_eq_bool_tbool';
EVER_NE_BOOL_TBOOL: 'EVER_NE_BOOL_TBOOL' | 'ever_ne_bool_tbool';
EVER_EQ_TBIGINT_BIGINT: 'EVER_EQ_TBIGINT_BIGINT' | 'ever_eq_tbigint_bigint';
EVER_GE_TBIGINT_BIGINT: 'EVER_GE_TBIGINT_BIGINT' | 'ever_ge_tbigint_bigint';
EVER_GT_TBIGINT_BIGINT: 'EVER_GT_TBIGINT_BIGINT' | 'ever_gt_tbigint_bigint';
EVER_LE_TBIGINT_BIGINT: 'EVER_LE_TBIGINT_BIGINT' | 'ever_le_tbigint_bigint';
EVER_LT_TBIGINT_BIGINT: 'EVER_LT_TBIGINT_BIGINT' | 'ever_lt_tbigint_bigint';
EVER_NE_TBIGINT_BIGINT: 'EVER_NE_TBIGINT_BIGINT' | 'ever_ne_tbigint_bigint';
EVER_EQ_TBIGINT_TBIGINT: 'EVER_EQ_TBIGINT_TBIGINT' | 'ever_eq_tbigint_tbigint';
EVER_GE_TBIGINT_TBIGINT: 'EVER_GE_TBIGINT_TBIGINT' | 'ever_ge_tbigint_tbigint';
EVER_GT_TBIGINT_TBIGINT: 'EVER_GT_TBIGINT_TBIGINT' | 'ever_gt_tbigint_tbigint';
EVER_LE_TBIGINT_TBIGINT: 'EVER_LE_TBIGINT_TBIGINT' | 'ever_le_tbigint_tbigint';
EVER_LT_TBIGINT_TBIGINT: 'EVER_LT_TBIGINT_TBIGINT' | 'ever_lt_tbigint_tbigint';
EVER_NE_TBIGINT_TBIGINT: 'EVER_NE_TBIGINT_TBIGINT' | 'ever_ne_tbigint_tbigint';
EVER_EQ_TBOOL_BOOL: 'EVER_EQ_TBOOL_BOOL' | 'ever_eq_tbool_bool';
EVER_NE_TBOOL_BOOL: 'EVER_NE_TBOOL_BOOL' | 'ever_ne_tbool_bool';
EVER_EQ_TEMPORAL_TEMPORAL: 'EVER_EQ_TEMPORAL_TEMPORAL' | 'ever_eq_temporal_temporal';
EVER_GE_TEMPORAL_TEMPORAL: 'EVER_GE_TEMPORAL_TEMPORAL' | 'ever_ge_temporal_temporal';
EVER_GT_TEMPORAL_TEMPORAL: 'EVER_GT_TEMPORAL_TEMPORAL' | 'ever_gt_temporal_temporal';
EVER_LE_TEMPORAL_TEMPORAL: 'EVER_LE_TEMPORAL_TEMPORAL' | 'ever_le_temporal_temporal';
EVER_LT_TEMPORAL_TEMPORAL: 'EVER_LT_TEMPORAL_TEMPORAL' | 'ever_lt_temporal_temporal';
EVER_NE_TEMPORAL_TEMPORAL: 'EVER_NE_TEMPORAL_TEMPORAL' | 'ever_ne_temporal_temporal';
EVER_EQ_FLOAT_TFLOAT: 'EVER_EQ_FLOAT_TFLOAT' | 'ever_eq_float_tfloat';
EVER_GE_FLOAT_TFLOAT: 'EVER_GE_FLOAT_TFLOAT' | 'ever_ge_float_tfloat';
EVER_GT_FLOAT_TFLOAT: 'EVER_GT_FLOAT_TFLOAT' | 'ever_gt_float_tfloat';
EVER_LE_FLOAT_TFLOAT: 'EVER_LE_FLOAT_TFLOAT' | 'ever_le_float_tfloat';
EVER_LT_FLOAT_TFLOAT: 'EVER_LT_FLOAT_TFLOAT' | 'ever_lt_float_tfloat';
EVER_NE_FLOAT_TFLOAT: 'EVER_NE_FLOAT_TFLOAT' | 'ever_ne_float_tfloat';
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
EVER_EQ_INT_TINT: 'EVER_EQ_INT_TINT' | 'ever_eq_int_tint';
EVER_GE_INT_TINT: 'EVER_GE_INT_TINT' | 'ever_ge_int_tint';
EVER_GT_INT_TINT: 'EVER_GT_INT_TINT' | 'ever_gt_int_tint';
EVER_LE_INT_TINT: 'EVER_LE_INT_TINT' | 'ever_le_int_tint';
EVER_LT_INT_TINT: 'EVER_LT_INT_TINT' | 'ever_lt_int_tint';
EVER_NE_INT_TINT: 'EVER_NE_INT_TINT' | 'ever_ne_int_tint';
EVER_EQ_TINT_INT: 'EVER_EQ_TINT_INT' | 'ever_eq_tint_int';
EVER_GE_TINT_INT: 'EVER_GE_TINT_INT' | 'ever_ge_tint_int';
EVER_GT_TINT_INT: 'EVER_GT_TINT_INT' | 'ever_gt_tint_int';
EVER_LE_TINT_INT: 'EVER_LE_TINT_INT' | 'ever_le_tint_int';
EVER_LT_TINT_INT: 'EVER_LT_TINT_INT' | 'ever_lt_tint_int';
EVER_NE_TINT_INT: 'EVER_NE_TINT_INT' | 'ever_ne_tint_int';
EVER_EQ_TINT_TINT: 'EVER_EQ_TINT_TINT' | 'ever_eq_tint_tint';
EVER_GE_TINT_TINT: 'EVER_GE_TINT_TINT' | 'ever_ge_tint_tint';
EVER_GT_TINT_TINT: 'EVER_GT_TINT_TINT' | 'ever_gt_tint_tint';
EVER_LE_TINT_TINT: 'EVER_LE_TINT_TINT' | 'ever_le_tint_tint';
EVER_LT_TINT_TINT: 'EVER_LT_TINT_TINT' | 'ever_lt_tint_tint';
EVER_NE_TINT_TINT: 'EVER_NE_TINT_TINT' | 'ever_ne_tint_tint';
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
TAND_BOOL_TBOOL: 'TAND_BOOL_TBOOL' | 'tand_bool_tbool';
TAND_TBOOL_BOOL: 'TAND_TBOOL_BOOL' | 'tand_tbool_bool';
TAND_TBOOL_TBOOL: 'TAND_TBOOL_TBOOL' | 'tand_tbool_tbool';
TNOT_TBOOL: 'TNOT_TBOOL' | 'tnot_tbool';
TOR_BOOL_TBOOL: 'TOR_BOOL_TBOOL' | 'tor_bool_tbool';
TOR_TBOOL_BOOL: 'TOR_TBOOL_BOOL' | 'tor_tbool_bool';
TOR_TBOOL_TBOOL: 'TOR_TBOOL_TBOOL' | 'tor_tbool_tbool';
TEQ_BOOL_TBOOL: 'TEQ_BOOL_TBOOL' | 'teq_bool_tbool';
TEQ_FLOAT_TFLOAT: 'TEQ_FLOAT_TFLOAT' | 'teq_float_tfloat';
TEQ_INT_TINT: 'TEQ_INT_TINT' | 'teq_int_tint';
TEQ_TBOOL_BOOL: 'TEQ_TBOOL_BOOL' | 'teq_tbool_bool';
TEQ_TEMPORAL_TEMPORAL: 'TEQ_TEMPORAL_TEMPORAL' | 'teq_temporal_temporal';
TEQ_TFLOAT_FLOAT: 'TEQ_TFLOAT_FLOAT' | 'teq_tfloat_float';
TEQ_TINT_INT: 'TEQ_TINT_INT' | 'teq_tint_int';
TNE_TBOOL_BOOL: 'TNE_TBOOL_BOOL' | 'tne_tbool_bool';
TNE_BOOL_TBOOL: 'TNE_BOOL_TBOOL' | 'tne_bool_tbool';
TNE_TFLOAT_FLOAT: 'TNE_TFLOAT_FLOAT' | 'tne_tfloat_float';
TNE_FLOAT_TFLOAT: 'TNE_FLOAT_TFLOAT' | 'tne_float_tfloat';
TNE_TINT_INT: 'TNE_TINT_INT' | 'tne_tint_int';
TNE_INT_TINT: 'TNE_INT_TINT' | 'tne_int_tint';
TNE_TEMPORAL_TEMPORAL: 'TNE_TEMPORAL_TEMPORAL' | 'tne_temporal_temporal';
TGE_TFLOAT_FLOAT: 'TGE_TFLOAT_FLOAT' | 'tge_tfloat_float';
TGE_FLOAT_TFLOAT: 'TGE_FLOAT_TFLOAT' | 'tge_float_tfloat';
TGE_TINT_INT: 'TGE_TINT_INT' | 'tge_tint_int';
TGE_INT_TINT: 'TGE_INT_TINT' | 'tge_int_tint';
TGE_TEMPORAL_TEMPORAL: 'TGE_TEMPORAL_TEMPORAL' | 'tge_temporal_temporal';
TGT_TFLOAT_FLOAT: 'TGT_TFLOAT_FLOAT' | 'tgt_tfloat_float';
TGT_FLOAT_TFLOAT: 'TGT_FLOAT_TFLOAT' | 'tgt_float_tfloat';
TGT_TINT_INT: 'TGT_TINT_INT' | 'tgt_tint_int';
TGT_INT_TINT: 'TGT_INT_TINT' | 'tgt_int_tint';
TGT_TEMPORAL_TEMPORAL: 'TGT_TEMPORAL_TEMPORAL' | 'tgt_temporal_temporal';
TLE_TFLOAT_FLOAT: 'TLE_TFLOAT_FLOAT' | 'tle_tfloat_float';
TLE_FLOAT_TFLOAT: 'TLE_FLOAT_TFLOAT' | 'tle_float_tfloat';
TLE_TINT_INT: 'TLE_TINT_INT' | 'tle_tint_int';
TLE_INT_TINT: 'TLE_INT_TINT' | 'tle_int_tint';
TLE_TEMPORAL_TEMPORAL: 'TLE_TEMPORAL_TEMPORAL' | 'tle_temporal_temporal';
TLT_TFLOAT_FLOAT: 'TLT_TFLOAT_FLOAT' | 'tlt_tfloat_float';
TLT_FLOAT_TFLOAT: 'TLT_FLOAT_TFLOAT' | 'tlt_float_tfloat';
TLT_TINT_INT: 'TLT_TINT_INT' | 'tlt_tint_int';
TLT_INT_TINT: 'TLT_INT_TINT' | 'tlt_int_tint';
TLT_TEMPORAL_TEMPORAL: 'TLT_TEMPORAL_TEMPORAL' | 'tlt_temporal_temporal';
TBOOL_TO_TINT: 'TBOOL_TO_TINT' | 'tbool_to_tint';
EVER_EQ_TTEXT_TEXT: 'EVER_EQ_TTEXT_TEXT' | 'ever_eq_ttext_text';
EVER_NE_TTEXT_TEXT: 'EVER_NE_TTEXT_TEXT' | 'ever_ne_ttext_text';
EVER_GE_TTEXT_TEXT: 'EVER_GE_TTEXT_TEXT' | 'ever_ge_ttext_text';
EVER_GT_TTEXT_TEXT: 'EVER_GT_TTEXT_TEXT' | 'ever_gt_ttext_text';
EVER_LE_TTEXT_TEXT: 'EVER_LE_TTEXT_TEXT' | 'ever_le_ttext_text';
EVER_LT_TTEXT_TEXT: 'EVER_LT_TTEXT_TEXT' | 'ever_lt_ttext_text';
EVER_EQ_TEXT_TTEXT: 'EVER_EQ_TEXT_TTEXT' | 'ever_eq_text_ttext';
EVER_NE_TEXT_TTEXT: 'EVER_NE_TEXT_TTEXT' | 'ever_ne_text_ttext';
EVER_GE_TEXT_TTEXT: 'EVER_GE_TEXT_TTEXT' | 'ever_ge_text_ttext';
EVER_GT_TEXT_TTEXT: 'EVER_GT_TEXT_TTEXT' | 'ever_gt_text_ttext';
EVER_LE_TEXT_TTEXT: 'EVER_LE_TEXT_TTEXT' | 'ever_le_text_ttext';
EVER_LT_TEXT_TTEXT: 'EVER_LT_TEXT_TTEXT' | 'ever_lt_text_ttext';
ALWAYS_EQ_TTEXT_TEXT: 'ALWAYS_EQ_TTEXT_TEXT' | 'always_eq_ttext_text';
ALWAYS_NE_TTEXT_TEXT: 'ALWAYS_NE_TTEXT_TEXT' | 'always_ne_ttext_text';
ALWAYS_GE_TTEXT_TEXT: 'ALWAYS_GE_TTEXT_TEXT' | 'always_ge_ttext_text';
ALWAYS_GT_TTEXT_TEXT: 'ALWAYS_GT_TTEXT_TEXT' | 'always_gt_ttext_text';
ALWAYS_LE_TTEXT_TEXT: 'ALWAYS_LE_TTEXT_TEXT' | 'always_le_ttext_text';
ALWAYS_LT_TTEXT_TEXT: 'ALWAYS_LT_TTEXT_TEXT' | 'always_lt_ttext_text';
ALWAYS_EQ_TEXT_TTEXT: 'ALWAYS_EQ_TEXT_TTEXT' | 'always_eq_text_ttext';
ALWAYS_NE_TEXT_TTEXT: 'ALWAYS_NE_TEXT_TTEXT' | 'always_ne_text_ttext';
ALWAYS_GE_TEXT_TTEXT: 'ALWAYS_GE_TEXT_TTEXT' | 'always_ge_text_ttext';
ALWAYS_GT_TEXT_TTEXT: 'ALWAYS_GT_TEXT_TTEXT' | 'always_gt_text_ttext';
ALWAYS_LE_TEXT_TTEXT: 'ALWAYS_LE_TEXT_TTEXT' | 'always_le_text_ttext';
ALWAYS_LT_TEXT_TTEXT: 'ALWAYS_LT_TEXT_TTEXT' | 'always_lt_text_ttext';
TEQ_TTEXT_TEXT: 'TEQ_TTEXT_TEXT' | 'teq_ttext_text';
TNE_TTEXT_TEXT: 'TNE_TTEXT_TEXT' | 'tne_ttext_text';
TEQ_TEXT_TTEXT: 'TEQ_TEXT_TTEXT' | 'teq_text_ttext';
TNE_TEXT_TTEXT: 'TNE_TEXT_TTEXT' | 'tne_text_ttext';
TGE_TTEXT_TEXT: 'TGE_TTEXT_TEXT' | 'tge_ttext_text';
TGE_TEXT_TTEXT: 'TGE_TEXT_TTEXT' | 'tge_text_ttext';
TGT_TTEXT_TEXT: 'TGT_TTEXT_TEXT' | 'tgt_ttext_text';
TGT_TEXT_TTEXT: 'TGT_TEXT_TTEXT' | 'tgt_text_ttext';
TLE_TTEXT_TEXT: 'TLE_TTEXT_TEXT' | 'tle_ttext_text';
TLE_TEXT_TTEXT: 'TLE_TEXT_TTEXT' | 'tle_text_ttext';
TLT_TTEXT_TEXT: 'TLT_TTEXT_TEXT' | 'tlt_ttext_text';
TLT_TEXT_TTEXT: 'TLT_TEXT_TTEXT' | 'tlt_text_ttext';
TTEXT_UPPER: 'TTEXT_UPPER' | 'ttext_upper';
TTEXT_LOWER: 'TTEXT_LOWER' | 'ttext_lower';
TTEXT_INITCAP: 'TTEXT_INITCAP' | 'ttext_initcap';
TEXTCAT_TTEXT_TEXT: 'TEXTCAT_TTEXT_TEXT' | 'textcat_ttext_text';
TEXTCAT_TEXT_TTEXT: 'TEXTCAT_TEXT_TTEXT' | 'textcat_text_ttext';
TEXTCAT_TTEXT_TTEXT: 'TEXTCAT_TTEXT_TTEXT' | 'textcat_ttext_ttext';
GEOM_LENGTH: 'GEOM_LENGTH' | 'geom_length';
GEOM_PERIMETER: 'GEOM_PERIMETER' | 'geom_perimeter';
GEOM_AZIMUTH: 'GEOM_AZIMUTH' | 'geom_azimuth';
GEOG_AREA: 'GEOG_AREA' | 'geog_area';
GEOG_LENGTH: 'GEOG_LENGTH' | 'geog_length';
GEOG_PERIMETER: 'GEOG_PERIMETER' | 'geog_perimeter';
GEOM_IS_EMPTY: 'GEOM_IS_EMPTY' | 'geo_is_empty';
AINTERSECTS_TGEO_GEO: 'AINTERSECTS_TGEO_GEO' | 'aintersects_tgeo_geo';
ACOVERS_TGEO_GEO: 'ACOVERS_TGEO_GEO' | 'acovers_tgeo_geo';
ADISJOINT_TGEO_GEO: 'ADISJOINT_TGEO_GEO' | 'adisjoint_tgeo_geo';
ADWITHIN_TGEO_GEO: 'ADWITHIN_TGEO_GEO' | 'adwithin_tgeo_geo';
EINTERSECTS_TGEO_GEO: 'EINTERSECTS_TGEO_GEO' | 'eintersects_tgeo_geo';
ETOUCHES_TGEO_GEO: 'ETOUCHES_TGEO_GEO' | 'etouches_tgeo_geo';
ECONTAINS_TGEO_GEO: 'ECONTAINS_TGEO_GEO' | 'econtains_tgeo_geo';
ACONTAINS_TGEO_GEO: 'ACONTAINS_TGEO_GEO' | 'acontains_tgeo_geo';
ATOUCHES_TGEO_GEO: 'ATOUCHES_TGEO_GEO' | 'atouches_tgeo_geo';
GEO_NUM_POINTS: 'GEO_NUM_POINTS' | 'geo_num_points';
GEO_NUM_GEOS: 'GEO_NUM_GEOS' | 'geo_num_geos';
GEO_SRID: 'GEO_SRID' | 'geo_srid';
GEO_IS_UNITARY: 'GEO_IS_UNITARY' | 'geo_is_unitary';
GEO_EQUALS: 'GEO_EQUALS' | 'geo_equals';
GEO_SAME: 'GEO_SAME' | 'geo_same';
GEOG_DISTANCE: 'GEOG_DISTANCE' | 'geog_distance';
NAD_TGEO_GEO: 'NAD_TGEO_GEO' | 'nad_tgeo_geo';
EVER_EQ_TGEO_GEO: 'EVER_EQ_TGEO_GEO' | 'ever_eq_tgeo_geo';
EVER_NE_TGEO_GEO: 'EVER_NE_TGEO_GEO' | 'ever_ne_tgeo_geo';
ALWAYS_EQ_TGEO_GEO: 'ALWAYS_EQ_TGEO_GEO' | 'always_eq_tgeo_geo';
ALWAYS_NE_TGEO_GEO: 'ALWAYS_NE_TGEO_GEO' | 'always_ne_tgeo_geo';
GEOG_INTERSECTS: 'GEOG_INTERSECTS' | 'geog_intersects';
GEOG_DWITHIN: 'GEOG_DWITHIN' | 'geog_dwithin';
ACOVERS_GEO_TGEO: 'ACOVERS_GEO_TGEO' | 'acovers_geo_tgeo';
GEOM_INTERSECTS: 'GEOM_INTERSECTS' | 'geom_intersects';
GEOM_DWITHIN: 'GEOM_DWITHIN' | 'geom_dwithin';
H3_GS_POINT_TO_CELL: 'H3_GS_POINT_TO_CELL' | 'h3_gs_point_to_cell';
EVER_EQ_TH3INDEX_H3INDEX: 'EVER_EQ_TH3INDEX_H3INDEX' | 'ever_eq_th3index_h3index';
EVER_NE_TH3INDEX_H3INDEX: 'EVER_NE_TH3INDEX_H3INDEX' | 'ever_ne_th3index_h3index';
ALWAYS_EQ_TH3INDEX_H3INDEX: 'ALWAYS_EQ_TH3INDEX_H3INDEX' | 'always_eq_th3index_h3index';
ALWAYS_NE_TH3INDEX_H3INDEX: 'ALWAYS_NE_TH3INDEX_H3INDEX' | 'always_ne_th3index_h3index';
TH3INDEX_GET_RESOLUTION: 'TH3INDEX_GET_RESOLUTION' | 'th3index_get_resolution';
TH3INDEX_GET_BASE_CELL_NUMBER: 'TH3INDEX_GET_BASE_CELL_NUMBER' | 'th3index_get_base_cell_number';
TH3INDEX_IS_VALID_CELL: 'TH3INDEX_IS_VALID_CELL' | 'th3index_is_valid_cell';
TH3INDEX_IS_PENTAGON: 'TH3INDEX_IS_PENTAGON' | 'th3index_is_pentagon';
TH3INDEX_CELL_TO_PARENT_NEXT: 'TH3INDEX_CELL_TO_PARENT_NEXT' | 'th3index_cell_to_parent_next';
TH3INDEX_CELL_TO_CENTER_CHILD_NEXT: 'TH3INDEX_CELL_TO_CENTER_CHILD_NEXT' | 'th3index_cell_to_center_child_next';
TH3INDEX_CELL_TO_PARENT: 'TH3INDEX_CELL_TO_PARENT' | 'th3index_cell_to_parent';
TH3INDEX_CELL_TO_CENTER_CHILD: 'TH3INDEX_CELL_TO_CENTER_CHILD' | 'th3index_cell_to_center_child';
TH3INDEX_CELL_TO_CHILD_POS: 'TH3INDEX_CELL_TO_CHILD_POS' | 'th3index_cell_to_child_pos';
TH3INDEX_ARE_NEIGHBOR_CELLS: 'TH3INDEX_ARE_NEIGHBOR_CELLS' | 'th3index_are_neighbor_cells';
TH3INDEX_GRID_DISTANCE: 'TH3INDEX_GRID_DISTANCE' | 'th3index_grid_distance';
EINTERSECTS_TCBUFFER_GEO: 'EINTERSECTS_TCBUFFER_GEO' | 'eintersects_tcbuffer_geo';
AINTERSECTS_TCBUFFER_GEO: 'AINTERSECTS_TCBUFFER_GEO' | 'aintersects_tcbuffer_geo';
ECOVERS_TCBUFFER_GEO: 'ECOVERS_TCBUFFER_GEO' | 'ecovers_tcbuffer_geo';
ACOVERS_TCBUFFER_GEO: 'ACOVERS_TCBUFFER_GEO' | 'acovers_tcbuffer_geo';
EDISJOINT_TCBUFFER_GEO: 'EDISJOINT_TCBUFFER_GEO' | 'edisjoint_tcbuffer_geo';
ADISJOINT_TCBUFFER_GEO: 'ADISJOINT_TCBUFFER_GEO' | 'adisjoint_tcbuffer_geo';
ETOUCHES_TCBUFFER_GEO: 'ETOUCHES_TCBUFFER_GEO' | 'etouches_tcbuffer_geo';
ATOUCHES_TCBUFFER_GEO: 'ATOUCHES_TCBUFFER_GEO' | 'atouches_tcbuffer_geo';
ECONTAINS_TCBUFFER_GEO: 'ECONTAINS_TCBUFFER_GEO' | 'econtains_tcbuffer_geo';
ACONTAINS_TCBUFFER_GEO: 'ACONTAINS_TCBUFFER_GEO' | 'acontains_tcbuffer_geo';
NAD_TCBUFFER_GEO: 'NAD_TCBUFFER_GEO' | 'nad_tcbuffer_geo';
EDWITHIN_TCBUFFER_GEO: 'EDWITHIN_TCBUFFER_GEO' | 'edwithin_tcbuffer_geo';
ADWITHIN_TCBUFFER_GEO: 'ADWITHIN_TCBUFFER_GEO' | 'adwithin_tcbuffer_geo';
EINTERSECTS_TCBUFFER_CBUFFER: 'EINTERSECTS_TCBUFFER_CBUFFER' | 'eintersects_tcbuffer_cbuffer';
AINTERSECTS_TCBUFFER_CBUFFER: 'AINTERSECTS_TCBUFFER_CBUFFER' | 'aintersects_tcbuffer_cbuffer';
ECOVERS_TCBUFFER_CBUFFER: 'ECOVERS_TCBUFFER_CBUFFER' | 'ecovers_tcbuffer_cbuffer';
ACOVERS_TCBUFFER_CBUFFER: 'ACOVERS_TCBUFFER_CBUFFER' | 'acovers_tcbuffer_cbuffer';
EDISJOINT_TCBUFFER_CBUFFER: 'EDISJOINT_TCBUFFER_CBUFFER' | 'edisjoint_tcbuffer_cbuffer';
ADISJOINT_TCBUFFER_CBUFFER: 'ADISJOINT_TCBUFFER_CBUFFER' | 'adisjoint_tcbuffer_cbuffer';
ETOUCHES_TCBUFFER_CBUFFER: 'ETOUCHES_TCBUFFER_CBUFFER' | 'etouches_tcbuffer_cbuffer';
ATOUCHES_TCBUFFER_CBUFFER: 'ATOUCHES_TCBUFFER_CBUFFER' | 'atouches_tcbuffer_cbuffer';
ECONTAINS_TCBUFFER_CBUFFER: 'ECONTAINS_TCBUFFER_CBUFFER' | 'econtains_tcbuffer_cbuffer';
ACONTAINS_TCBUFFER_CBUFFER: 'ACONTAINS_TCBUFFER_CBUFFER' | 'acontains_tcbuffer_cbuffer';
EVER_EQ_TCBUFFER_CBUFFER: 'EVER_EQ_TCBUFFER_CBUFFER' | 'ever_eq_tcbuffer_cbuffer';
ALWAYS_EQ_TCBUFFER_CBUFFER: 'ALWAYS_EQ_TCBUFFER_CBUFFER' | 'always_eq_tcbuffer_cbuffer';
EVER_NE_TCBUFFER_CBUFFER: 'EVER_NE_TCBUFFER_CBUFFER' | 'ever_ne_tcbuffer_cbuffer';
ALWAYS_NE_TCBUFFER_CBUFFER: 'ALWAYS_NE_TCBUFFER_CBUFFER' | 'always_ne_tcbuffer_cbuffer';
NAD_TCBUFFER_CBUFFER: 'NAD_TCBUFFER_CBUFFER' | 'nad_tcbuffer_cbuffer';
EVER_EQ_TCBUFFER_TCBUFFER: 'EVER_EQ_TCBUFFER_TCBUFFER' | 'ever_eq_tcbuffer_tcbuffer';
ALWAYS_EQ_TCBUFFER_TCBUFFER: 'ALWAYS_EQ_TCBUFFER_TCBUFFER' | 'always_eq_tcbuffer_tcbuffer';
EVER_NE_TCBUFFER_TCBUFFER: 'EVER_NE_TCBUFFER_TCBUFFER' | 'ever_ne_tcbuffer_tcbuffer';
ALWAYS_NE_TCBUFFER_TCBUFFER: 'ALWAYS_NE_TCBUFFER_TCBUFFER' | 'always_ne_tcbuffer_tcbuffer';
EINTERSECTS_TCBUFFER_TCBUFFER: 'EINTERSECTS_TCBUFFER_TCBUFFER' | 'eintersects_tcbuffer_tcbuffer';
AINTERSECTS_TCBUFFER_TCBUFFER: 'AINTERSECTS_TCBUFFER_TCBUFFER' | 'aintersects_tcbuffer_tcbuffer';
ECOVERS_TCBUFFER_TCBUFFER: 'ECOVERS_TCBUFFER_TCBUFFER' | 'ecovers_tcbuffer_tcbuffer';
ACOVERS_TCBUFFER_TCBUFFER: 'ACOVERS_TCBUFFER_TCBUFFER' | 'acovers_tcbuffer_tcbuffer';
ADISJOINT_TCBUFFER_TCBUFFER: 'ADISJOINT_TCBUFFER_TCBUFFER' | 'adisjoint_tcbuffer_tcbuffer';
ETOUCHES_TCBUFFER_TCBUFFER: 'ETOUCHES_TCBUFFER_TCBUFFER' | 'etouches_tcbuffer_tcbuffer';
ATOUCHES_TCBUFFER_TCBUFFER: 'ATOUCHES_TCBUFFER_TCBUFFER' | 'atouches_tcbuffer_tcbuffer';
NAD_TCBUFFER_TCBUFFER: 'NAD_TCBUFFER_TCBUFFER' | 'nad_tcbuffer_tcbuffer';
EDWITHIN_TCBUFFER_TCBUFFER: 'EDWITHIN_TCBUFFER_TCBUFFER' | 'edwithin_tcbuffer_tcbuffer';
ADWITHIN_TCBUFFER_TCBUFFER: 'ADWITHIN_TCBUFFER_TCBUFFER' | 'adwithin_tcbuffer_tcbuffer';
MINDISTANCE_TCBUFFER_TCBUFFER: 'MINDISTANCE_TCBUFFER_TCBUFFER' | 'mindistance_tcbuffer_tcbuffer';
NAD_TNPOINT_GEO: 'NAD_TNPOINT_GEO' | 'nad_tnpoint_geo';
EVER_EQ_TNPOINT_NPOINT: 'EVER_EQ_TNPOINT_NPOINT' | 'ever_eq_tnpoint_npoint';
ALWAYS_EQ_TNPOINT_NPOINT: 'ALWAYS_EQ_TNPOINT_NPOINT' | 'always_eq_tnpoint_npoint';
EVER_NE_TNPOINT_NPOINT: 'EVER_NE_TNPOINT_NPOINT' | 'ever_ne_tnpoint_npoint';
ALWAYS_NE_TNPOINT_NPOINT: 'ALWAYS_NE_TNPOINT_NPOINT' | 'always_ne_tnpoint_npoint';
NAD_TNPOINT_NPOINT: 'NAD_TNPOINT_NPOINT' | 'nad_tnpoint_npoint';
EVER_EQ_NPOINT_TNPOINT: 'EVER_EQ_NPOINT_TNPOINT' | 'ever_eq_npoint_tnpoint';
ALWAYS_EQ_NPOINT_TNPOINT: 'ALWAYS_EQ_NPOINT_TNPOINT' | 'always_eq_npoint_tnpoint';
EVER_NE_NPOINT_TNPOINT: 'EVER_NE_NPOINT_TNPOINT' | 'ever_ne_npoint_tnpoint';
ALWAYS_NE_NPOINT_TNPOINT: 'ALWAYS_NE_NPOINT_TNPOINT' | 'always_ne_npoint_tnpoint';
EVER_EQ_TNPOINT_TNPOINT: 'EVER_EQ_TNPOINT_TNPOINT' | 'ever_eq_tnpoint_tnpoint';
ALWAYS_EQ_TNPOINT_TNPOINT: 'ALWAYS_EQ_TNPOINT_TNPOINT' | 'always_eq_tnpoint_tnpoint';
EVER_NE_TNPOINT_TNPOINT: 'EVER_NE_TNPOINT_TNPOINT' | 'ever_ne_tnpoint_tnpoint';
ALWAYS_NE_TNPOINT_TNPOINT: 'ALWAYS_NE_TNPOINT_TNPOINT' | 'always_ne_tnpoint_tnpoint';
NAD_TNPOINT_TNPOINT: 'NAD_TNPOINT_TNPOINT' | 'nad_tnpoint_tnpoint';
NAD_TPOSE_GEO: 'NAD_TPOSE_GEO' | 'nad_tpose_geo';
EVER_EQ_TPOSE_POSE: 'EVER_EQ_TPOSE_POSE' | 'ever_eq_tpose_pose';
ALWAYS_EQ_TPOSE_POSE: 'ALWAYS_EQ_TPOSE_POSE' | 'always_eq_tpose_pose';
EVER_NE_TPOSE_POSE: 'EVER_NE_TPOSE_POSE' | 'ever_ne_tpose_pose';
ALWAYS_NE_TPOSE_POSE: 'ALWAYS_NE_TPOSE_POSE' | 'always_ne_tpose_pose';
NAD_TPOSE_POSE: 'NAD_TPOSE_POSE' | 'nad_tpose_pose';
EVER_EQ_POSE_TPOSE: 'EVER_EQ_POSE_TPOSE' | 'ever_eq_pose_tpose';
ALWAYS_EQ_POSE_TPOSE: 'ALWAYS_EQ_POSE_TPOSE' | 'always_eq_pose_tpose';
EVER_NE_POSE_TPOSE: 'EVER_NE_POSE_TPOSE' | 'ever_ne_pose_tpose';
ALWAYS_NE_POSE_TPOSE: 'ALWAYS_NE_POSE_TPOSE' | 'always_ne_pose_tpose';
EVER_EQ_TPOSE_TPOSE: 'EVER_EQ_TPOSE_TPOSE' | 'ever_eq_tpose_tpose';
ALWAYS_EQ_TPOSE_TPOSE: 'ALWAYS_EQ_TPOSE_TPOSE' | 'always_eq_tpose_tpose';
EVER_NE_TPOSE_TPOSE: 'EVER_NE_TPOSE_TPOSE' | 'ever_ne_tpose_tpose';
ALWAYS_NE_TPOSE_TPOSE: 'ALWAYS_NE_TPOSE_TPOSE' | 'always_ne_tpose_tpose';
NAD_TPOSE_TPOSE: 'NAD_TPOSE_TPOSE' | 'nad_tpose_tpose';
EVER_EQ_TRGEOMETRY_GEO: 'EVER_EQ_TRGEOMETRY_GEO' | 'ever_eq_trgeometry_geo';
ALWAYS_EQ_TRGEOMETRY_GEO: 'ALWAYS_EQ_TRGEOMETRY_GEO' | 'always_eq_trgeometry_geo';
EVER_NE_TRGEOMETRY_GEO: 'EVER_NE_TRGEOMETRY_GEO' | 'ever_ne_trgeometry_geo';
ALWAYS_NE_TRGEOMETRY_GEO: 'ALWAYS_NE_TRGEOMETRY_GEO' | 'always_ne_trgeometry_geo';
NAD_TRGEOMETRY_GEO: 'NAD_TRGEOMETRY_GEO' | 'nad_trgeometry_geo';
EVER_EQ_GEO_TRGEOMETRY: 'EVER_EQ_GEO_TRGEOMETRY' | 'ever_eq_geo_trgeometry';
ALWAYS_EQ_GEO_TRGEOMETRY: 'ALWAYS_EQ_GEO_TRGEOMETRY' | 'always_eq_geo_trgeometry';
EVER_NE_GEO_TRGEOMETRY: 'EVER_NE_GEO_TRGEOMETRY' | 'ever_ne_geo_trgeometry';
ALWAYS_NE_GEO_TRGEOMETRY: 'ALWAYS_NE_GEO_TRGEOMETRY' | 'always_ne_geo_trgeometry';
EVER_EQ_TRGEOMETRY_TRGEOMETRY: 'EVER_EQ_TRGEOMETRY_TRGEOMETRY' | 'ever_eq_trgeometry_trgeometry';
ALWAYS_EQ_TRGEOMETRY_TRGEOMETRY: 'ALWAYS_EQ_TRGEOMETRY_TRGEOMETRY' | 'always_eq_trgeometry_trgeometry';
EVER_NE_TRGEOMETRY_TRGEOMETRY: 'EVER_NE_TRGEOMETRY_TRGEOMETRY' | 'ever_ne_trgeometry_trgeometry';
ALWAYS_NE_TRGEOMETRY_TRGEOMETRY: 'ALWAYS_NE_TRGEOMETRY_TRGEOMETRY' | 'always_ne_trgeometry_trgeometry';
NAD_TRGEOMETRY_TRGEOMETRY: 'NAD_TRGEOMETRY_TRGEOMETRY' | 'nad_trgeometry_trgeometry';
EINTERSECTS_TPCPOINT_GEO: 'EINTERSECTS_TPCPOINT_GEO' | 'eintersects_tpcpoint_geo';
NAD_TPCPOINT_GEO: 'NAD_TPCPOINT_GEO' | 'nad_tpcpoint_geo';
QUADBIN_POINT_TO_CELL: 'QUADBIN_POINT_TO_CELL' | 'quadbin_point_to_cell';
QUADBIN_IS_VALID_CELL: 'QUADBIN_IS_VALID_CELL' | 'quadbin_is_valid_cell';
QUADBIN_GET_RESOLUTION: 'QUADBIN_GET_RESOLUTION' | 'quadbin_get_resolution';
QUADBIN_CELL_AREA: 'QUADBIN_CELL_AREA' | 'quadbin_cell_area';
QUADBIN_CELL_TO_QUADKEY: 'QUADBIN_CELL_TO_QUADKEY' | 'quadbin_cell_to_quadkey';
QUADBIN_CELL_TO_PARENT: 'QUADBIN_CELL_TO_PARENT' | 'quadbin_cell_to_parent';
QUADBIN_TILE_TO_CELL: 'QUADBIN_TILE_TO_CELL' | 'quadbin_tile_to_cell';
EVER_EQ_TJSONB_JSONB: 'EVER_EQ_TJSONB_JSONB' | 'ever_eq_tjsonb_jsonb';
ALWAYS_EQ_TJSONB_JSONB: 'ALWAYS_EQ_TJSONB_JSONB' | 'always_eq_tjsonb_jsonb';
EVER_NE_TJSONB_JSONB: 'EVER_NE_TJSONB_JSONB' | 'ever_ne_tjsonb_jsonb';
ALWAYS_NE_TJSONB_JSONB: 'ALWAYS_NE_TJSONB_JSONB' | 'always_ne_tjsonb_jsonb';
EVER_EQ_TJSONB_TJSONB: 'EVER_EQ_TJSONB_TJSONB' | 'ever_eq_tjsonb_tjsonb';
ALWAYS_EQ_TJSONB_TJSONB: 'ALWAYS_EQ_TJSONB_TJSONB' | 'always_eq_tjsonb_tjsonb';
EVER_NE_TJSONB_TJSONB: 'EVER_NE_TJSONB_TJSONB' | 'ever_ne_tjsonb_tjsonb';
ALWAYS_NE_TJSONB_TJSONB: 'ALWAYS_NE_TJSONB_TJSONB' | 'always_ne_tjsonb_tjsonb';
GEOM_BOUNDARY: 'GEOM_BOUNDARY' | 'geom_boundary';
GEOM_CENTROID: 'GEOM_CENTROID' | 'geom_centroid';
GEOM_CONVEX_HULL: 'GEOM_CONVEX_HULL' | 'geom_convex_hull';
GEO_REVERSE: 'GEO_REVERSE' | 'geo_reverse';
GEO_POINTS: 'GEO_POINTS' | 'geo_points';
GEOM_UNARY_UNION: 'GEOM_UNARY_UNION' | 'geom_unary_union';
GEOM_DIFFERENCE2D: 'GEOM_DIFFERENCE2D' | 'geom_difference2d';
GEOM_INTERSECTION2D: 'GEOM_INTERSECTION2D' | 'geom_intersection2d';
GEOM_SHORTESTLINE2D: 'GEOM_SHORTESTLINE2D' | 'geom_shortestline2d';
GEOM_SHORTESTLINE3D: 'GEOM_SHORTESTLINE3D' | 'geom_shortestline3d';
GEO_SET_SRID: 'GEO_SET_SRID' | 'geo_set_srid';
GEO_TRANSFORM: 'GEO_TRANSFORM' | 'geo_transform';
GEO_ROUND: 'GEO_ROUND' | 'geo_round';
GEOM_BUFFER: 'GEOM_BUFFER' | 'geom_buffer';
LINE_NUMPOINTS: 'LINE_NUMPOINTS' | 'line_numpoints';
LINE_LOCATE_POINT: 'LINE_LOCATE_POINT' | 'line_locate_point';
LINE_INTERPOLATE_POINT: 'LINE_INTERPOLATE_POINT' | 'line_interpolate_point';
LINE_SUBSTRING: 'LINE_SUBSTRING' | 'line_substring';
GEOM_POINT_MAKE2D: 'GEOM_POINT_MAKE2D' | 'geompoint_make2d';
GEOM_POINT_MAKE3DZ: 'GEOM_POINT_MAKE3DZ' | 'geompoint_make3dz';
GEOG_POINT_MAKE2D: 'GEOG_POINT_MAKE2D' | 'geogpoint_make2d';
GEOG_POINT_MAKE3DZ: 'GEOG_POINT_MAKE3DZ' | 'geogpoint_make3dz';
GEOM_TO_GEOG: 'GEOM_TO_GEOG' | 'geom_to_geog';
GEOG_TO_GEOM: 'GEOG_TO_GEOM' | 'geog_to_geom';
GEOG_CENTROID: 'GEOG_CENTROID' | 'geog_centroid';
GEO_GEO_N: 'GEO_GEO_N' | 'geo_geo_n';
LINE_POINT_N: 'LINE_POINT_N' | 'line_point_n';
GEOM_INTERSECTION2D_COLL: 'GEOM_INTERSECTION2D_COLL' | 'geom_intersection2d_coll';
GEO_AS_GEOJSON: 'GEO_AS_GEOJSON' | 'geo_as_geojson';
GEO_AS_HEXEWKB: 'GEO_AS_HEXEWKB' | 'geo_as_hexewkb';
GEO_AS_EWKT: 'GEO_AS_EWKT' | 'geo_as_ewkt';
GEO_FROM_GEOJSON: 'GEO_FROM_GEOJSON' | 'geo_from_geojson';
GEOM_FROM_HEXEWKB: 'GEOM_FROM_HEXEWKB' | 'geom_from_hexewkb';
GEO_TRANSFORM_PIPELINE: 'GEO_TRANSFORM_PIPELINE' | 'geo_transform_pipeline';
GEOM_MIN_BOUNDING_CENTER: 'GEOM_MIN_BOUNDING_CENTER' | 'geom_min_bounding_radius';
GEOM_MIN_BOUNDING_RADIUS: 'GEOM_MIN_BOUNDING_RADIUS' | 'geom_min_bounding_radius';
EVER_EQ_TGEO_TGEO: 'EVER_EQ_TGEO_TGEO' | 'ever_eq_tgeo_tgeo';
EVER_NE_TGEO_TGEO: 'EVER_NE_TGEO_TGEO' | 'ever_ne_tgeo_tgeo';
ALWAYS_EQ_TGEO_TGEO: 'ALWAYS_EQ_TGEO_TGEO' | 'always_eq_tgeo_tgeo';
ALWAYS_NE_TGEO_TGEO: 'ALWAYS_NE_TGEO_TGEO' | 'always_ne_tgeo_tgeo';
EINTERSECTS_TGEO_TGEO: 'EINTERSECTS_TGEO_TGEO' | 'eintersects_tgeo_tgeo';
ECOVERS_TGEO_TGEO: 'ECOVERS_TGEO_TGEO' | 'ecovers_tgeo_tgeo';
EDISJOINT_TGEO_TGEO: 'EDISJOINT_TGEO_TGEO' | 'edisjoint_tgeo_tgeo';
ECONTAINS_TGEO_TGEO: 'ECONTAINS_TGEO_TGEO' | 'econtains_tgeo_tgeo';
ETOUCHES_TGEO_TGEO: 'ETOUCHES_TGEO_TGEO' | 'etouches_tgeo_tgeo';
AINTERSECTS_TGEO_TGEO: 'AINTERSECTS_TGEO_TGEO' | 'aintersects_tgeo_tgeo';
ACOVERS_TGEO_TGEO: 'ACOVERS_TGEO_TGEO' | 'acovers_tgeo_tgeo';
ADISJOINT_TGEO_TGEO: 'ADISJOINT_TGEO_TGEO' | 'adisjoint_tgeo_tgeo';
ACONTAINS_TGEO_TGEO: 'ACONTAINS_TGEO_TGEO' | 'acontains_tgeo_tgeo';
ATOUCHES_TGEO_TGEO: 'ATOUCHES_TGEO_TGEO' | 'atouches_tgeo_tgeo';
EDWITHIN_TGEO_TGEO: 'EDWITHIN_TGEO_TGEO' | 'edwithin_tgeo_tgeo';
ADWITHIN_TGEO_TGEO: 'ADWITHIN_TGEO_TGEO' | 'adwithin_tgeo_tgeo';
H3INDEX_EQ: 'H3INDEX_EQ' | 'h3index_eq';
H3INDEX_NE: 'H3INDEX_NE' | 'h3index_ne';
H3INDEX_LT: 'H3INDEX_LT' | 'h3index_lt';
H3INDEX_LE: 'H3INDEX_LE' | 'h3index_le';
H3INDEX_GT: 'H3INDEX_GT' | 'h3index_gt';
H3INDEX_GE: 'H3INDEX_GE' | 'h3index_ge';
H3INDEX_CMP: 'H3INDEX_CMP' | 'h3index_cmp';
H3INDEX_OUT: 'H3INDEX_OUT' | 'h3index_out';
H3INDEX_IN: 'H3INDEX_IN' | 'h3index_in';
JSON_ARRAY_LENGTH: 'JSON_ARRAY_LENGTH' | 'json_array_length';
JSON_TYPEOF: 'JSON_TYPEOF' | 'json_typeof';
JSON_OBJECT_FIELD_TEXT: 'JSON_OBJECT_FIELD_TEXT' | 'json_object_field_text';
JSON_ARRAY_ELEMENT_TEXT: 'JSON_ARRAY_ELEMENT_TEXT' | 'json_array_element_text';
JSONB_ARRAY_LENGTH: 'JSONB_ARRAY_LENGTH' | 'jsonb_array_length';
JSONB_EQ: 'JSONB_EQ' | 'jsonb_eq';
JSONB_NE: 'JSONB_NE' | 'jsonb_ne';
JSONB_LT: 'JSONB_LT' | 'jsonb_lt';
JSONB_LE: 'JSONB_LE' | 'jsonb_le';
JSONB_GT: 'JSONB_GT' | 'jsonb_gt';
JSONB_GE: 'JSONB_GE' | 'jsonb_ge';
JSONB_CMP: 'JSONB_CMP' | 'jsonb_cmp';
JSONB_CONTAINS: 'JSONB_CONTAINS' | 'jsonb_contains';
JSONB_CONTAINED: 'JSONB_CONTAINED' | 'jsonb_contained';
JSONB_EXISTS: 'JSONB_EXISTS' | 'jsonb_exists';
JSONB_TO_CSTRING: 'JSONB_TO_CSTRING' | 'jsonb_to_cstring';
JSONB_PRETTY: 'JSONB_PRETTY' | 'jsonb_pretty';
JSONB_OBJECT_FIELD_TEXT: 'JSONB_OBJECT_FIELD_TEXT' | 'jsonb_object_field_text';
JSONB_ARRAY_ELEMENT_TEXT: 'JSONB_ARRAY_ELEMENT_TEXT' | 'jsonb_array_element_text';
INTSPAN_MAKE: 'INTSPAN_MAKE' | 'intspan_make';
FLOATSPAN_MAKE: 'FLOATSPAN_MAKE' | 'floatspan_make';
INTSPAN_LOWER: 'INTSPAN_LOWER' | 'intspan_lower';
INTSPAN_UPPER: 'INTSPAN_UPPER' | 'intspan_upper';
INTSPAN_WIDTH: 'INTSPAN_WIDTH' | 'intspan_width';
INTSPAN_LOWER_INC: 'INTSPAN_LOWER_INC' | 'intspan_lower_inc';
INTSPAN_UPPER_INC: 'INTSPAN_UPPER_INC' | 'intspan_upper_inc';
FLOATSPAN_LOWER: 'FLOATSPAN_LOWER' | 'floatspan_lower';
FLOATSPAN_UPPER: 'FLOATSPAN_UPPER' | 'floatspan_upper';
FLOATSPAN_WIDTH: 'FLOATSPAN_WIDTH' | 'floatspan_width';
FLOATSPAN_LOWER_INC: 'FLOATSPAN_LOWER_INC' | 'floatspan_lower_inc';
FLOATSPAN_UPPER_INC: 'FLOATSPAN_UPPER_INC' | 'floatspan_upper_inc';
CONTAINED_INT_SPAN: 'CONTAINED_INT_SPAN' | 'contained_int_span';
CONTAINED_FLOAT_SPAN: 'CONTAINED_FLOAT_SPAN' | 'contained_float_span';
CONTAINS_SPAN_INT: 'CONTAINS_SPAN_INT' | 'contains_span_int';
CONTAINS_SPAN_FLOAT: 'CONTAINS_SPAN_FLOAT' | 'contains_span_float';
CONTAINED_SPAN_SPAN: 'CONTAINED_SPAN_SPAN' | 'contained_intspan_intspan';
CONTAINS_SPAN_SPAN: 'CONTAINS_SPAN_SPAN' | 'contains_intspan_intspan';
CONTAINED_FLOATSPAN_SPAN: 'CONTAINED_FLOATSPAN_SPAN' | 'contained_floatspan_floatspan';
CONTAINS_FLOATSPAN_SPAN: 'CONTAINS_FLOATSPAN_SPAN' | 'contains_floatspan_floatspan';
TEXT_UPPER: 'TEXT_UPPER' | 'text_upper';
TEXT_LOWER: 'TEXT_LOWER' | 'text_lower';
TEXT_INITCAP: 'TEXT_INITCAP' | 'text_initcap';
QUADBIN_EQ: 'QUADBIN_EQ' | 'quadbin_eq';
QUADBIN_NE: 'QUADBIN_NE' | 'quadbin_ne';
QUADBIN_LT: 'QUADBIN_LT' | 'quadbin_lt';
QUADBIN_LE: 'QUADBIN_LE' | 'quadbin_le';
QUADBIN_GT: 'QUADBIN_GT' | 'quadbin_gt';
QUADBIN_GE: 'QUADBIN_GE' | 'quadbin_ge';
QUADBIN_CMP: 'QUADBIN_CMP' | 'quadbin_cmp';
EVER_EQ_QUADBIN_TQUADBIN: 'EVER_EQ_QUADBIN_TQUADBIN' | 'ever_eq_quadbin_tquadbin';
EVER_NE_QUADBIN_TQUADBIN: 'EVER_NE_QUADBIN_TQUADBIN' | 'ever_ne_quadbin_tquadbin';
ALWAYS_EQ_QUADBIN_TQUADBIN: 'ALWAYS_EQ_QUADBIN_TQUADBIN' | 'always_eq_quadbin_tquadbin';
ALWAYS_NE_QUADBIN_TQUADBIN: 'ALWAYS_NE_QUADBIN_TQUADBIN' | 'always_ne_quadbin_tquadbin';
EVER_EQ_TQUADBIN_QUADBIN: 'EVER_EQ_TQUADBIN_QUADBIN' | 'ever_eq_tquadbin_quadbin';
EVER_NE_TQUADBIN_QUADBIN: 'EVER_NE_TQUADBIN_QUADBIN' | 'ever_ne_tquadbin_quadbin';
ALWAYS_EQ_TQUADBIN_QUADBIN: 'ALWAYS_EQ_TQUADBIN_QUADBIN' | 'always_eq_tquadbin_quadbin';
ALWAYS_NE_TQUADBIN_QUADBIN: 'ALWAYS_NE_TQUADBIN_QUADBIN' | 'always_ne_tquadbin_quadbin';
EVER_EQ_TQUADBIN_TQUADBIN: 'EVER_EQ_TQUADBIN_TQUADBIN' | 'ever_eq_tquadbin_tquadbin';
EVER_NE_TQUADBIN_TQUADBIN: 'EVER_NE_TQUADBIN_TQUADBIN' | 'ever_ne_tquadbin_tquadbin';
ALWAYS_EQ_TQUADBIN_TQUADBIN: 'ALWAYS_EQ_TQUADBIN_TQUADBIN' | 'always_eq_tquadbin_tquadbin';
ALWAYS_NE_TQUADBIN_TQUADBIN: 'ALWAYS_NE_TQUADBIN_TQUADBIN' | 'always_ne_tquadbin_tquadbin';
EINTERSECTS_TRGEOMETRY_GEO: 'EINTERSECTS_TRGEOMETRY_GEO' | 'eintersects_trgeometry_geo';
AINTERSECTS_TRGEOMETRY_GEO: 'AINTERSECTS_TRGEOMETRY_GEO' | 'aintersects_trgeometry_geo';
EDISJOINT_TRGEOMETRY_GEO: 'EDISJOINT_TRGEOMETRY_GEO' | 'edisjoint_trgeometry_geo';
ADISJOINT_TRGEOMETRY_GEO: 'ADISJOINT_TRGEOMETRY_GEO' | 'adisjoint_trgeometry_geo';
ETOUCHES_TRGEOMETRY_GEO: 'ETOUCHES_TRGEOMETRY_GEO' | 'etouches_trgeometry_geo';
ATOUCHES_TRGEOMETRY_GEO: 'ATOUCHES_TRGEOMETRY_GEO' | 'atouches_trgeometry_geo';
ECOVERS_TRGEOMETRY_GEO: 'ECOVERS_TRGEOMETRY_GEO' | 'ecovers_trgeometry_geo';
ACOVERS_TRGEOMETRY_GEO: 'ACOVERS_TRGEOMETRY_GEO' | 'acovers_trgeometry_geo';
EDWITHIN_TRGEOMETRY_GEO: 'EDWITHIN_TRGEOMETRY_GEO' | 'edwithin_trgeometry_geo';
ADWITHIN_TRGEOMETRY_GEO: 'ADWITHIN_TRGEOMETRY_GEO' | 'adwithin_trgeometry_geo';
ECONTAINS_GEO_TRGEOMETRY: 'ECONTAINS_GEO_TRGEOMETRY' | 'econtains_geo_trgeometry';
ACONTAINS_GEO_TRGEOMETRY: 'ACONTAINS_GEO_TRGEOMETRY' | 'acontains_geo_trgeometry';
ECOVERS_GEO_TRGEOMETRY: 'ECOVERS_GEO_TRGEOMETRY' | 'ecovers_geo_trgeometry';
ACOVERS_GEO_TRGEOMETRY: 'ACOVERS_GEO_TRGEOMETRY' | 'acovers_geo_trgeometry';
GEOM_RELATE_PATTERN: 'GEOM_RELATE_PATTERN' | 'geom_relate_pattern';
GEOM_INTERSECTS2D: 'GEOM_INTERSECTS2D' | 'geom_intersects2d';
GEOM_DWITHIN2D: 'GEOM_DWITHIN2D' | 'geom_dwithin2d';
GEOM_CONTAINS: 'GEOM_CONTAINS' | 'geom_contains';
GEOM_DISJOINT2D: 'GEOM_DISJOINT2D' | 'geom_disjoint2d';
GEOM_COVERS: 'GEOM_COVERS' | 'geom_covers';
GEOM_TOUCHES: 'GEOM_TOUCHES' | 'geom_touches';
GEOM_INTERSECTS3D: 'GEOM_INTERSECTS3D' | 'geom_intersects3d';
GEOM_DWITHIN3D: 'GEOM_DWITHIN3D' | 'geom_dwithin3d';
GEOM_DISTANCE2D: 'GEOM_DISTANCE2D' | 'geom_distance2d';
GEOM_DISTANCE3D: 'GEOM_DISTANCE3D' | 'geom_distance3d';
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
