# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Intersect LogicalFunction nes-logical-operators IntersectLogicalFunction.cpp)
#add_plugin(TemporalDWithin LogicalFunction nes-logical-operators TemporalDWithinFunction.cpp)
add_plugin(TemporalIntersects LogicalFunction nes-logical-operators TemporalIntersectsFunction.cpp)
add_plugin(TemporalIntersectsGeometry LogicalFunction nes-logical-operators TemporalIntersectsGeometryLogicalFunction.cpp)
add_plugin(TemporalAIntersectsGeometry LogicalFunction nes-logical-operators TemporalAIntersectsGeometryLogicalFunction.cpp)
add_plugin(TemporalEContainsGeometry LogicalFunction nes-logical-operators TemporalEContainsGeometryLogicalFunction.cpp)
add_plugin(TemporalEDWithinGeometry LogicalFunction nes-logical-operators TemporalEDWithinGeometryLogicalFunction.cpp)
add_plugin(TemporalAtStBox LogicalFunction nes-logical-operators TemporalAtStBoxLogicalFunction.cpp)
add_plugin(TgeompointToTnpoint LogicalFunction nes-logical-operators TgeompointToTnpointLogicalFunction.cpp)
add_plugin(TposeAngularSpeed LogicalFunction nes-logical-operators TposeAngularSpeedLogicalFunction.cpp)
add_plugin(TposePitch LogicalFunction nes-logical-operators TposePitchLogicalFunction.cpp)
add_plugin(TposeRoll LogicalFunction nes-logical-operators TposeRollLogicalFunction.cpp)
add_plugin(TposeSpeed LogicalFunction nes-logical-operators TposeSpeedLogicalFunction.cpp)
add_plugin(TposeYaw LogicalFunction nes-logical-operators TposeYawLogicalFunction.cpp)
add_plugin(TrgeometryCentroid LogicalFunction nes-logical-operators TrgeometryCentroidLogicalFunction.cpp)
add_plugin(TrgeometryCumulativeLength LogicalFunction nes-logical-operators TrgeometryCumulativeLengthLogicalFunction.cpp)
add_plugin(TrgeometrySpeed LogicalFunction nes-logical-operators TrgeometrySpeedLogicalFunction.cpp)
add_plugin(TrgeometryToTgeometry LogicalFunction nes-logical-operators TrgeometryToTgeometryLogicalFunction.cpp)
add_plugin(TfloatCos LogicalFunction nes-logical-operators TfloatCosLogicalFunction.cpp)
add_plugin(TfloatSin LogicalFunction nes-logical-operators TfloatSinLogicalFunction.cpp)
add_plugin(TfloatTan LogicalFunction nes-logical-operators TfloatTanLogicalFunction.cpp)
add_plugin(TfloatToTbigint LogicalFunction nes-logical-operators TfloatToTbigintLogicalFunction.cpp)
add_plugin(TintToTbigint LogicalFunction nes-logical-operators TintToTbigintLogicalFunction.cpp)
