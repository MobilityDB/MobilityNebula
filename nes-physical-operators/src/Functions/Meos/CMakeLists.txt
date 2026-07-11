# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(NES_ENABLE_MEOS)
add_plugin(TemporalIntersects PhysicalFunction nes-physical-operators TemporalIntersectsPhysicalFunction.cpp)
add_plugin(TemporalIntersectsGeometry PhysicalFunction nes-physical-operators TemporalIntersectsGeometryPhysicalFunction.cpp)
add_plugin(TemporalAIntersectsGeometry PhysicalFunction nes-physical-operators TemporalAIntersectsGeometryPhysicalFunction.cpp)
add_plugin(TemporalEContainsGeometry PhysicalFunction nes-physical-operators TemporalEContainsGeometryPhysicalFunction.cpp)
add_plugin(TemporalEDWithinGeometry PhysicalFunction nes-physical-operators TemporalEDWithinGeometryPhysicalFunction.cpp)
add_plugin(TemporalAtStBox PhysicalFunction nes-physical-operators TemporalAtStBoxPhysicalFunction.cpp)
add_plugin(TgeompointToTnpoint PhysicalFunction nes-physical-operators TgeompointToTnpointPhysicalFunction.cpp)
add_plugin(TposeAngularSpeed PhysicalFunction nes-physical-operators TposeAngularSpeedPhysicalFunction.cpp)
add_plugin(TposePitch PhysicalFunction nes-physical-operators TposePitchPhysicalFunction.cpp)
add_plugin(TposeRoll PhysicalFunction nes-physical-operators TposeRollPhysicalFunction.cpp)
add_plugin(TposeSpeed PhysicalFunction nes-physical-operators TposeSpeedPhysicalFunction.cpp)
add_plugin(TposeYaw PhysicalFunction nes-physical-operators TposeYawPhysicalFunction.cpp)
add_plugin(TrgeometryCentroid PhysicalFunction nes-physical-operators TrgeometryCentroidPhysicalFunction.cpp)
add_plugin(TrgeometryCumulativeLength PhysicalFunction nes-physical-operators TrgeometryCumulativeLengthPhysicalFunction.cpp)
add_plugin(TrgeometrySpeed PhysicalFunction nes-physical-operators TrgeometrySpeedPhysicalFunction.cpp)
add_plugin(TrgeometryToTgeometry PhysicalFunction nes-physical-operators TrgeometryToTgeometryPhysicalFunction.cpp)
add_plugin(TfloatCos PhysicalFunction nes-physical-operators TfloatCosPhysicalFunction.cpp)
add_plugin(TfloatSin PhysicalFunction nes-physical-operators TfloatSinPhysicalFunction.cpp)
add_plugin(TfloatTan PhysicalFunction nes-physical-operators TfloatTanPhysicalFunction.cpp)
add_plugin(TfloatToTbigint PhysicalFunction nes-physical-operators TfloatToTbigintPhysicalFunction.cpp)
add_plugin(TintToTbigint PhysicalFunction nes-physical-operators TintToTbigintPhysicalFunction.cpp)
endif()
