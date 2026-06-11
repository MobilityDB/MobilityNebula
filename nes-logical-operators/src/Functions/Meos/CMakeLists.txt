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
add_plugin(TemporalEDisjointGeometry LogicalFunction nes-logical-operators TemporalEDisjointGeometryLogicalFunction.cpp)
add_plugin(TemporalATouchesGeometry LogicalFunction nes-logical-operators TemporalATouchesGeometryLogicalFunction.cpp)
add_plugin(TemporalECoversGeometry LogicalFunction nes-logical-operators TemporalECoversGeometryLogicalFunction.cpp)
add_plugin(TemporalAContainsGeometry LogicalFunction nes-logical-operators TemporalAContainsGeometryLogicalFunction.cpp)
add_plugin(TemporalETouchesGeometry LogicalFunction nes-logical-operators TemporalETouchesGeometryLogicalFunction.cpp)
add_plugin(TemporalADisjointGeometry LogicalFunction nes-logical-operators TemporalADisjointGeometryLogicalFunction.cpp)
add_plugin(TemporalEIntersectsGeometry LogicalFunction nes-logical-operators TemporalEIntersectsGeometryLogicalFunction.cpp)
add_plugin(TemporalEContainsTGeometry LogicalFunction nes-logical-operators TemporalEContainsTGeometryLogicalFunction.cpp)
add_plugin(TemporalECoversTGeometry LogicalFunction nes-logical-operators TemporalECoversTGeometryLogicalFunction.cpp)
add_plugin(TemporalEDisjointTGeometry LogicalFunction nes-logical-operators TemporalEDisjointTGeometryLogicalFunction.cpp)
add_plugin(TemporalEIntersectsTGeometry LogicalFunction nes-logical-operators TemporalEIntersectsTGeometryLogicalFunction.cpp)
add_plugin(TemporalETouchesTGeometry LogicalFunction nes-logical-operators TemporalETouchesTGeometryLogicalFunction.cpp)
add_plugin(TemporalAContainsTGeometry LogicalFunction nes-logical-operators TemporalAContainsTGeometryLogicalFunction.cpp)
add_plugin(TemporalADisjointTGeometry LogicalFunction nes-logical-operators TemporalADisjointTGeometryLogicalFunction.cpp)
add_plugin(TemporalAIntersectsTGeometry LogicalFunction nes-logical-operators TemporalAIntersectsTGeometryLogicalFunction.cpp)
add_plugin(TemporalATouchesTGeometry LogicalFunction nes-logical-operators TemporalATouchesTGeometryLogicalFunction.cpp)
