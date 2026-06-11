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
add_plugin(TemporalEDisjointGeometry PhysicalFunction nes-physical-operators TemporalEDisjointGeometryPhysicalFunction.cpp)
add_plugin(TemporalATouchesGeometry PhysicalFunction nes-physical-operators TemporalATouchesGeometryPhysicalFunction.cpp)
add_plugin(TemporalECoversGeometry PhysicalFunction nes-physical-operators TemporalECoversGeometryPhysicalFunction.cpp)
add_plugin(TemporalAContainsGeometry PhysicalFunction nes-physical-operators TemporalAContainsGeometryPhysicalFunction.cpp)
add_plugin(TemporalETouchesGeometry PhysicalFunction nes-physical-operators TemporalETouchesGeometryPhysicalFunction.cpp)
add_plugin(TemporalADisjointGeometry PhysicalFunction nes-physical-operators TemporalADisjointGeometryPhysicalFunction.cpp)
add_plugin(TemporalEIntersectsGeometry PhysicalFunction nes-physical-operators TemporalEIntersectsGeometryPhysicalFunction.cpp)
endif()
