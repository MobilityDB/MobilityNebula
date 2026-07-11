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
add_plugin(AcoversGeoTgeo PhysicalFunction nes-physical-operators AcoversGeoTgeoPhysicalFunction.cpp)
add_plugin(AdisjointGeoTgeo PhysicalFunction nes-physical-operators AdisjointGeoTgeoPhysicalFunction.cpp)
add_plugin(AdwithinGeoTgeo PhysicalFunction nes-physical-operators AdwithinGeoTgeoPhysicalFunction.cpp)
add_plugin(AintersectsGeoTgeo PhysicalFunction nes-physical-operators AintersectsGeoTgeoPhysicalFunction.cpp)
add_plugin(AtouchesGeoTgeo PhysicalFunction nes-physical-operators AtouchesGeoTgeoPhysicalFunction.cpp)
add_plugin(EdisjointGeoTgeo PhysicalFunction nes-physical-operators EdisjointGeoTgeoPhysicalFunction.cpp)
add_plugin(EdwithinGeoTgeo PhysicalFunction nes-physical-operators EdwithinGeoTgeoPhysicalFunction.cpp)
add_plugin(EintersectsGeoTgeo PhysicalFunction nes-physical-operators EintersectsGeoTgeoPhysicalFunction.cpp)
add_plugin(EtouchesGeoTgeo PhysicalFunction nes-physical-operators EtouchesGeoTgeoPhysicalFunction.cpp)
endif()
