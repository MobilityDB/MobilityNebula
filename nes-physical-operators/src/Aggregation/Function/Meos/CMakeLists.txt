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
add_plugin(TemporalSequence AggregationPhysicalFunction nes-physical-operators TemporalSequenceAggregationPhysicalFunction.cpp)
add_plugin(TemporalLength AggregationPhysicalFunction nes-physical-operators TemporalLengthAggregationPhysicalFunction.cpp)
add_plugin(Var AggregationPhysicalFunction nes-physical-operators VarAggregationFunction.cpp)
endif()
