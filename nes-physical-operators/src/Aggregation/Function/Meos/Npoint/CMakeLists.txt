# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# MEOS type-family operators, gated by the NPOINT compile option.

add_plugin(TnpointCumulativeLength AggregationPhysicalFunction nes-physical-operators TnpointCumulativeLengthAggregationPhysicalFunction.cpp)
add_plugin(TnpointSpeed AggregationPhysicalFunction nes-physical-operators TnpointSpeedAggregationPhysicalFunction.cpp)
add_plugin(TnpointToTgeompoint AggregationPhysicalFunction nes-physical-operators TnpointToTgeompointAggregationPhysicalFunction.cpp)
add_plugin(NpointUnionTransfn AggregationPhysicalFunction nes-physical-operators NpointUnionTransfnAggregationPhysicalFunction.cpp)
add_plugin(TnpointTcentroidTransfn AggregationPhysicalFunction nes-physical-operators TnpointTcentroidTransfnAggregationPhysicalFunction.cpp)
add_plugin(NpointsetMake AggregationPhysicalFunction nes-physical-operators NpointsetMakeAggregationPhysicalFunction.cpp)
