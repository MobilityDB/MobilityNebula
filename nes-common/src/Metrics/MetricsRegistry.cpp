/*
    Licensed under the Apache License, Version 2.0 (the "License");
*/

#include <Metrics/MetricsRegistry.hpp>

namespace NES::Metrics
{

MetricsRegistry& MetricsRegistry::instance()
{
    static MetricsRegistry inst;
    return inst;
}

} // namespace NES::Metrics

