#include <Functions/Meos/TemporalEContainsGeometryPhysicalFunction.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <MEOSWrapper.hpp>
#include <fmt/format.h>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <ErrorHandling.hpp>
#include <iostream>

namespace NES {

/* ─────────── constructors ─────────── */

TemporalEContainsGeometryPhysicalFunction::
TemporalEContainsGeometryPhysicalFunction(PhysicalFunction param1,
                                         PhysicalFunction param2,
                                         PhysicalFunction param3,
                                         PhysicalFunction param4)
    : paramFns{std::move(param1),std::move(param2),std::move(param3),std::move(param4)} {
}

TemporalEContainsGeometryPhysicalFunction::
TemporalEContainsGeometryPhysicalFunction(PhysicalFunction lon1,
                                         PhysicalFunction lat1,
                                         PhysicalFunction ts1,
                                         PhysicalFunction lon2,
                                         PhysicalFunction lat2,
                                         PhysicalFunction ts2)
    : paramFns{std::move(lon1),std::move(lat1),std::move(ts1),
               std::move(lon2),std::move(lat2),std::move(ts2)} {
}

/* ─────────── dispatch ─────────── */

VarVal TemporalEContainsGeometryPhysicalFunction::execute(const Record& rec,
                                                          ArenaRef& arena) const {
    std::cout << "TemporalEContainsGeometryPhysicalFunction::execute called with " << paramFns.size() << " arguments" << std::endl;
    std::vector<VarVal> vals; vals.reserve(paramFns.size());
    for(auto& f:paramFns){ 
        vals.push_back(f.execute(rec, arena));
    }
    
    // For 4-parameter case, determine mode based on first parameter type at compile-time
    if (paramFns.size() == 4) {
        // Use customVisit to determine type at compile-time and dispatch accordingly
        return vals[0].customVisit([this, &vals](const auto& val) -> VarVal {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, VariableSizedData>) {
                return execStaticTemporal(vals);  // static, lon, lat, ts
            } else {
                return execTemporalStatic(vals);  // lon, lat, ts, static
            }
        });
    }
    
    // 6-parameter case is always temporal-temporal
    return execTemporalTemporal(vals);
}

/* ─────────── helpers ─────────── */

VarVal
TemporalEContainsGeometryPhysicalFunction::execTemporalTemporal(const std::vector<VarVal>& p) const {
    auto lon1 = p[0].cast<nautilus::val<double>>();
    auto lat1 = p[1].cast<nautilus::val<double>>();
    auto ts1  = p[2].cast<nautilus::val<uint64_t>>();
    auto lon2 = p[3].cast<nautilus::val<double>>();
    auto lat2 = p[4].cast<nautilus::val<double>>();
    auto ts2  = p[5].cast<nautilus::val<uint64_t>>();

    std::cout << "6-param temporal-temporal contains function with coordinate values" << std::endl;


    const auto res = nautilus::invoke(
        +[](double lo1,double la1,uint64_t t1, double lo2, double la2, uint64_t t2) -> int {
            try {
                MEOS::Meos::ensureMeosInitialized();
                auto inRange = [](double lo, double la){ return lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0; };
                if (!inRange(lo1, la1) || !inRange(lo2, la2)) {
                    std::cout << "TemporalEContains: coordinates out of range" << std::endl;
                    return 0;
                }
                auto tsToStr = MEOS::Meos::convertEpochToTimestamp;
                std::string left  = fmt::format("SRID=4326;Point({} {})@{}", lo1, la1, tsToStr(t1));
                std::string right = fmt::format("SRID=4326;Point({} {})@{}", lo2, la2, tsToStr(t2));
                MEOS::Meos::TemporalGeometry l(left), r(right);
                return l.contains(r);
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry contains: " << e.what() << std::endl;
                return -1;  // Error case
            } catch (...) {
                std::cout << "Unknown error in temporal geometry contains" << std::endl;
                return -1;  // Error case
            }
        }, lon1,lat1,ts1,lon2,lat2,ts2
    );

    return VarVal(res);
}

VarVal
TemporalEContainsGeometryPhysicalFunction::execTemporalStatic(const std::vector<VarVal>& p) const 
{
    auto lon  = p[0].cast<nautilus::val<double>>();
    auto lat  = p[1].cast<nautilus::val<double>>();
    auto ts   = p[2].cast<nautilus::val<uint64_t>>();
    auto stat = p[3].cast<VariableSizedData>();
    std::cout << "4-param temporal-static contains function with coordinate values" << std::endl;


    const auto res = nautilus::invoke(
        +[](double lo,double la,uint64_t t, const char* g, uint32_t sz) -> int {
            try {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0)) {
                    std::cout << "TemporalEContains: coordinates out of range" << std::endl;
                    return 0;
                }
                std::string tsStr = MEOS::Meos::convertEpochToTimestamp(t);
                std::string left  = fmt::format("SRID=4326;Point({} {})@{}", lo, la, tsStr);
                std::string right(g, sz);
                while (!right.empty() && (right.front() == '\'' || right.front() == '"')) {
                    right = right.substr(1);
                }
                while (!right.empty() && (right.back() == '\'' || right.back() == '"')) {
                    right = right.substr(0, right.size() - 1);
                }
                std::cout << "Built geometries:" << std::endl;
                std::cout << "Left (temporal): " << left << std::endl;
                std::cout << "Right (static): " << right << std::endl;

                // Validate input strings are not empty
                if (left.empty() || right.empty()) {
                    std::cout << "Empty geometry WKT string(s)" << std::endl;
                    return -1;
                }
                std::cout << "Using temporal-static contains function (econtains_tgeo_geo)" << std::endl;

                MEOS::Meos::TemporalGeometry  l(left);
                if (!l.getGeometry()) {
                    std::cout << "TemporalEContains: MEOS temporal geometry is null" << std::endl;
                    return 0;
                }
                MEOS::Meos::StaticGeometry    r(right);
                if (!r.getGeometry()) {
                    std::cout << "TemporalEContains: MEOS static geometry is null" << std::endl;
                    return 0;
                }
                int contains_result = l.containsStatic(r);
                std::cout << "econtains_tgeo_geo result: " << contains_result << std::endl;
                return contains_result;
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry contains: " << e.what() << std::endl;
                return -1;  // Error case
            } catch (...) {
                std::cout << "Unknown error in temporal geometry contains" << std::endl;
                return -1;  // Error case
            }
    }, lon,lat,ts, stat.getContent(), stat.getContentSize());

    return VarVal(res);
}

VarVal
TemporalEContainsGeometryPhysicalFunction::execStaticTemporal(const std::vector<VarVal>& p) const
{
    auto stat = p[0].cast<VariableSizedData>();
    auto lon  = p[1].cast<nautilus::val<double>>();
    auto lat  = p[2].cast<nautilus::val<double>>();
    auto ts   = p[3].cast<nautilus::val<uint64_t>>();
    std::cout << "4-param static-temporal contains function with coordinate values" << std::endl;


    const auto res = nautilus::invoke(
        +[](const char* g,uint32_t sz, double lo,double la,uint64_t t) -> int {
            try {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0)) {
                    std::cout << "TemporalEContains: coordinates out of range" << std::endl;
                    return 0;
                }
                std::string tsStr = MEOS::Meos::convertEpochToTimestamp(t);
                std::string right  = fmt::format("SRID=4326;Point({} {})@{}", lo, la, tsStr);
                std::string left(g, sz);
                while (!left.empty() && (left.front() == '\'' || left.front() == '"')) {
                    left = left.substr(1);
                }
                while (!left.empty() && (left.back() == '\'' || left.back() == '"')) {
                    left = left.substr(0, left.size() - 1);
                }
                std::cout << "Built geometries:" << std::endl;
                std::cout << "Left (static): " << left << std::endl;
                std::cout << "Right (temporal): " << right << std::endl;

                // Validate input strings are not empty
                if (left.empty() || right.empty()) {
                    std::cout << "Empty geometry WKT string(s)" << std::endl;
                    return -1;
                }
                std::cout << "Using static-temporal contains function (econtains_geo_tgeo)" << std::endl;

                MEOS::Meos::StaticGeometry l(left);
                if (!l.getGeometry()) {
                    std::cout << "TemporalEContains: MEOS static geometry is null" << std::endl;
                    return 0;
                }
                MEOS::Meos::TemporalGeometry r(right);
                if (!r.getGeometry()) {
                    std::cout << "TemporalEContains: MEOS temporal geometry is null" << std::endl;
                    return 0;
                }
                int contains_result = l.containsTemporal(r);
                std::cout << "econtains_geo_tgeo result: " << contains_result << std::endl;
                return contains_result;
            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry contains: " << e.what() << std::endl;
                return -1;  // Error case
            } catch (...) {
                std::cout << "Unknown error in temporal geometry contains" << std::endl;
                return -1;  // Error case
            }
    }, stat.getContent(), stat.getContentSize(), lon,lat,ts);

    return VarVal(res);
}

/* ─────────── registry ─────────── */

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTemporalEContainsGeometryPhysicalFunction(
        PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments){
    if (physicalFunctionRegistryArguments.childFunctions.size() == 6) {
        return TemporalEContainsGeometryPhysicalFunction(
            physicalFunctionRegistryArguments.childFunctions[0],
            physicalFunctionRegistryArguments.childFunctions[1],
            physicalFunctionRegistryArguments.childFunctions[2],
            physicalFunctionRegistryArguments.childFunctions[3],
            physicalFunctionRegistryArguments.childFunctions[4],
            physicalFunctionRegistryArguments.childFunctions[5]
        );
    }
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size()==4,
                 "TemporalEContainsGeometry expects 4 or 6 child functions, got {}", physicalFunctionRegistryArguments.childFunctions.size());

    return TemporalEContainsGeometryPhysicalFunction(physicalFunctionRegistryArguments.childFunctions[0],physicalFunctionRegistryArguments.childFunctions[1],
                                                    physicalFunctionRegistryArguments.childFunctions[2],physicalFunctionRegistryArguments.childFunctions[3]);
}

} // namespace NES
