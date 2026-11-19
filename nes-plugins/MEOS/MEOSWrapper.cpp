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

// Include standard library headers first, before MEOS
#include <string>
#include <vector>
#include <ctime>
#include <iostream>
#include <cstdlib>
#include <mutex>
#include <filesystem>
#include <cstdint>

// Include MEOS wrapper after standard headers
#include <MEOSWrapper.hpp>



namespace MEOS {

    // Global MEOS initialization
    static bool meos_initialized = false;
    static std::mutex meos_init_mutex;
    static std::mutex meos_parse_mutex;
    static std::mutex meos_exec_mutex;

    static void cleanupMeos() {
        if (meos_initialized) {
            meos_finalize();
            meos_initialized = false;
        }
    }

    static void ensureMeosInitialized() {
        std::lock_guard<std::mutex> lk(meos_init_mutex);
        if (!meos_initialized) {
            // Ensure a sane timezone environment before initializing MEOS (uses PostgreSQL tzdb)
            const char* tzEnv = std::getenv("TZ");
            if (!tzEnv || *tzEnv == '\0') {
                setenv("TZ", "UTC", 1);
            }
            // PGTZ is used by the underlying PG timezone code; prefer same value as TZ
            const char* pgtzEnv = std::getenv("PGTZ");
            if (!pgtzEnv || *pgtzEnv == '\0') {
                const char* tzNow = std::getenv("TZ");
                setenv("PGTZ", tzNow ? tzNow : "UTC", 1);
            }
            // Provide a tz database directory if none is set and a common system path exists
            const char* tzdirEnv = std::getenv("TZDIR");
            if (!tzdirEnv || *tzdirEnv == '\0') {
                namespace fs = std::filesystem;
                const char* candidates[] = {"/usr/share/zoneinfo", "/usr/lib/zoneinfo", "/usr/share/lib/zoneinfo"};
                for (const auto* cand : candidates) {
                    std::error_code ec;
                    if (fs::exists(cand, ec) && !ec) {
                        setenv("TZDIR", cand, 1);
                        break;
                    }
                }
            }
            tzset();

            meos_initialize();
            // An error returns the function's failure value with meos_errno set, rather than ending the
            // process as the default handler does: a stream reading a malformed value refuses it and runs on.
            // meos_initialize installs the default handler, so this comes after it.
            meos_initialize_noexit_error_handler();
            meos_initialized = true;
            // Register cleanup function to be called at program exit
            std::atexit(cleanupMeos);
        }
    }

    Meos::Meos() { 
        ensureMeosInitialized();
    }

    Meos::~Meos() { 
        // Do not finalize MEOS here - it should remain initialized for the lifetime of the program
        //we finalize at cleanupMeos
    }

    // StaticGeometry implementation
    Meos::StaticGeometry::StaticGeometry(const std::string& wkt_string) {
        ensureMeosInitialized();

        std::cout << "Creating MEOS StaticGeometry from: " << wkt_string << std::endl;

        // Use geom_in to parse static WKT geometry (no temporal component)
        {
            std::lock_guard<std::mutex> lk(meos_parse_mutex);
            geometry = geom_in(wkt_string.c_str(), -1);
        }

        if (geometry == nullptr) {
            std::cout << "Failed to parse static geometry" << std::endl;
        } else {
            std::cout << "Successfully created static geometry" << std::endl;
        }
    }

    GSERIALIZED* Meos::StaticGeometry::getGeometry() const {
        return geometry;
    }

    Meos::StaticGeometry::~StaticGeometry() {
        // Do not free: avoid an allocator mismatch across builds.
        geometry = nullptr;
    }

    // Static wrapper functions for MEOS API
    void* Meos::parseTemporalPoint(const std::string& trajStr) {
        ensureMeosInitialized();
        
        if (trajStr.empty()) {
            return nullptr;
        }
        
        // Clear any previous errors
        meos_errno_reset();
        
        Temporal* temp = tgeompoint_in(trajStr.c_str());
        if (!temp) {
            // Try with SRID prefix as fallback
            std::string sridStr = "SRID=4326;" + trajStr;
            temp = tgeompoint_in(sridStr.c_str());
        }
        
        return temp;
    }
    
    void Meos::freeTemporalObject(void* temporal) {
        // Intentionally no-op; avoid allocator mismatch.
        (void)temporal;
    }
    
    uint8_t* Meos::temporalToWKB(void* temporal, size_t& size) {
        if (!temporal) {
            size = 0;
            return nullptr;
        }
        
        // Use extended WKB format (0x08)
        uint8_t* data = temporal_as_wkb((Temporal*)temporal, 0x08, &size);
        return data;
    }
    
    void Meos::ensureMeosInitialized() {
        MEOS::ensureMeosInitialized();
    }

    int Meos::safe_edwithin_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs, double dist)
    {
        std::lock_guard<std::mutex> lk(meos_exec_mutex);
        return edwithin_tgeo_geo(temp, gs, dist, true);
    }

    int Meos::safe_eintersects_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs)
    {
        std::lock_guard<std::mutex> lk(meos_exec_mutex);
        return eintersects_tgeo_geo(temp, gs);
    }

    Temporal* Meos::safe_tgeo_at_stbox(const Temporal* temp, const STBox* box, bool border_inc)
    {
        std::lock_guard<std::mutex> lk(meos_exec_mutex);
        return tgeo_at_stbox(temp, box, border_inc);
    }

    // SpatioTemporalBox implementation
    Meos::SpatioTemporalBox::SpatioTemporalBox(const std::string& wkt_string) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();
        // Use MEOS stbox_in function to parse the WKT string
        {
            std::lock_guard<std::mutex> lk(meos_parse_mutex);
            stbox_ptr = stbox_in(wkt_string.c_str());
            if (!stbox_ptr) {
                // Attempt to convert legacy STBOX((x,y,t),(x2,y2,t2)) into STBOX XT(((x,y),(x2,y2)),[t,t2])
                std::string sridPrefix;
                std::string core = wkt_string;
                if (auto semi = core.find(';'); semi != std::string::npos) {
                    sridPrefix = core.substr(0, semi + 1); // keep trailing ';'
                    core = core.substr(semi + 1);
                }
                auto start = core.find("STBOX((");
                auto end = core.rfind(")");
                if (start != std::string::npos && end != std::string::npos && end > start + 8) {
                    std::string inner = core.substr(start + 7, end - (start + 7)); // after 'STBOX('
                    // Expect inner like: (x,y,t),(x2,y2,t2)
                    // Remove possible outer parentheses
                    if (!inner.empty() && inner.front() == '(' && inner.back() == ')') {
                        inner = inner.substr(1, inner.size() - 2);
                    }
                    auto mid = inner.find("),(");
                    if (mid != std::string::npos) {
                        auto first = inner.substr(0, mid);
                        auto second = inner.substr(mid + 3);
                        auto trim = [](std::string& s){
                            while (!s.empty() && (s.front()==' '||s.front()=='\t')) s.erase(s.begin());
                            while (!s.empty() && (s.back()==' '||s.back()=='\t')) s.pop_back();
                            if (!s.empty() && s.front()=='(') s.erase(s.begin());
                            if (!s.empty() && s.back()==')') s.pop_back();
                        };
                        trim(first); trim(second);
                        auto split3 = [](const std::string& s){
                            std::vector<std::string> out; out.reserve(3);
                            size_t c1 = s.find(',');
                            if (c1==std::string::npos) return out;
                            size_t c2 = s.find(',', c1+1);
                            if (c2==std::string::npos) return out;
                            out.push_back(s.substr(0,c1));
                            out.push_back(s.substr(c1+1, c2-(c1+1)));
                            out.push_back(s.substr(c2+1));
                            return out;
                        };
                        auto a = split3(first);
                        auto b = split3(second);
                        if (a.size()==3 && b.size()==3) {
                            auto trimSpaces = [](std::string& s){
                                while (!s.empty() && (s.front()==' '||s.front()=='\t')) s.erase(s.begin());
                                while (!s.empty() && (s.back()==' '||s.back()=='\t')) s.pop_back();
                            };
                            for (auto* v : {&a[0],&a[1],&a[2],&b[0],&b[1],&b[2]}) trimSpaces(*v);
                            std::string xt = sridPrefix + "STBOX XT(((" + a[0] + "," + a[1] + "),(" + b[0] + "," + b[1] + ")), [" + a[2] + ", " + b[2] + "])";
                            stbox_ptr = stbox_in(xt.c_str());
                        }
                    }
                }
            }
        }
    }

    Meos::SpatioTemporalBox::~SpatioTemporalBox() {
        // Do not free; managed by MEOS.
        stbox_ptr = nullptr;
    }

    STBox* Meos::SpatioTemporalBox::getBox() const {
        return static_cast<STBox*>(stbox_ptr);
    }


    Meos::TemporalHolder::TemporalHolder(Temporal* temporalPtr)
        : temporal(temporalPtr) {}

    Meos::TemporalHolder::~TemporalHolder() {
        // Do not free; managed by MEOS.
        temporal = nullptr;
    }

    Temporal* Meos::TemporalHolder::get() const {
        return temporal;
    }


}// namespace MEOS
