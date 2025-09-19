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
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <cstdlib>
#include <cstdint>

// Include MEOS wrapper after standard headers
#include <MEOSWrapper.hpp>



namespace MEOS {

    // Global MEOS initialization
    static bool meos_initialized = false;

    static void cleanupMeos() {
        if (meos_initialized) {
            meos_finalize();
            meos_initialized = false;
        }
    }

    static void ensureMeosInitialized() {
        if (!meos_initialized) {
            // Set timezone to UTC if not set (common issue in Docker)
            if (!std::getenv("TZ")) {
                setenv("TZ", "UTC", 1);
                tzset();
            }
            
            meos_initialize();
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

    std::string Meos::convertSecondsToTimestamp(long long seconds) {
        std::chrono::seconds sec(seconds);
        std::chrono::time_point<std::chrono::system_clock> tp(sec);

        // Convert to time_t for formatting
        std::time_t time = std::chrono::system_clock::to_time_t(tp);

        // Convert to local time
        std::tm local_tm = *std::localtime(&time);

        // Format the time as a string
        std::ostringstream oss;
        oss << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    // TemporalInstant constructor
    Meos::TemporalInstant::TemporalInstant(double lon, double lat, long long ts, int srid) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();
        
        std::string ts_string = Meos::convertSecondsToTimestamp(ts);
        std::string str_pointbuffer = "SRID=" + std::to_string(srid) + ";POINT(" + std::to_string(lon) + " " + std::to_string(lat) + ")@" + ts_string;

        std::cout << "Creating MEOS TemporalInstant from: " << str_pointbuffer << std::endl;

        Temporal *temp = tgeompoint_in(str_pointbuffer.c_str());

        if (temp == nullptr) {
            std::cout << "Failed to parse temporal point with temporal_from_text" << std::endl;
            // Try alternative format or set to null
            instant = nullptr;
        } else {
            instant = temp;
            std::cout << "Successfully created temporal point" << std::endl;
        }
    }


    Meos::TemporalInstant::~TemporalInstant() { 
        if (instant) {
            free(instant); 
        }
    }

    bool Meos::TemporalInstant::intersects(const TemporalInstant& point) const {  
        std::cout << "TemporalInstant::intersects called" << std::endl;
        // Use MEOS eintersects function for temporal points  - this will change 
        bool result = eintersects_tgeo_tgeo((const Temporal *)this->instant, (const Temporal *)point.instant);
        if (result) {
            std::cout << "TemporalInstant intersects" << std::endl;
        }

        return result;
    }


    Meos::TemporalGeometry::TemporalGeometry(const std::string& wkt_string){

        ensureMeosInitialized();

        std::cout << "Creating MEOS TemporalGeometry from: " << wkt_string << std::endl;

        Temporal *temp = tgeometry_in(wkt_string.c_str());

        if (temp == nullptr) {
            std::cout << "Failed to parse temporal geometry with temporal_from_text" << std::endl;
            // Try alternative format or set to null
            geometry = nullptr;
        } else {
            geometry = temp;
            std::cout << "Successfully created temporal geometry" << std::endl;
        }        

    }

    Temporal* Meos::TemporalGeometry::getGeometry() const {
        return geometry;
    }

    Meos::TemporalGeometry::~TemporalGeometry() { 
        if (geometry) {
            free(geometry); 
        }
    }

    int Meos::TemporalGeometry::intersects(const TemporalGeometry& geom) const{
        std::cout << "TemporalGeometry::intersects called" << std::endl;        
        int result = eintersects_tgeo_tgeo((const Temporal *)this->geometry, (const Temporal *)geom.geometry);
        return result;
    }

    int Meos::TemporalGeometry::contains(const TemporalGeometry& geom) const{
        std::cout << "TemporalGeometry::contains called" << std::endl;        
        int result = econtains_tgeo_tgeo((const Temporal *)this->geometry, (const Temporal *)geom.geometry);
        return result;
    }

    // StaticGeometry implementation
    Meos::StaticGeometry::StaticGeometry(const std::string& wkt_string) {
        ensureMeosInitialized();

        std::cout << "Creating MEOS StaticGeometry from: " << wkt_string << std::endl;

        // Use geom_in to parse static WKT geometry (no temporal component)
        geometry = geom_in(wkt_string.c_str(), -1);

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
        if (geometry) {
            free(geometry);
        }
    }

    int Meos::TemporalGeometry::intersectsStatic(const StaticGeometry& static_geom) const {
        std::cout << "TemporalGeometry::intersectsStatic called" << std::endl;
        
        // Use eintersects_tgeo_geo for temporal-static intersection
        int result = eintersects_tgeo_geo((const Temporal*)this->geometry, static_geom.getGeometry());

        return result;
    }

    int Meos::TemporalGeometry::aintersects(const TemporalGeometry& geom) const{
        std::cout << "TemporalGeometry::aintersects called" << std::endl;        
        int result = aintersects_tgeo_tgeo((const Temporal *)this->geometry, (const Temporal *)geom.geometry);
        return result;
    }   

    int Meos::TemporalGeometry::aintersectsStatic(const StaticGeometry& static_geom) const {
        std::cout << "TemporalGeometry::aintersectsStatic called" << std::endl;
        
        // Use aintersects_tgeo_geo for temporal-static intersection
        int result = aintersects_tgeo_geo((const Temporal*)this->geometry, static_geom.getGeometry());

        return result;
    }

    // called if static geometry is the first parameter
    int Meos::StaticGeometry::containsTemporal(const TemporalGeometry& temporal_geom) const {
        std::cout << "StaticGeometry::containsTemporal called" << std::endl;
        int result = econtains_geo_tgeo((const GSERIALIZED*)this->geometry, (const Temporal *)temporal_geom.getGeometry());
        if (result==1) {
            std::cout << "StaticGeometry contains TemporalGeometry" << std::endl;
        }
        else {
            std::cout << "StaticGeometry does NOT contain TemporalGeometry" << std::endl;
        }
        return result;
    }

    // called if temporal geometry is the first parameter
    int Meos::TemporalGeometry::containsStatic(const StaticGeometry& static_geom) const {
        std::cout << "TemporalGeometry::containsStatic called" << std::endl;
        int result = econtains_tgeo_geo((const Temporal *)this->geometry, (const GSERIALIZED*)static_geom.getGeometry());
        if (result==1) {
            std::cout << "TemporalGeometry contains StaticGeometry" << std::endl;
        }
        else {
            std::cout << "TemporalGeometry does NOT contain StaticGeometry" << std::endl;
        }
        return result;
    }


    // Constructor for creating a trajectory from multiple temporal instants
    Meos::TemporalSequence::TemporalSequence(const std::vector<TemporalInstant*>& instants) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();

        sequence = nullptr;
        // TODO:call the aggregation function
        
        //TODO: with the result of the aggregation function, we can create a temporal sequence
        std::cout << "TemporalSequence created from " << instants.size() << " temporal instants" << std::endl;
    }
   


    Meos::TemporalSequence::~TemporalSequence() { 
        if (sequence) {
            // Check if this is a vector of instants or a single temporal object
            // This is a bit hacky but works for our current implementation
            try {
                // Try to interpret as vector first
                auto* instants_vec = reinterpret_cast<std::vector<Temporal*>*>(sequence);
                // Simple heuristic: if the "vector" has a reasonable size, treat as vector
                if (instants_vec && reinterpret_cast<uintptr_t>(instants_vec) > 0x1000) {
                    // Likely a vector pointer
                    for (auto* instant : *instants_vec) {
                        if (instant) free(instant);
                    }
                    delete instants_vec;
                } else {
                    free(sequence);
                }
            } catch (...) {
                free(sequence);
            }
            sequence = nullptr;
        }
    }

    double Meos::TemporalSequence::length(const TemporalInstant& /* instant */) const {
        // Placeholder implementation
        // Using comment to avoid unused parameter warning
        return 0.0;
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
        if (temporal) {
            free(temporal);
        }
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

    // SpatioTemporalBox implementation
    Meos::SpatioTemporalBox::SpatioTemporalBox(const std::string& wkt_string) {
        // Ensure MEOS is initialized
        ensureMeosInitialized();
        // Use MEOS stbox_in function to parse the WKT string
        stbox_ptr = stbox_in(wkt_string.c_str());
    }

    Meos::SpatioTemporalBox::~SpatioTemporalBox() {
        if (stbox_ptr) {
            free(stbox_ptr);
            stbox_ptr = nullptr;
        }
    }


}// namespace MEOS

