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

#ifndef NES_PLUGINS_MEOS_HPP
#define NES_PLUGINS_MEOS_HPP

#include <string>

extern "C" {
    #include <meos.h>
    #include <meos_geo.h>
}

namespace MEOS {

class Meos {
  public:
    /**
     * @brief Initialize MEOS library
     */
    Meos();

    /**
     * @brief Finalize MEOS library, free the timezone cache
     */
    ~Meos();

    class SpatioTemporalBox {
    public:
        /**
         * @brief Create SpatioTemporal from WKT string
         * @param wkt_string String in format "SRID=4326;SpatioTemporal X((3.5, 50.5),(4.5, 51.5))"
         */
        explicit SpatioTemporalBox(const std::string& wkt_string);
        ~SpatioTemporalBox();

        // Non-copyable, movable to avoid double-free of MEOS-managed memory
        SpatioTemporalBox(const SpatioTemporalBox&) = delete;
        SpatioTemporalBox& operator=(const SpatioTemporalBox&) = delete;
        SpatioTemporalBox(SpatioTemporalBox&& other) noexcept : stbox_ptr(other.stbox_ptr) { other.stbox_ptr = nullptr; }
        SpatioTemporalBox& operator=(SpatioTemporalBox&& other) noexcept {
            if (this != &other) { stbox_ptr = other.stbox_ptr; other.stbox_ptr = nullptr; }
            return *this;
        }

        STBox* getBox() const;


    private:
        void* stbox_ptr;
    };

    class StaticGeometry {
    public:
        explicit StaticGeometry(const std::string& wkt_string);
        ~StaticGeometry();

        StaticGeometry(const StaticGeometry&) = delete;
        StaticGeometry& operator=(const StaticGeometry&) = delete;
        StaticGeometry(StaticGeometry&& other) noexcept : geometry(other.geometry) { other.geometry = nullptr; }
        StaticGeometry& operator=(StaticGeometry&& other) noexcept { if (this!=&other){ geometry = other.geometry; other.geometry=nullptr;} return *this; }

        GSERIALIZED* getGeometry() const;


    private:
        GSERIALIZED* geometry;
    };

    
    class TemporalHolder {
    public:
        explicit TemporalHolder(Temporal* temporalPtr);
        ~TemporalHolder();

        TemporalHolder(const TemporalHolder&) = delete;
        TemporalHolder& operator=(const TemporalHolder&) = delete;
        TemporalHolder(TemporalHolder&& other) noexcept : temporal(other.temporal) { other.temporal = nullptr; }
        TemporalHolder& operator=(TemporalHolder&& other) noexcept { if (this!=&other){ temporal = other.temporal; other.temporal=nullptr;} return *this; }

        Temporal* get() const;

    private:
        Temporal* temporal;
    };


    // Thread-safe wrappers around selected MEOS functions to avoid internal races
    static int safe_edwithin_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs, double dist);
    static int safe_eintersects_tgeo_geo(const Temporal* temp, const GSERIALIZED* gs);
    static Temporal* safe_tgeo_at_stbox(const Temporal* temp, const STBox* box, bool border_inc);
    
    /**
     * @brief Parse a temporal point string into a MEOS Temporal object
     * @param trajStr String representation of temporal point (e.g., "{Point(1.0 2.0)@2023-01-01 00:00:00}")
     * @return Void pointer to Temporal object, nullptr on failure. Caller must free with freeTemporalObject()
     */
    static void* parseTemporalPoint(const std::string& trajStr);
    
    /**
     * @brief Free a MEOS Temporal object
     * @param temporal Pointer to Temporal object to free
     */
    static void freeTemporalObject(void* temporal);
    
    /**
     * @brief Convert a Temporal object to WKB format
     * @param temporal Pointer to Temporal object
     * @param size Output parameter for the size of the WKB data
     * @return Pointer to WKB data, nullptr on failure. Caller must free the returned pointer.
     */
    static uint8_t* temporalToWKB(void* temporal, size_t& size);
    
    /**
     * @brief Ensure MEOS is initialized
     */
    static void ensureMeosInitialized();
    
    bool finalized=false;

};

}
#endif // NES_PLUGINS_MEOS_HPP
