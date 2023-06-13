#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>

#include "gen_cpp/BackendService.h"
#include "olap/tablet.h"

namespace doris::cloud {

struct HotspotCounter {
    HotspotCounter(int64_t table_id, int64_t index_id, int64_t partition_id)
            : table_id(table_id), index_id(index_id), partition_id(partition_id) {}
    void make_dot_point();
    uint64_t qpd();
    uint64_t qpw();
    int64_t table_id;
    int64_t index_id;
    int64_t partition_id;
    std::chrono::system_clock::time_point last_access_time;
    std::atomic_uint64_t cur_counter {0};
    std::deque<uint64_t> history_counters;
    std::atomic_uint64_t week_history_counter {0};
    std::atomic_uint64_t day_history_counter {0};
    static inline int64_t time_interval = 1 * 60 * 60;
    static inline int64_t week_counters_size = ((7 * 24 * 60 * 60) / time_interval) - 1;
    static inline int64_t day_counters_size = ((24 * 60 * 60) / time_interval) - 1;
};

using HotspotCounterPtr = std::shared_ptr<HotspotCounter>;

class TabletHotspot {
public:
    static TabletHotspot* instance() {
        static TabletHotspot tablet_hotspot;
        return &tablet_hotspot;
    }
    TabletHotspot();
    ~TabletHotspot();
    void count(const TabletSharedPtr& tablet);
    void get_top_n_hot_partition(std::vector<THotTableMessage>* hot_tables);

private:
    void make_dot_point();

    struct HotspotMap {
        std::mutex mtx;
        std::unordered_map<int64_t, HotspotCounterPtr> map;
    };
    static constexpr size_t s_slot_size = 1024;
    std::array<HotspotMap, s_slot_size> _tablets_hotspot;
    std::thread _counter_thread;
    bool _closed {false};
    std::mutex _mtx;
    std::condition_variable _cond;
};

} // namespace doris::cloud
