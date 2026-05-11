#include "KV_store.h"
#include <mutex>

void kv_store::put(const std::string &key, const std::string &val)
{
    std::unique_lock<std::shared_mutex> lock(rw_lock);
    store_[key] = val; // Phase 1: Basic storage logic
}

std::optional<std::string> kv_store::get(const std::string& key) const
{
    std::shared_lock<std::shared_mutex> lock(rw_lock);
    auto it = store_.find(key);
    if (it != store_.end())
    {
        return it->second;
    }
    return std::nullopt;
}

bool kv_store::del(const std::string &key)
{
    std::unique_lock<std::shared_mutex> lock(rw_lock);
    return store_.erase(key) > 0;
}
