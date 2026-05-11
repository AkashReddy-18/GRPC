#pragma once
#include<string>
#include<unordered_map>
#include<optional>
#include<shared_mutex>

class kv_store
{
public:
    kv_store()=default;
    ~kv_store()=default;

    void put(const std::string &key, const std::string& value);
    std::optional<std::string> get(const std::string&key)const;

    bool del(const std::string &key);//del short for delete
private:
    std::unordered_map<std::string, std::string> store_; // Phase 1: Underlying in-memory storage

    mutable std::shared_mutex rw_lock; // Phase 3: Readers-Writer lock for thread safety
};