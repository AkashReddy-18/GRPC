#pragma once

#include<string>
#include<map>
#include<functional>
#include<stdexcept>
#include <cstdint>

class consistent_hash // Phase 4: Data structure for horizontal scaling and sharding
{
public:
    explicit consistent_hash(int num_vnodes=100): vnodes(num_vnodes){}
    ~consistent_hash()=default;

    void add_server(const std::string &server_node)
    {
        for(int i=0;i<vnodes;i++)
        {
            std::string vnode_key=server_node+"#"+std::to_string(i);
            uint32_t hash_val=Hash(vnode_key);
            ring[hash_val]=server_node;
        }
    }

    void remove_server(const std::string &server_node)
    {
        for(int i=0;i<vnodes;i++)
        {
            std::string vnode_key=server_node+"#"+ std::to_string(i);
            uint32_t hash_val=Hash(vnode_key);
            ring.erase(hash_val);
        }
    }

    std:: string get_server(const std::string &key)const
    {
        if(ring.empty())
        {
            throw std::runtime_error("Consistent Hashing Ring is empty!");
        }
        uint32_t hash_val = Hash(key);
        auto it = ring.lower_bound(hash_val);
        if (it == ring.end()) {
            it = ring.begin();
        }
        return it->second;
    }

    size_t get_ring_size() const 
    {
        return ring.size();
    }

private:
    int vnodes;
    std::map<uint32_t,std::string> ring;
    uint32_t Hash(const std::string &key)const
    {
        std::hash<std::string> hasher;
        return static_cast<uint32_t>(hasher(key));
    }
};