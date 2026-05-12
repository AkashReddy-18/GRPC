#pragma once

#include<string>
#include<map>
#include<functional>
#include<stdexcept>
#include <cstdint>

// Implementation of MurmurHash3 (32-bit)
inline uint32_t MurmurHash3_x86_32(const void* key, int len, uint32_t seed) {
    const uint8_t* data = (const uint8_t*)key;
    const int nblocks = len / 4;

    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    const uint32_t* blocks = (const uint32_t*)(data + nblocks * 4);

    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];

        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> (32 - 15));
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> (32 - 13));
        h1 = h1 * 5 + 0xe6546b64;
    }

    const uint8_t* tail = (const uint8_t*)(data + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3) {
    case 3: k1 ^= tail[2] << 16;
    case 2: k1 ^= tail[1] << 8;
    case 1: k1 ^= tail[0];
            k1 *= c1; k1 = (k1 << 15) | (k1 >> (32 - 15)); k1 *= c2; h1 ^= k1;
    }

    h1 ^= len;
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

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
        return MurmurHash3_x86_32(key.c_str(), key.length(), 0x12345678);
    }
};