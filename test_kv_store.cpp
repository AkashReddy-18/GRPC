#include <gtest/gtest.h>
#include "KV_store.h"

TEST(KVStoreTest, PutAndGet) {
    kv_store store;
    store.put("key1", "value1");
    auto val = store.get("key1");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), "value1");
}

TEST(KVStoreTest, GetNonExistent) {
    kv_store store;
    auto val = store.get("nonexistent");
    EXPECT_FALSE(val.has_value());
}

TEST(KVStoreTest, DeleteKey) {
    kv_store store;
    store.put("key1", "value1");
    EXPECT_TRUE(store.del("key1"));
    auto val = store.get("key1");
    EXPECT_FALSE(val.has_value());
}

TEST(KVStoreTest, DeleteNonExistent) {
    kv_store store;
    EXPECT_FALSE(store.del("nonexistent"));
}

TEST(KVStoreTest, OverwriteKey) {
    kv_store store;
    store.put("key1", "value1");
    store.put("key1", "value2");
    auto val = store.get("key1");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), "value2");
}
