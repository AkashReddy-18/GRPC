#pragma once
#include <vector>
#include <string>
#include <mutex>
#include <thread>
#include <chrono>
#include <memory>
#include <functional>
#include <unordered_map>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include "KV_store.grpc.pb.h"

enum class RaftState { FOLLOWER, CANDIDATE, LEADER };

class RaftNode : public std::enable_shared_from_this<RaftNode> {
public:
    using ApplyCallback = std::function<void(const kvstore::log_entry&)>;

    RaftNode(const std::string& self_id, const std::vector<std::string>& peer_ids, ApplyCallback callback);
    ~RaftNode();

    // Raft RPC Handlers (called by gRPC server)
    kvstore::vote_response handle_request_vote(const kvstore::vote_request& request);
    kvstore::append_response handle_append_entries(const kvstore::append_request& request);

    // Client Entry Point
    bool propose(const kvstore::log_entry& entry);
    
    bool is_leader() const { 
        std::lock_guard<std::mutex> lock(mu_);
        return state_ == RaftState::LEADER; 
    }
    
    std::string get_leader_id() const { 
        std::lock_guard<std::mutex> lock(mu_);
        return leader_id_; 
    }

private:
    void run_loop(); // Background thread for timers
    void reset_election_timeout();
    void start_election();
    void send_heartbeats();

    // Persistent State
    uint64_t current_term_ = 0;
    std::string voted_for_ = "";
    std::vector<kvstore::log_entry> log_;

    // Volatile State
    int64_t commit_index_ = 0;
    int64_t last_applied_ = 0;
    RaftState state_ = RaftState::FOLLOWER;
    std::string self_id_;
    std::vector<std::string> peers_;
    std::string leader_id_ = "";

    // Leader State
    std::unordered_map<std::string, int64_t> next_index_;
    std::unordered_map<std::string, int64_t> match_index_;

    // Timers
    std::chrono::steady_clock::time_point last_heartbeat_received_;
    std::chrono::steady_clock::time_point last_heartbeat_sent_;
    std::chrono::milliseconds election_timeout_;

    mutable std::mutex mu_;
    std::condition_variable commit_cv_;
    std::thread background_thread_;
    bool running_ = true;
    ApplyCallback apply_callback_;
    std::unordered_map<std::string, std::unique_ptr<kvstore::KeyValueStore::Stub>> stubs_;
};
