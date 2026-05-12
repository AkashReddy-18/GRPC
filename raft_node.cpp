/* Phase 5: Implementation of Raft voting, heartbeats, and log replication */
#include "raft_node.h"
#include <random>
#include <fstream>
#include <iostream>

void RaftNode::persist() {
    std::ofstream out(storage_path_, std::ios::binary);
    if (out.is_open()) {
        out.write(reinterpret_cast<const char*>(&current_term_), sizeof(current_term_));
        
        size_t voted_for_len = voted_for_.length();
        out.write(reinterpret_cast<const char*>(&voted_for_len), sizeof(voted_for_len));
        out.write(voted_for_.c_str(), voted_for_len);

        size_t log_size = log_.size();
        out.write(reinterpret_cast<const char*>(&log_size), sizeof(log_size));
        for (const auto& entry : log_) {
            std::string serialized;
            entry.SerializeToString(&serialized);
            size_t entry_len = serialized.length();
            out.write(reinterpret_cast<const char*>(&entry_len), sizeof(entry_len));
            out.write(serialized.c_str(), entry_len);
        }
    }
}

void RaftNode::read_persist() {
    std::ifstream in(storage_path_, std::ios::binary);
    if (in.is_open()) {
        in.read(reinterpret_cast<char*>(&current_term_), sizeof(current_term_));
        
        size_t voted_for_len;
        in.read(reinterpret_cast<char*>(&voted_for_len), sizeof(voted_for_len));
        voted_for_.resize(voted_for_len);
        in.read(&voted_for_[0], voted_for_len);

        size_t log_size;
        in.read(reinterpret_cast<char*>(&log_size), sizeof(log_size));
        log_.clear();
        for (size_t i = 0; i < log_size; ++i) {
            size_t entry_len;
            in.read(reinterpret_cast<char*>(&entry_len), sizeof(entry_len));
            std::string serialized(entry_len, '\0');
            in.read(&serialized[0], entry_len);
            kvstore::log_entry entry;
            entry.ParseFromString(serialized);
            log_.push_back(entry);
        }
    } else {
        // First run, initialize with dummy entry
        kvstore::log_entry dummy;
        dummy.set_term(0);
        log_.push_back(dummy);
    }
}

RaftNode::RaftNode(const std::string& self_id, const std::vector<std::string>& peer_ids, ApplyCallback callback)
    : self_id_(self_id), peers_(peer_ids), apply_callback_(callback) {
    
    // Create a unique storage path for this node to avoid conflicts on localhost
    std::string safe_id = self_id;
    std::replace(safe_id.begin(), safe_id.end(), ':', '_');
    storage_path_ = "raft_state_" + safe_id + ".bin";

    for (const auto& peer_id : peer_ids) {
        auto channel = grpc::CreateChannel(peer_id, grpc::InsecureChannelCredentials());
        stubs_[peer_id] = kvstore::KeyValueStore::NewStub(channel);
    }

    read_persist();

    reset_election_timeout();
    last_heartbeat_received_ = std::chrono::steady_clock::now();
    last_heartbeat_sent_ = std::chrono::steady_clock::now();
    background_thread_ = std::thread(&RaftNode::run_loop, this);
}

RaftNode::~RaftNode() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
        commit_cv_.notify_all();
    }
    if (background_thread_.joinable()) {
        background_thread_.join();
    }
}

void RaftNode::reset_election_timeout() {
    // 150ms to 300ms random timeout
    static std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(150, 300);
    election_timeout_ = std::chrono::milliseconds(dist(rng));
}

void RaftNode::run_loop() {
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (!running_) break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));

        std::lock_guard<std::mutex> lock(mu_);
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_received_);

        if (state_ != RaftState::LEADER && duration >= election_timeout_) {
            start_election();
        }

        if (state_ == RaftState::LEADER) {
            auto heartbeat_duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_sent_);
            if (heartbeat_duration >= std::chrono::milliseconds(50)) {
                send_heartbeats();
                last_heartbeat_sent_ = now;
            }
        }
    }
}

kvstore::vote_response RaftNode::handle_request_vote(const kvstore::vote_request& request) {
    std::lock_guard<std::mutex> lock(mu_);
    kvstore::vote_response response;
    
    if (request.term() > current_term_) {
        current_term_ = request.term();
        state_ = RaftState::FOLLOWER;
        voted_for_ = "";
        leader_id_ = "";
    }

    response.set_term(current_term_);

    if (request.term() < current_term_) {
        response.set_vote_granted(false);
        return response;
    }

    bool log_ok = false;
    uint64_t last_log_term = log_.back().term();
    int64_t last_log_index = log_.size() - 1;

    if (request.last_log_term() > last_log_term || 
        (request.last_log_term() == last_log_term && request.last_log_index() >= last_log_index)) {
        log_ok = true;
    }

    if ((voted_for_ == "" || voted_for_ == request.candidate_id()) && log_ok) {
        voted_for_ = request.candidate_id();
        response.set_vote_granted(true);
        last_heartbeat_received_ = std::chrono::steady_clock::now();
    } else {
        response.set_vote_granted(false);
    }

    return response;
}

kvstore::append_response RaftNode::handle_append_entries(const kvstore::append_request& request) {
    std::vector<kvstore::log_entry> to_apply;
    kvstore::append_response response;
    {
        std::lock_guard<std::mutex> lock(mu_);
        response.set_term(current_term_);
        response.set_success(false);

        if (request.term() < current_term_) {
            return response;
        }

        if (request.term() > current_term_) {
            current_term_ = request.term();
            state_ = RaftState::FOLLOWER;
            voted_for_ = "";
        }

        last_heartbeat_received_ = std::chrono::steady_clock::now();
        leader_id_ = request.leader_id();
        if (state_ == RaftState::CANDIDATE) {
            state_ = RaftState::FOLLOWER;
        }

        // 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
        if (request.prev_log_index() >= (int64_t)log_.size()) {
            return response;
        }
        if (request.prev_log_index() >= 0 && log_[request.prev_log_index()].term() != request.prev_log_term()) {
            return response;
        }

        // 3. If an existing entry conflicts with a new one
        int64_t current_idx = request.prev_log_index() + 1;
        int entries_idx = 0;
        while (current_idx < (int64_t)log_.size() && entries_idx < request.entries_size()) {
            if (log_[current_idx].term() != request.entries(entries_idx).term()) {
                log_.erase(log_.begin() + current_idx, log_.end());
                break;
            }
            current_idx++;
            entries_idx++;
        }

        // 4. Append new entries
        while (entries_idx < request.entries_size()) {
            log_.push_back(request.entries(entries_idx));
            entries_idx++;
        }

        // 5. Update commitIndex
        if (request.leader_commit() > commit_index_) {
            commit_index_ = std::min(request.leader_commit(), (int64_t)log_.size() - 1);
            commit_cv_.notify_all();
        }

        // Collect entries to apply
        while (last_applied_ < commit_index_) {
            last_applied_++;
            if (last_applied_ > 0) {
                to_apply.push_back(log_[last_applied_]);
            }
        }
        
        response.set_success(true);
        response.set_term(current_term_);
    }

    // Apply outside lock to prevent deadlocks
    for (const auto& entry : to_apply) {
        apply_callback_(entry);
    }
    
    return response;
}

bool RaftNode::propose(const kvstore::log_entry& entry) {
    std::unique_lock<std::mutex> lock(mu_);
    if (state_ != RaftState::LEADER) return false;

    kvstore::log_entry entry_with_term = entry;
    entry_with_term.set_term(current_term_);
    log_.push_back(entry_with_term);
    int64_t index = log_.size() - 1;

    send_heartbeats(); 

    // Wait for commitIndex to reach index or until we are no longer leader
    commit_cv_.wait_for(lock, std::chrono::milliseconds(500), [this, index] {
        return commit_index_ >= index || state_ != RaftState::LEADER || !running_;
    });

    return commit_index_ >= index && state_ == RaftState::LEADER;
}

void RaftNode::start_election() {
    state_ = RaftState::CANDIDATE;
    current_term_++;
    voted_for_ = self_id_;
    last_heartbeat_received_ = std::chrono::steady_clock::now();
    reset_election_timeout();

    std::cout << self_id_ << " starting election for term " << current_term_ << std::endl;

    auto votes_received = std::make_shared<int>(1);
    int majority = (peers_.size() + 1) / 2 + 1;
    uint64_t saved_term = current_term_;

    kvstore::vote_request request;
    request.set_term(saved_term);
    request.set_candidate_id(self_id_);
    request.set_last_log_index(log_.size() - 1);
    request.set_last_log_term(log_.back().term());

    for (const auto& peer : peers_) {
        std::weak_ptr<RaftNode> weak_self = weak_from_this();
        std::thread([weak_self, peer, request, saved_term, votes_received, majority]() {
            auto self = weak_self.lock();
            if (!self) return;

            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
            kvstore::vote_response response;
            
            // Use .at() for thread-safe read-only access to map
            auto status = self->stubs_.at(peer)->request_vote(&context, request, &response);
            
            if (status.ok()) {
                std::lock_guard<std::mutex> lock(self->mu_);
                if (self->state_ != RaftState::CANDIDATE || self->current_term_ != saved_term) {
                    return;
                }
                
                if (response.term() > self->current_term_) {
                    self->current_term_ = response.term();
                    self->state_ = RaftState::FOLLOWER;
                    self->voted_for_ = "";
                    self->persist();
                    self->commit_cv_.notify_all(); // Wake up any waiters
                    return;
                }
                
                if (response.vote_granted()) {
                    (*votes_received)++;
                    if (*votes_received >= majority) {
                        self->state_ = RaftState::LEADER;
                        self->leader_id_ = self->self_id_;
                        for (const auto& p : self->peers_) {
                            self->next_index_[p] = self->log_.size();
                            self->match_index_[p] = 0;
                        }
                        std::cout << self->self_id_ << " became LEADER for term " << self->current_term_ << std::endl;
                        self->send_heartbeats();
                        self->commit_cv_.notify_all();
                    }
                }
            }
        }).detach();
    }
}

void RaftNode::send_heartbeats() {
    uint64_t saved_term = current_term_;
    
    for (const auto& peer : peers_) {
        kvstore::append_request request;
        request.set_term(saved_term);
        request.set_leader_id(self_id_);
        
        int64_t prev_idx = next_index_[peer] - 1;
        request.set_prev_log_index(prev_idx);
        if (prev_idx >= 0) {
            request.set_prev_log_term(log_[prev_idx].term());
        } else {
            request.set_prev_log_term(0);
        }

        for (size_t i = next_index_[peer]; i < log_.size(); ++i) {
            *request.add_entries() = log_[i];
        }
        
        request.set_leader_commit(commit_index_);

        std::weak_ptr<RaftNode> weak_self = weak_from_this();
        std::thread([weak_self, peer, request, saved_term]() {
            auto self = weak_self.lock();
            if (!self) return;

            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(50));
            kvstore::append_response response;
            
            auto status = self->stubs_.at(peer)->append_entries(&context, request, &response);
            
            if (status.ok()) {
                std::vector<kvstore::log_entry> to_apply;
                {
                    std::lock_guard<std::mutex> lock(self->mu_);
                    if (self->state_ != RaftState::LEADER || self->current_term_ != saved_term) {
                        return;
                    }
                    
                    if (response.term() > self->current_term_) {
                        self->current_term_ = response.term();
                        self->state_ = RaftState::FOLLOWER;
                        self->voted_for_ = "";
                        self->commit_cv_.notify_all();
                        return;
                    }

                    if (response.success()) {
                        self->next_index_[peer] = request.prev_log_index() + request.entries_size() + 1;
                        self->match_index_[peer] = self->next_index_[peer] - 1;

                        // Update commitIndex
                        for (int64_t n = self->log_.size() - 1; n > self->commit_index_; --n) {
                            if (self->log_[n].term() != self->current_term_) break;
                            int count = 1; // Count self
                            for (const auto& p : self->peers_) {
                                if (self->match_index_[p] >= n) count++;
                            }
                            if (count >= (int)(self->peers_.size() + 1) / 2 + 1) {
                                self->commit_index_ = n;
                                self->commit_cv_.notify_all();
                                while (self->last_applied_ < self->commit_index_) {
                                    self->last_applied_++;
                                    if (self->last_applied_ > 0) {
                                        to_apply.push_back(self->log_[self->last_applied_]);
                                    }
                                }
                                break;
                            }
                        }
                    } else {
                        self->next_index_[peer] = std::max((int64_t)1, self->next_index_[peer] - 1);
                    }
                }
                // Apply outside lock
                for (const auto& e : to_apply) self->apply_callback_(e);
            }
        }).detach();
    }
}
