#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <sstream>

#include <grpcpp/grpcpp.h>
#include "KV_store.h"
#include "KV_store.grpc.pb.h"
#include "raft_node.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using kvstore::KeyValueStore;
using kvstore::put_request;
using kvstore::put_response;
using kvstore::get_request;
using kvstore::get_response;
using kvstore::del_request;
using kvstore::del_response;
using kvstore::vote_request;
using kvstore::vote_response;
using kvstore::append_request;
using kvstore::append_response;

class KeyValueStoreImpl final : public KeyValueStore::Service { // Phase 2: Implementation of the gRPC service contract
public:
    explicit KeyValueStoreImpl(const std::string& self_id, const std::vector<std::string>& peers) 
        : store_(std::make_shared<kv_store>()) {
        
        raft_node_ = std::make_shared<RaftNode>(self_id, peers, [this](const kvstore::log_entry& entry) {
            if (entry.op_type() == kvstore::log_entry::PUT) {
                store_->put(entry.key(), entry.value());
            } else if (entry.op_type() == kvstore::log_entry::DELETE) {
                store_->del(entry.key());
            }
        });
    }

    Status put(ServerContext* context, const put_request* request, put_response* response) override {
        if (!raft_node_->is_leader()) {
            response->set_success(false);
            response->set_leader_hint(raft_node_->get_leader_id());
            return Status::OK;
        }

        kvstore::log_entry entry;
        entry.set_key(request->key());
        entry.set_value(request->value());
        entry.set_op_type(kvstore::log_entry::PUT);

        if (raft_node_->propose(entry)) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_leader_hint(raft_node_->get_leader_id());
        }
        return Status::OK;
    }

    Status get(ServerContext* context, const get_request* request, get_response* response) override {
        // For linearizability, we could send heartbeats, but for now we just check if leader
        if (!raft_node_->is_leader()) {
            response->set_found(false);
            response->set_leader_hint(raft_node_->get_leader_id());
            return Status::OK;
        }

        auto val = store_->get(request->key());
        if (val.has_value()) {
            response->set_value(val.value());
            response->set_found(true);
        } else {
            response->set_found(false);
        }
        return Status::OK;
    }

    Status del(ServerContext* context, const del_request* request, del_response* response) override {
        if (!raft_node_->is_leader()) {
            response->set_success(false);
            response->set_leader_hint(raft_node_->get_leader_id());
            return Status::OK;
        }

        kvstore::log_entry entry;
        entry.set_key(request->key());
        entry.set_op_type(kvstore::log_entry::DELETE);

        if (raft_node_->propose(entry)) {
            response->set_success(true);
        } else {
            response->set_success(false);
            response->set_leader_hint(raft_node_->get_leader_id());
        }
        return Status::OK;
    }

    Status request_vote(ServerContext* context, const vote_request* request, vote_response* response) override {
        *response = raft_node_->handle_request_vote(*request);
        return Status::OK;
    }

    Status append_entries(ServerContext* context, const append_request* request, append_response* response) override {
        *response = raft_node_->handle_append_entries(*request);
        return Status::OK;
    }

private:
    std::shared_ptr<kv_store> store_;
    std::shared_ptr<RaftNode> raft_node_; // Phase 5: High availability via Raft consensus engine
};

void RunServer(const std::string& self_id, const std::vector<std::string>& peers) {
    // self_id is "host:port"
    ServerBuilder builder;
    builder.AddListeningPort(self_id, grpc::InsecureServerCredentials());
    
    KeyValueStoreImpl service(self_id, peers);
    builder.RegisterService(&service);
    
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << self_id << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    if (argc < 2) { // Phase 4: Support for multi-node configuration (ID and peer list)
        std::cerr << "Usage: " << argv[0] << " self_id [peer_id1 peer_id2 ...]" << std::endl;
        return 1;
    }

    std::string self_id = argv[1];
    std::vector<std::string> peers;
    for (int i = 2; i < argc; ++i) {
        peers.push_back(argv[i]);
    }

    RunServer(self_id, peers);
    return 0;
}
