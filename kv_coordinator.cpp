#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include <grpcpp/grpcpp.h>
#include "KV_store.grpc.pb.h"
#include "consistent_hash.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::Channel;
using kvstore::KeyValueStore;
using kvstore::put_request;
using kvstore::put_response;
using kvstore::get_request;
using kvstore::get_response;
using kvstore::del_request;
using kvstore::del_response;

class CoordinatorImpl final : public KeyValueStore::Service {
public:
    CoordinatorImpl(const std::vector<std::string>& shard_addresses) {
        for (const auto& addr : shard_addresses) {
            hash_ring_.add_server(addr);
            stubs_[addr] = KeyValueStore::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
            std::cout << "Coordinator connected to shard: " << addr << std::endl;
        }
    }

    Status put(ServerContext* context, const put_request* request, put_response* response) override {
        std::string target_shard = hash_ring_.get_server(request->key());
        grpc::ClientContext client_context;
        return stubs_.at(target_shard)->put(&client_context, *request, response);
    }

    Status get(ServerContext* context, const get_request* request, get_response* response) override {
        std::string target_shard = hash_ring_.get_server(request->key());
        grpc::ClientContext client_context;
        return stubs_.at(target_shard)->get(&client_context, *request, response);
    }

    Status del(ServerContext* context, const del_request* request, del_response* response) override {
        std::string target_shard = hash_ring_.get_server(request->key());
        grpc::ClientContext client_context;
        return stubs_.at(target_shard)->del(&client_context, *request, response);
    }

private:
    consistent_hash hash_ring_;
    std::unordered_map<std::string, std::unique_ptr<KeyValueStore::Stub>> stubs_;
};

void RunCoordinator(const std::vector<std::string>& shards) {
    std::string coord_address("0.0.0.0:50060");
    CoordinatorImpl service(shards);

    ServerBuilder builder;
    builder.AddListeningPort(coord_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Coordinator listening on " << coord_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    // Default shard addresses for testing. 
    // Ideally, this list should be configurable via command-line or config file.
    // Keeping the current list for Task 2 verification.
    std::vector<std::string> shards = {"localhost:50051", "localhost:50052", "localhost:50053"};
    RunCoordinator(shards);
    return 0;
}
