#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "KV_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using kvstore::KeyValueStore;
using kvstore::put_request;
using kvstore::put_response;
using kvstore::get_request;
using kvstore::get_response;
using kvstore::del_request;
using kvstore::del_response;

class KVClient {
public:
    KVClient(std::shared_ptr<Channel> channel)
        : stub_(KeyValueStore::NewStub(channel)) {}

    void put(const std::string& key, const std::string& value) {
        put_request request;
        request.set_key(key);
        request.set_value(value);
        put_response response;
        ClientContext context;

        Status status = stub_->put(&context, request, &response);
        if (status.ok()) {
            if (response.success()) {
                std::cout << "PUT success: OK" << std::endl;
            } else {
                std::cout << "PUT failed. Leader hint: " << response.leader_hint() << std::endl;
            }
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
        }
    }

    void get(const std::string& key) {
        get_request request;
        request.set_key(key);
        get_response response;
        ClientContext context;

        Status status = stub_->get(&context, request, &response);
        if (status.ok()) {
            if (response.found()) {
                std::cout << "GET value: " << response.value() << std::endl;
            } else if (!response.leader_hint().empty()) {
                std::cout << "GET failed. Leader hint: " << response.leader_hint() << std::endl;
            } else {
                std::cout << "GET: Key not found" << std::endl;
            }
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
        }
    }

    void del(const std::string& key) {
        del_request request;
        request.set_key(key);
        del_response response;
        ClientContext context;

        Status status = stub_->del(&context, request, &response);
        if (status.ok()) {
            if (response.success()) {
                std::cout << "DEL success: OK" << std::endl;
            } else {
                std::cout << "DEL failed. Leader hint: " << response.leader_hint() << std::endl;
            }
        } else {
            std::cout << "RPC failed: " << status.error_message() << std::endl;
        }
    }

private:
    std::unique_ptr<KeyValueStore::Stub> stub_;
};

int main(int argc, char** argv) {
    std::string server_addr = "localhost:50051";
    int offset = 0;

    if (argc >= 2 && std::string(argv[1]).find(":") != std::string::npos) {
        server_addr = argv[1];
        offset = 1;
    }

    if (argc - offset < 3) {
        std::cerr << "Usage: ./kv_client [server_addr] <cmd: put|get|del> <key> [value]" << std::endl;
        return 1;
    }

    std::string cmd = argv[1 + offset];
    std::string key = argv[2 + offset];
    KVClient client(grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()));

    if (cmd == "put" && argc - offset == 4) {
        client.put(key, argv[3 + offset]);
    } else if (cmd == "get") {
        client.get(key);
    } else if (cmd == "del") {
        client.del(key);
    } else {
        std::cerr << "Invalid command or missing arguments" << std::endl;
        return 1;
    }

    return 0;
}
