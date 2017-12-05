#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <unistd.h>
#include <unordered_set>
#include "file_shard.h"
#include "mapreduce_spec.h"
#include "masterworker.grpc.pb.h"
#include "smart_ptrs.h"
#include "thread_pool.h"

using masterworker::MasterWorker;
using masterworker::MasterQuery;
using masterworker::WorkerReply;
using masterworker::ShardInfo;
using masterworker::TempFiles;

enum WORKER_STATUS {
  IDLE,
  INPROCESS,
  COMPLETE,
};

class Master {
public:
  Master(const MapReduceSpec&, const std::vector<FileShard>&);
  bool run();

private:
  bool callRunMapperTask();
  bool callRunReducerTask();
  bool remoteCallMap( const std::string& ip_addr_port, const FileShard& file_shard);
  bool remoteCallReduce(const std::string& ip_addr_port, const std::string& file_name);
  std::string selectIdleWorker();

  MapReduceSpec mrSpec;
  std::vector<FileShard> fileShards;

  // worker status: IDLE, INPROCESS
  std::unordered_map<std::string, WORKER_STATUS> workerStatus;

  // save temp filenames from workers
  std::unordered_set<std::string> tempFileName;

  // master built-in thread pool
  std::unique_ptr<ThreadPool> threadPool;

  std::mutex mutex;
  std::mutex mutexTask;

  // notify when all map task have been done
  int count_;
  std::condition_variable notEmpty;
};

/* CS6210_TASK: This is all the information your master will get from the
   framework.
        You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
  threadPool = make_unique<ThreadPool>(mr_spec.workerNums);
  mrSpec = mr_spec;
  fileShards = std::move(file_shards);

  for (auto& work_addr : mr_spec.workerAddrs) {
    workerStatus[work_addr] = IDLE;
  }
}

inline std::string Master::selectIdleWorker() {
  for (auto& work_addr : mrSpec.workerAddrs) {
    if (workerStatus[work_addr] == IDLE) {
      workerStatus[work_addr] = INPROCESS;
      return work_addr;
    }
  }
  return "";
}

bool Master::callRunMapperTask() {
  count_ = fileShards.size();
  for (int i = 0; i < fileShards.size(); ++i) {
    threadPool->AddTask([&, i]() {
      std::string idleWorker;
      do {
        {
          std::lock_guard<std::mutex> lock(mutex);
          idleWorker = selectIdleWorker();
        }
      } while (idleWorker.empty());
      // map function ...
      remoteCallMap(idleWorker, fileShards[i]);
      notEmpty.notify_one();
    });
  }
  return true;
}

bool Master::remoteCallMap(const std::string& ip_addr_port, const FileShard& file_shard) {
  std::unique_ptr<MasterWorker::Stub> stub_ = MasterWorker::NewStub(
      grpc::CreateChannel(ip_addr_port, grpc::InsecureChannelCredentials()));

  // 1. set grpc query parameters
  MasterQuery query;
  query.set_ismap(true);
  query.set_userid(mrSpec.userId);
  query.set_outputnum(mrSpec.outputNums);

  for (auto& shardmap : file_shard.shardsMap) {
    ShardInfo* shard_info = query.add_shard();
    shard_info->set_filename(shardmap.first);
    shard_info->set_offstart(static_cast<int>(shardmap.second.first));
    shard_info->set_offend(static_cast<int>(shardmap.second.second));
  }

  // 2. set async grpc service
  WorkerReply reply;
  grpc::ClientContext context;
  grpc::CompletionQueue cq;
  grpc::Status status;

  std::unique_ptr<grpc::ClientAsyncResponseReader<WorkerReply>> rpc(
      stub_->AsyncmapReduce(&context, query, &cq));

  rpc->Finish(&reply, &status, (void*)1);
  void* got_tag;
  bool ok = false;
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  GPR_ASSERT(got_tag == (void*)1);
  GPR_ASSERT(ok);

  if (!status.ok()) {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
    return false;
  }
 // 3. master receive intermediate file names
  std::cout << "receive temp filenames from " << ip_addr_port << std::endl;
  workerStatus[ip_addr_port] = COMPLETE;
  
  int size = reply.tempfiles_size();  
  for (int i = 0; i < size; ++i) {
    tempFileName.insert(reply.tempfiles(i).filename());
  }

  // 4. recover server to available
  workerStatus[ip_addr_port] = IDLE;

  return true;
}

bool Master::callRunReducerTask() {
  count_ = tempFileName.size();
  for (auto& temp_input : tempFileName) {
    threadPool->AddTask([&]() {
      std::string idleWorker;
      do {
        {
          std::lock_guard<std::mutex> lock(mutex);
          idleWorker = selectIdleWorker();
        }
      } while (idleWorker.empty());
      // map function ...
      remoteCallReduce(idleWorker, temp_input);
      notEmpty.notify_one();
    });
  }

  return true;
}

bool Master::remoteCallReduce(const std::string& ip_addr_port, const std::string& file_name) {
  std::unique_ptr<MasterWorker::Stub> stub_ = MasterWorker::NewStub(
      grpc::CreateChannel(ip_addr_port, grpc::InsecureChannelCredentials()));

  // 1. set grpc query parameters
  MasterQuery query;
  query.set_ismap(false);  // reduce procedure
  query.set_userid(mrSpec.userId);
  query.set_location(file_name);

  // 2. set async grpc service
  WorkerReply reply;
  grpc::ClientContext context;
  grpc::CompletionQueue cq;
  grpc::Status status;

  std::unique_ptr<grpc::ClientAsyncResponseReader<WorkerReply>> rpc(
      stub_->AsyncmapReduce(&context, query, &cq));

  rpc->Finish(&reply, &status, (void*)1);
  void* got_tag;
  bool ok = false;
  GPR_ASSERT(cq.Next(&got_tag, &ok));
  GPR_ASSERT(got_tag == (void*)1);
  GPR_ASSERT(ok);

  if (!status.ok()) {
    std::cout << status.error_code() << ": " << status.error_message()
              << std::endl;
    return false;
  }

  // 3. finish grpc
  GPR_ASSERT(reply.isdone());

  // 4. recover server to available
  workerStatus[ip_addr_port] = IDLE;

  return true;
}

/* CS6210_TASK: Here you go. once this function is called you will complete
 * whole map reduce task and return true if succeeded */
bool Master::run() {
  GPR_ASSERT(callRunMapperTask());
  // for simplicity, once all map tasks done, reduce will start to execution
  std::unique_lock<std::mutex> lock(mutexTask);
  notEmpty.wait(lock, [this] { return --count_ == 1; });
  GPR_ASSERT(callRunReducerTask());
  notEmpty.wait(lock, [this] { return --count_ == 1; });
  std::cout << "map reduce job done .........." << std::endl;

  return true;
}
