#pragma once

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <dirent.h>
#include <map>
#include <vector>

#include "masterworker.grpc.pb.h"

#include "mapreduce_spec.h"
#include "file_shard.h"


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		MapReduceSpec mr_spec_;
		std::vector<FileShard> fileShards_;
		std::vector<std::unique_ptr<masterworker::MasterWorker::Stub> > stub_;

		enum workerStatus {
			IDLE,
			COMPLETE,
			INPROCESS,
		};

		enum shardProgress {
			PENDING,
			PROCESSING,
			COMPLETED,
		};

		typedef struct WorkerState {
			std::string workerID;
			workerStatus status;
		} WorkerState;

		typedef struct ShardStatus {
			FileShard shard;
			std::string workerID;
			masterworker::MasterQuery query;
			shardProgress progress;
			
		} ShardStatus;

		typedef struct ReducerTask {
			masterworker::MasterQuery query;
			std::string workerID;
			shardProgress progress;
		}ReducerTask;

		typedef struct ReplyBookKeep {
			masterworker::WorkerReply reply;
			ShardStatus* shard;
			ReducerTask * task;
		}ReplyBookKeep;

		std::vector<ShardStatus> shardBookKeep_;
		std::vector<WorkerState> workerBookKeep_;

		std::vector<ReducerTask> reducerTasks_;

		int getIdleWorker();
		int getFirstPendingShard();

		std::map <std::string, std::vector <std::string> > mapperFiles_;

		bool allShardsProcessed();
		std::string extractKeyFromDirectory(std::string);

		int getWorkerIndexOfWorker(std::string);

		int getFisrtPendingTask();

		bool allTasksProcessed();
		bool reduceTasksCreated;

		bool hasAllIdleWorkers();

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
: mr_spec_(mr_spec),
fileShards_ (file_shards) {
	std::cout << "master constructor called" << std::endl;
	reduceTasksCreated = false;
	for (int i = 0; i < fileShards_.size(); i++) {
		ShardStatus status;
		status.shard = fileShards_[i];
		status.workerID = "";
		
		masterworker::MasterQuery query;

		for (auto &shardMap : status.shard.shardsMap) {
			masterworker::ShardInfo* info = query.add_shard();

			info -> set_offstart(static_cast<int> (shardMap.second.first));
			info -> set_offend(static_cast<int> (shardMap.second.second));
			info -> set_filename(shardMap.first);
		}

		query.set_userid(mr_spec_.userId);
		query.set_ismap(true);
		query.set_workerid(std::to_string(i));

		status.query = query;

		status.progress = PENDING;

		shardBookKeep_.push_back(status);
	}

	for (int i = 0; i < mr_spec_.workerAddrs.size(); i++) {
		WorkerState state;
		state.workerID = mr_spec_.workerAddrs[i];
		state.status = IDLE;

		workerBookKeep_.push_back(state);
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {

	system ("mkdir ../test/output/mapper/");
	system ("mkdir ../test/output/reducer/");

	bool mapDone =  false;
	bool reduceDone = false;

	grpc::CompletionQueue cq;

	while(!reduceDone) {
		std::cout << "outer while loop" << std::endl;
		std::vector<ReplyBookKeep> replyVector (workerBookKeep_.size());

		std::vector<bool> statusList (workerBookKeep_.size());

		grpc::Status rpcStatus[workerBookKeep_.size()];

		

		for (int i = 0; i < workerBookKeep_.size(); i++) {
			std::cout << "creating stubs" << std::endl;
			std::shared_ptr<grpc::Channel> channel(grpc::CreateChannel(workerBookKeep_[i].workerID, 
				grpc::InsecureChannelCredentials()));
			std::unique_ptr<masterworker::MasterWorker::Stub> 
				stub(masterworker::MasterWorker::NewStub(channel));

			stub_.push_back(std::move(stub)); //use same stub_ for both map and reduce
		}
			
		while (1) {

			std::cout<< "inner while loop" << std::endl;
			int workerIndex = getIdleWorker();

			if(workerIndex == -1)
				break;
			int shardIndex = getFirstPendingShard();
			if(shardIndex==-1){
				std::cout << "shards processed? " << (allShardsProcessed()?"YES": "NO") << std::endl;
				if (allShardsProcessed()) {
					mapDone=true;
            		
				}
				break;
            	
			}

			grpc::ClientContext clientContext;

			ShardStatus* currentProcessingShard = &shardBookKeep_[shardIndex];
			WorkerState* currentWorker = &workerBookKeep_[workerIndex];

			std::cout << "current shard status is processing before: " << ((currentProcessingShard -> progress == PROCESSING) ? "yes" : "no") << std::endl;

			currentProcessingShard -> workerID = currentWorker -> workerID;
			currentProcessingShard -> progress = PROCESSING;
			currentWorker -> status = INPROCESS;


			replyVector[workerIndex].shard = &shardBookKeep_[shardIndex];

			std::cout << "currentProcessingShard :: " << shardIndex << std::endl;
			std::cout << "current shard status is processing: " << ((currentProcessingShard -> progress == PROCESSING) ? "yes" : "no") << std::endl;

            std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::WorkerReply> > rpc (stub_[workerIndex] -> AsyncmapReduce(&clientContext, currentProcessingShard -> query, &cq));

			rpc -> Finish(&(replyVector[workerIndex].reply), &rpcStatus[workerIndex], (void*) &replyVector[workerIndex]);


		}


		// if (getIdleWorker() != -1 && !mapDone)
		// 	continue;
		if (!mapDone && getIdleWorker() != -1 && getFirstPendingShard() != -1 )
			continue;

		for (int i = 0; i < workerBookKeep_.size() && !mapDone && !hasAllIdleWorkers(); i++) {
			void *tag;
			bool status;
			std::cout << "waiting for reply" << std::endl;
			GPR_ASSERT(cq.Next(&tag, &status));
			statusList[i] =status;
			GPR_ASSERT(statusList[i]);
		    ReplyBookKeep* replyVector = (ReplyBookKeep*) tag;

			if(status) {
				std::cout << "Received Reply from Worker " << std::endl;
				replyVector -> shard -> progress=COMPLETED;

				std::string workerIP = replyVector -> shard -> workerID;
				int workerIndex = getWorkerIndexOfWorker(workerIP);
				workerBookKeep_[workerIndex].status = COMPLETE;

				std::string dirPath = replyVector -> reply.directory();
				std::cout << "reply received: " << dirPath << std::endl;
				DIR *dir;
				struct dirent *ent;
				if((dir = opendir(dirPath.c_str()))!= NULL){
					while((ent=readdir(dir))!=NULL) {
						std::string file (ent -> d_name);
						if (file.compare (".") == 0 || file.compare("..") == 0)
							continue;
						
						std::string key = file.substr(0, file.length() - 4);

						// std::cout << "key processed: " << key << std::endl;
						mapperFiles_[key].push_back(dirPath + file);


						// std::cout << "file name in reply: " << mapperFiles_[key][0] << std::endl;
						//this will be sorted by default by key
					}
				}

				workerBookKeep_[workerIndex].status = IDLE;	

			} else  {
				//set worker IDLE
			}
			
		}
		
		//reduce stuff

		if (mapDone && !reduceDone) {
			int numOutputFiles = mr_spec_.outputNums;
			int numKeysPerReducer;
			int numKeys = mapperFiles_.size();
			
			if (mapperFiles_.size() < numOutputFiles)
				numKeysPerReducer = numKeys;

			else
				numKeysPerReducer = numKeys/numOutputFiles + 1;

			int numReducerTasks = numOutputFiles;
			int currentTask = 0;
			int currKeys = 0;
			int keysProcessed = 0;
			int keysProcessedInCurrentTask = 0;


			for (int i = 0; i < numReducerTasks; i++) {
				ReducerTask task;
				task.query.set_userid(mr_spec_.userId);
				task.query.set_ismap(false);
				task.query.set_workerid(std::to_string(i));

				task.progress = PENDING;
				if(reducerTasks_.size() < numReducerTasks)
					reducerTasks_.push_back(task);
				else {
					reduceTasksCreated = true;
				}
			}

			std::cout << "populating queries" << std::endl;
			std::cout << "numKeys: " << numKeys << std::endl;
			for (std::map<std::string, std::vector<std::string> >::iterator iter = mapperFiles_.begin(); (iter != mapperFiles_.end()) && (!reduceTasksCreated); ++iter) {
				// std::cout << "processing keys" << std::endl;
				std::vector<std::string> filesVector = iter -> second;
				if (keysProcessedInCurrentTask < numKeysPerReducer || currentTask == numReducerTasks - 1) {
					// for(std::vector<std::string>::iterator it = filesVector.begin(); it != filesVector.end(); ++it) {
					// 	masterworker::TempFiles* keyFile = reducerTasks_[currentTask].query.add_keyfiles();
					// 	std::cout << iter -> first << ": " << *it << std::endl;
					// 	keyFile -> set_filename(*it);
					// }

					for (int i = 0; i < filesVector.size(); i++) {
						masterworker::TempFiles* keyFile = reducerTasks_[currentTask].query.add_keyfiles();
						keyFile -> set_filename(filesVector[i]);
						// std::cout << iter -> first << ": " << filesVector[i] << std::endl;
					}
					
				}

				keysProcessed++;
				keysProcessedInCurrentTask++;

				if (keysProcessedInCurrentTask == numKeysPerReducer && currentTask != numReducerTasks - 1) {
					keysProcessedInCurrentTask = 0;
					currentTask++;
				}
			}

			while (!reduceDone) {
				int workerIndex = getIdleWorker();

				if(workerIndex == -1)
					break;

				int taskIndex = getFisrtPendingTask();

				if (taskIndex == -1) {
					std::cout << "tasks processed? " << (allTasksProcessed()?"YES": "NO") << std::endl;
					if(allTasksProcessed())
						reduceDone = true;

					break;
				}

				WorkerState * currentWorker = &workerBookKeep_[workerIndex];
				ReducerTask * currentReducerTask = &reducerTasks_[taskIndex];

				grpc::ClientContext clientContext;

				currentReducerTask -> workerID = currentWorker -> workerID;
				currentReducerTask -> progress = PROCESSING;
				currentWorker -> status = INPROCESS;

				replyVector[workerIndex].task = &reducerTasks_[taskIndex];

				std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::WorkerReply> > rpc (stub_[workerIndex] -> AsyncmapReduce(&clientContext, currentReducerTask -> query, &cq));

				rpc -> Finish(&(replyVector[workerIndex].reply), &rpcStatus[workerIndex], (void*) &replyVector[workerIndex]);

			}

			if (reduceDone)
				break;

			if (getIdleWorker() != -1 && getFisrtPendingTask() != -1 && !reduceDone)
				continue;

			for (int i = 0; i < workerBookKeep_.size() && !hasAllIdleWorkers(); i++) {
				
				void *tag;
				bool status;
				std::cout << "waiting for reply" << std::endl;
				GPR_ASSERT(cq.Next(&tag, &status));
				statusList[i] =status;
				GPR_ASSERT(statusList[i]);
			    ReplyBookKeep* replyVector = (ReplyBookKeep*) tag;

				if(status) {
					std::cout << "Received Reply from Worker " << std::endl;
					replyVector -> task -> progress=COMPLETED;

					std::string workerIP = replyVector -> task -> workerID;
					int workerIndex = getWorkerIndexOfWorker(workerIP);
					workerBookKeep_[workerIndex].status = COMPLETE;

					std::string dirPath = replyVector -> reply.directory();
					std::cout << "reply received: " << dirPath << std::endl;
					

					workerBookKeep_[workerIndex].status = IDLE;
				} else {

				}	
			}

			if(allTasksProcessed())
				break;
		} else if (reduceDone) {
			break;
		}

			

	}
	return true;
}

int Master::getIdleWorker() {
	for(int i = 0; i < workerBookKeep_.size(); i++) {
		if(workerBookKeep_[i].status == IDLE) {
			return i;
		}
	}

	return -1;
}

int Master::getFirstPendingShard() {
	for(int i =0 ; i<shardBookKeep_.size(); i++) {
		if(shardBookKeep_[i].progress==PENDING)
			return i;
	}

	return -1;

}

bool Master::allShardsProcessed() {
	for(int i = 0; i < shardBookKeep_.size(); i++) {
		if (shardBookKeep_[i].progress != COMPLETED) {
			return false;
		}
	}

	return true;
}

std::string Master::extractKeyFromDirectory(std::string directory) {
	int lastIndex = directory.length() - 1;
	while(directory.at(lastIndex) != '/') {
		lastIndex--;
	}
	lastIndex++;

	std::string fileName = directory.substr(lastIndex);

	fileName = fileName.substr(0, fileName.length() - 4);

	return fileName;
}

int Master::getWorkerIndexOfWorker(std::string worker) {
	for (int i = 0; i < workerBookKeep_.size(); i++) {
		if (workerBookKeep_[i].workerID.compare(worker) == 0) {
			return i;
		}
	}

	return -1;
}

int Master::getFisrtPendingTask() {
	for (int i = 0; i < reducerTasks_.size(); i++) {
		if(reducerTasks_[i].progress == PENDING) {
			return i;
		}
	}

	return -1;
}

bool Master::allTasksProcessed() {
	for (int i = 0; i < reducerTasks_.size(); i++) {
		if(reducerTasks_[i].progress != COMPLETED) {
			return false;
		}
	}

	return true;
}

bool Master::hasAllIdleWorkers() {
	for(int i = 0; i < workerBookKeep_.size(); i++) {
		if (workerBookKeep_[i].status == INPROCESS) {
			return false;
		}
	}

	return true;
}
