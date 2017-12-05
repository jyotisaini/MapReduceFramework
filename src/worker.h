#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"

#include "masterworker.grpc.pb.h"

#include <grpc++/grpc++.h>
#include <grpc/support/log.h>

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string ip_addr_port_;
		masterworker::MasterWorker::AsyncService asyncService_;
		std::unique_ptr<grpc::ServerCompletionQueue> cq_;
		std::unique_ptr<grpc::Server> server_;


		

		void handleRPCs();

		class CallData {
			public:

				CallData(masterworker::MasterWorker::AsyncService * service, grpc::ServerCompletionQueue* cq)
				: service_(service),
				cq_(cq),
				responder_(&ctx_),
				status_ (CREATE) {
					proceed();

				}

				void proceed() {
					if (status_ == CREATE) {
						status_ = PROCESS;

						service_ -> RequestmapReduce(&ctx_, &request_, &responder_, cq_, cq_, this);
					} else if (status_ == PROCESS) {
						//TODO: do the processing

						status_ = FINISH;
						responder_.Finish(reply_, grpc::Status::OK, this);
					} else {
						GPR_ASSERT(status_ == FINISH);
						delete this;
					}
				}

			private: 
				masterworker::MasterWorker::AsyncService* service_;

				grpc::ServerCompletionQueue * cq_;
				grpc::ServerContext ctx_;
				
				masterworker::MasterQuery request_;
				masterworker::WorkerReply reply_;

				grpc::ServerAsyncResponseWriter<masterworker::WorkerReply> responder_;

				enum CallStatus{CREATE, PROCESS, FINISH};

				CallStatus status_;
		};

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port)
:ip_addr_port_(ip_addr_port),
asyncService_ (),
cq_ (nullptr),
server_ (nullptr) {
	
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */ 
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));

	grpc::ServerBuilder builder;

	builder.AddListeningPort(ip_addr_port_, grpc::InsecureServerCredentials());
	builder.RegisterService(&asyncService_);

	cq_ = builder.AddCompletionQueue();
	server_ = builder.BuildAndStart();

	std::cout << "Worker listening on " << ip_addr_port_ << std::endl;

	handleRPCs();

	return true;
}


void Worker::handleRPCs() {
	new CallData(&asyncService_, cq_.get());

	void * tag;
	bool ok;

	while (true) {
		GPR_ASSERT(cq_ -> Next(&tag, &ok));

		GPR_ASSERT(ok);
		static_cast<CallData *> (tag) -> proceed();
	}
}