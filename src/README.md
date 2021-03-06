# CS6210 Project 4: Map Reduce

### Group members:

- Raj Kripal Danday
- Jyoti Saini

## Overview:

- Here we have implemented a simplified version of map-reduce using grpc asynchronous calls for communication between master and workers.
- Workers are different processes acting as servers serving at different ports.
- The master keeps track of all the workers based on their IP Address and port and does all the book keeping of the task.
- The worker can behave as either a mapper or a reducer and receives map or reduce requests from the master with suitable inputs.
- The worker processes the input based on the user-given function and gives the final output.
- We are also not deleting the intermediate files generated so that the working can be clear while running the demo.

## Master:

- The master is the mastermind behind map and reduce
- In our implementation, the master runs in an "infinite" loop (until the map-reduce task gets over) and picks an idle worker and matches the worker to an unfinished task.
- The reduce tasks don't begin until the map tasks finish up.
- The master keeps track of all the file locations where the workers are writing data to and also keeps track of the states of each task assigned to a worker and takes differet actions based on this data.
- The master decides on the number of reduce tasks based on the required number of output files and number of keys that are to be processed.

### MR Spec:
- This is the strcuture populated by framework.
- In this helper function we read from the config file provided and populate the structure.
- We also validate the structure like valid input file paths, proper IP addresses, total worker count and no of IP addresses passed etc.
- This structure is finally passed to the master. Master uses this structure to shard files and run map-reduce tasks.

### Sharding:
- Input Files are iteratively read from the populated structure mrSpec.
- Each shard is  map<filename, <start,end>> where <start , end> are the stream::pos to read bytes from the filename. We maintain vector of such shards.
- For each file, we read the file in mrSpec.mapSize chunk. If file chunk we read has size less than the mrSpec.mapSize, we read (mrSpec.mapSize-chunk) size from next file and combine these two into one shard.
- We read all files in similar fashion and return vector of shards to framework.
- Framework passes this shard information to master which then use to assign files to map tasks.

### Handling Stragglers and failed workers:

- A failed worker will fail by giving a response. So it has nothing else to process. So based on the status of the grpc call we identify the failed worker and mark the worker back to idle.
- Keeping track of stragglers is a difficult task especially when the calls are asynchronous. What we are doing is simplifying this task by keeping the computation light. Every map or reduce task that has to be assigned to a worker has a vector of stragglers associated with it. A worker's IP is added to that vector once the request times out.
- Once we get a response from one of these stragglers in this vector, we mark all of them as idle.
- Why are we doing this? We feel it does not lead to any incorrect behavior as the requests will just be added to the straggler's queue asynchronously. Also it takes more memory to keep track of all the stragglers along with what task they are assigned to when they time out.
- We are also assuming it is a rare scenario where more than one worker straggles on a single task. Hence this design decision.

### Sorting the keys:

- The master also plays a role in keeping the keys sorted. The master keeps track of the keys that are processed by mappers and stores them in a ```std::map``` which keeps the keys sorted and assigns the reduce tasks accordingly to workers

## Workers:

- The workers just wait on a grpc completion queue in order to consume incoming requests and process them.
- There is a flag in the request that determines whether the worker should act as a mapper or reducer based on which the worker undergoes different control paths.

### Mapper:

- The mapper takes in the input lines from the file shard and passes it to the user defined mapper to emit key value pairs. These pairs are stored in a local buffer. Once all the key-value pairs are emitted in a task the mapper then writes them all down into different files with values corresponding to one key per file.

### Reducer:

- The reducer takes in the files assigned by the master to process. 
- Once the keys are extracted from these files and the values consolidated into a local data structure, they are passed to the user defined reduce function which emits a key value pair.
- These key value pairs are again stored in a local buffer and then are sorted and they output into a single file per reducer.

