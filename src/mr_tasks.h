#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/


  std::string hashKeys(const std::string& key);
  std::unordered_set<std::string> tempFiles;
  int outputNum;
  std::mutex mutex;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {

}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	//std::cout << "Dummy emit by BaseMapperInternal: " << key << ", " << val << std::endl;
//  write lines into intermediate files.
  std::lock_guard<std::mutex> lock(mutex);
  std::string file = hashKeys(key);
  std::ofstream myfile(file, std::ios::app);
  if (myfile.is_open()) {
    myfile << key << " " << val << std::endl;
    myfile.close();
  } else {
    std::cerr << "Failed to open file " << file << std::endl;
    exit(-1);
  }
  tempFiles.insert(file);
}

/* hash key to generate output file */
inline std::string BaseMapperInternal::hashKeys(const std::string& key) {
   std::hash<std::string> hashFunction;
   std::string outputFile ="output/temp" +
      std::to_string(hashFunction(const_cast<std::string&>(key)) % outputNum) +".txt";
  return outputFile;

}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}


/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::cout << "Dummy emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
