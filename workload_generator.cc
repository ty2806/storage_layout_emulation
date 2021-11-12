/*
 *  Created on: May 15, 2019
 *  Author: Subhadeep
 */

#include <sstream>
#include <iostream>
#include <cstdio>
#include <sys/time.h>
#include <cmath>
#include <unistd.h>
#include <assert.h>
#include <time.h>
#include <fstream>
#include <stdlib.h>

#include "args.hxx"
#include "workload_generator.h"

using namespace std;


/*
 * DEFINITIONS 
 * 
 */

long long WorkloadGenerator::KEY_DOMAIN_SIZE = 100000000;
vector < long > WorkloadGenerator::inserted_keys;

int WorkloadGenerator::generateWorkload(long long insert_count, long entry_size, int correlation) {
  
  ofstream workload_file;
  workload_file.open("workload.txt");

  //srand(time(0));
  string sortkey, deletekey;

  for (long i=0; i<insert_count; ++i) {
    if (correlation == 0)
      sortkey = generateKey();
    else
      sortkey = std::to_string(i + 1);
      
    deletekey = std::to_string(i + 1);

    // long value_size = entry_size - key.size();
    long value_size = entry_size - 2*sizeof(long);
    string value = generateValue(value_size);
    workload_file << "I " << sortkey << " " << deletekey << " " << value << std::endl;
    //workload_file << sortkey << std::endl;
  }
  
  workload_file.close();

  return 1;
}


string WorkloadGenerator::generateKey() {
  unsigned long randomness = rand() %  KEY_DOMAIN_SIZE;
  WorkloadGenerator::inserted_keys.push_back(randomness);
  string key = std::to_string(randomness);
  return key;
}

string WorkloadGenerator::generateValue(long value_size) {
  string value = std::string(value_size, 'a' + (rand() % 26));
  return value;
}

