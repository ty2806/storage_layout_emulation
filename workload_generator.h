/*
 *  Created on: May 15, 2019
 *  Author: Subhadeep
 */

#ifndef WORKLOAD_GEN_H_
#define WORKLOAD_GEN_H_


#include <iostream>

using namespace std;


class WorkloadGenerator
{
private:



public:
  static long long KEY_DOMAIN_SIZE;

  static int generateWorkload(long insert_count, long entry_size, int correlation);
  static string generateKey();
  static string generateValue(long value_size);
  


};

#endif /*WORKLOAD_GEN_H_*/

