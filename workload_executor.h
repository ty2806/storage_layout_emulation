/*
 *  Created on: May 14, 2019
 *  Author: Subhadeep, Papon
 */
 
#ifndef AWESOME_H_
#define AWESOME_H_

#include "emu_environment.h"

#include <iostream>
#include <vector>

using namespace std;

namespace workload_exec {

  class WorkloadExecutor {
    private:
    
    public:
    static long total_insert_count;
    static long buffer_update_count;
    static long buffer_insert_count;

    static int insert(long sortkey, long deletekey, string value);
    static int pointGet(long key);
    static int search(long key, int possible_level_of_occurrence);
    static int getWorkloadStatictics(EmuEnv* _env);
  };

  class Utility {
      public:
      static int sortAndWrite(vector < pair < pair < long, long >, string > > vector_to_compact, int level_to_flush_in);
      static int compactAndFlush(vector < pair < pair < long, long >, string > > vector_to_compact, int level_to_flush_in);
      static bool sortbysortkey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b);
      static bool sortbydeletekey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b);
  };


} // namespace awesome

#endif /*EMU_ENVIRONMENT_H_*/
