/*
 *  Created on: May 14, 2019
 *  Author: Subhadeep
 */
 
#ifndef AWESOME_H_
#define AWESOME_H_

#include "emu_environment.h"

#include <iostream>
#include <vector>

using namespace std;

namespace awesome {

  class MemoryBuffer {
    private:
    MemoryBuffer(EmuEnv* _env);
    static MemoryBuffer* buffer_instance;
    static long max_buffer_size;

    public:
    static float buffer_flush_threshold; 
    //static int buffer_split_factor;     //Doubt
    static long current_buffer_entry_count;
    static long current_buffer_size;
    static float current_buffer_saturation;
    static int buffer_flush_count;
    static vector < pair < pair <long, long>, string> > buffer;
    static int verbosity;

    static MemoryBuffer* getBufferInstance(EmuEnv* _env);
    static int getCurrentBufferStatistics();
    static int setCurrentBufferStatistics(int entry_count_change, int buffer_flush_threshold);
    static int initiateBufferFlush(int level_to_flush_in);      //??? dekhte hobe
    static int printBufferEntries();
    //long getBufferSize();


  };

  //MTIP

  class Page {
    public: 
    vector <pair < pair < long, long> , string>> kv_vector;
    static struct vector<Page> createNewPages(int page_count);
  };


  class DeleteTile {
  
  public: 
  //int tile_level;
  //string tile_id;
  vector < Page > page_vector;
  static struct vector<DeleteTile> createNewDeleteTiles(int delete_tile_size_in_pages);

  };

  class TestFile {
  
  public: 
  int file_level;
  string file_id;
  vector < DeleteTile > tile_vector;
  static struct TestFile createNewTestFile(int level_to_flush_in);

  }; 



  class SSTFile {

  public: 
  int file_level;
  string file_id;
  vector < pair < long, string > > file_instance;
  struct SSTFile* next_file_ptr;
  
  static struct SSTFile* createNewSSTFile(int level_to_flush_in);

  };

  class DiskMetaFile {
    private: 
    static int getCurrentLevelCount();
    static int getKeyLevel(long key);
    static pair <long, long> getMatchingKeyFile(long min_key, long max_key, int key_level); //??? Definition change hobe
    static int checkAndAdjustLevelSaturation(int level);

    public:
    //static SSTFile* head_level_1;

    //MTIP
    static vector < vector <TestFile>> test_files;
    static int setTestFileHead(TestFile arg, int level);
    static TestFile getTestFileHead(TestFile arg, int level);

    static int setSSTFileHead(SSTFile* arg, int level);
    static SSTFile* getSSTFileHead(int level);
    static int setMetaStatistics(int level);
    static int getMetaStatistics();
    static int initateCompaction(int compaction_mode);
    static int initiateAwesomeSortMerge();
    static int printFileEntries();

    static SSTFile* level_head[32];

    static long level_min_key[32];
    static long level_max_key[32];
    static long level_file_count[32];
    static long level_entry_count[32];
    static long level_current_size[32];

    static long level_max_size[32];
    static long global_level_file_counter[32];
    static float disk_run_flush_threshold[32];

    static int compaction_counter[32];
    static int compaction_through_sortmerge_counter[32];
    static int compaction_through_point_manipulation_counter[32];
    static int compaction_file_counter[32];


    ////static disk_run_flush_threshold;

  };


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

  

} // namespace awesome

#endif /*EMU_ENVIRONMENT_H_*/
