/*
 *  Created on: May 14, 2019
 *  Author: Subhadeep, Papon
 */

#include <iostream>
#include <cmath>
#include <sys/time.h>
#include <vector>
#include <cstdlib>
#include <algorithm>
#include <iomanip>

#include "emu_environment.h"
#include "tree_builder.h"
#include "workload_executor.h"

using namespace std;
using namespace tree_builder;
using namespace workload_exec;

long WorkloadExecutor::buffer_insert_count = 0;
long WorkloadExecutor::buffer_update_count = 0;
long WorkloadExecutor::total_insert_count = 0;
uint32_t WorkloadExecutor::counter = 0;

inline void showProgress(const uint32_t &workload_size, const uint32_t &counter) {
  
    // std::cout << "counter = " << counter << std::endl;
    if (counter / (workload_size/100) >= 1) {
      for (int i = 0; i<104; i++){
        std::cout << "\b";
        fflush(stdout);
      }
    }
    for (int i = 0; i<counter / (workload_size/100); i++){
      std::cout << "=" ;
      fflush(stdout);
    }
    std::cout << std::setfill(' ') << std::setw(101 - counter / (workload_size/100));
    std::cout << counter*100/workload_size << "%";
      fflush(stdout);

  if (counter == workload_size) {
    std::cout << "\n";
    return;
  }
}

//This is used to sort the whole file based on sort key
bool Utility::sortbysortkey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b)
{
  return (a.first.first < b.first.first);
}
//This is used to sort the whole file based on delete key
bool Utility::sortbydeletekey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b)
{
  return (a.first.second < b.first.second);
}

int Utility::minInt(int a, int b)
{
  if (a <= b)
    return a;
  else
  {
    return b;
  }
}

// this function implement compaction
// return the number of SST files written to the level
int Utility::compactAndFlush(vector < pair < pair < long, long >, string > > vector_to_compact, int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();
  
  int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;
  int file_count = ceil(vector_to_compact.size() / (entries_per_file*1.0));
  if (MemoryBuffer::verbosity == 2)
    std::cout << "\nwriting " << file_count << " file(s)\n";

  SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
  SSTFile *moving_head = head_level_1;
  SSTFile *moving_head_prev = head_level_1;   //Need this to overwrite current overlapping file
  SSTFile *end_ptr = moving_head;

  // if the level is empty, add new SST files to the level
  if (!head_level_1)
  {
    for (int i = 0; i < file_count; i++)
    {
      vector<pair<pair<long, long>, string>> vector_to_populate_file;
      entries_per_file = Utility::minInt(entries_per_file, vector_to_compact.size());
      for (int j = 0; j < entries_per_file; ++j)
      {
        vector_to_populate_file.push_back(vector_to_compact[j]);
      }
      vector_to_compact.erase(vector_to_compact.begin(), vector_to_compact.begin() + entries_per_file);
      SSTFile *new_file = SSTFile::createNewSSTFile(level_to_flush_in);
      SSTFile *moving_head = head_level_1;
      if (i == 0)
      {
        head_level_1 = new_file;
        DiskMetaFile::setSSTFileHead(head_level_1, level_to_flush_in);
        int status = SSTFile::PopulateFile(new_file, vector_to_populate_file, level_to_flush_in);
      }
      else
      {
        while (moving_head->next_file_ptr)
        {
          moving_head = moving_head->next_file_ptr;
        }
        int status = SSTFile::PopulateFile(new_file, vector_to_populate_file, level_to_flush_in);
        moving_head->next_file_ptr = new_file;
      }
      vector_to_populate_file.clear();
    }
  }
  // if the level is not empty, insert SST files
  else
  {
    //Calculating end pointer
    while (moving_head)
    {
      if (moving_head->max_sort_key <= vector_to_compact[0].first.first)
      {
        end_ptr = NULL;
        moving_head = moving_head->next_file_ptr;
        continue;
      }
      end_ptr = moving_head;  // Need to assign this in case vector gets inserted in between with no overlapping
      if (moving_head->min_sort_key >= vector_to_compact[vector_to_compact.size() - 1].first.first )
      {
        //safe to say we have calculated end pointer
        break;
      }
      end_ptr = moving_head->next_file_ptr;
      moving_head = moving_head->next_file_ptr;
    }

    if (MemoryBuffer::verbosity == 2)
      cout << "Calculated end pointer" << endl;
    
    moving_head = head_level_1;
    int flag = 0; //1 means vector to be appended at the begining or vector to be inserted in between two files (NO OVERLAPPING); 2 means not possible to be head; 0 means overlapping
    int hasHeadChanged = 0;  //head?

    while (moving_head)
    {
      if (moving_head->max_sort_key <= vector_to_compact[0].first.first)
      {
        moving_head_prev = moving_head; 
        flag = 2; //Not possible to be a head anymore
        if (moving_head->next_file_ptr)
        {
          moving_head = moving_head->next_file_ptr;
          continue;
        }
      }
      //cout << "Prev max: " << moving_head_prev->max_sort_key << endl;
      //cout << "Current max: " << moving_head->max_sort_key << endl;
      // if (end_ptr != NULL)
      //   cout << "End max: " << end_ptr->max_sort_key << endl;
      if (moving_head->min_sort_key >= vector_to_compact[vector_to_compact.size()-1].first.first)
      {
        flag = 1; //vector to be appended very begining or vector to be inserted in between with no overlapping
      }
      //std::cout << "\nFile Count: " << file_count << std::endl; 
      for (int i = 0; i < file_count; i++)
      {
        vector<pair<pair<long, long>, string>> vector_to_populate_file;
        entries_per_file = Utility::minInt(entries_per_file, vector_to_compact.size());
        for (int j = 0; j < entries_per_file; ++j)
        {
          vector_to_populate_file.push_back(vector_to_compact[j]);
        }

        if (MemoryBuffer::verbosity == 2)
        {
          std::cout << "\nprinting before trimming " << std::endl;
          for (int j = 0; j < vector_to_compact.size(); ++j)
            std::cout << "< " << vector_to_compact[j].first.first << ",  " << vector_to_compact[j].first.second << " >"
                      << "\t" << std::endl;
        }
        vector_to_compact.erase(vector_to_compact.begin(), vector_to_compact.begin() + entries_per_file);

        if (MemoryBuffer::verbosity == 2)
        {
          std::cout << "\nprinting after trimming " << std::endl;
          for (int j = 0; j < vector_to_compact.size(); ++j)
            std::cout << "< " << vector_to_compact[j].first.first << ",  " << vector_to_compact[j].first.second << " >"
                      << "\t";
        }
        if (MemoryBuffer::verbosity == 2)
        {
          std::cout << "\npopulating file " << head_level_1 << std::endl;
          std::cout << "Vector to populate file: " << endl;
          for (int j = 0; j < vector_to_populate_file.size(); ++j)
            std::cout << "< " << vector_to_populate_file[j].first.first << ",  " << vector_to_populate_file[j].first.second << " >"
                      << "\t";
        }                          

        SSTFile *new_file = SSTFile::createNewSSTFile(level_to_flush_in);
        int status = SSTFile::PopulateFile(new_file, vector_to_populate_file, level_to_flush_in);
        new_file->next_file_ptr = end_ptr;
        //SSTFile *moving_head = head_level_1;

        if (moving_head == DiskMetaFile::getSSTFileHead(level_to_flush_in) && i==0 && flag!=2)    //Further Optimize?
        {
          DiskMetaFile::setSSTFileHead(new_file, level_to_flush_in);
          hasHeadChanged = 1;
        }

        if (flag == 1 && hasHeadChanged == 0)   //This means file to be inserted in between with no overlapping
        {
          moving_head_prev->next_file_ptr = new_file;
        }

        if (flag != 1)
        {
          moving_head_prev->next_file_ptr = new_file;
          moving_head_prev = new_file;
        }
        
        vector_to_populate_file.clear();
      }
      break;
    }
  }
  return file_count;
}

// sort merge and compact the vector_to_compact to a given level
// return the number of SST files written to the level
// this function will invoke checkAndAdjustLevelSaturation and check if a level is saturated and conduct comaction accordingly
int Utility::sortAndWrite(vector < pair < pair < long, long >, string > > vector_to_compact, int level_to_flush_in)
{
    // Initialize variables
    EmuEnv *_env = EmuEnv::getInstance();
    SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
    int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;
    int write_file_count = 0;

    // Sort the key-value pairs by sort key
    std::sort(vector_to_compact.begin(), vector_to_compact.end(), Utility::sortbysortkey);

    // If there is no SST file at this level, and the number of key-value pairs in the vector is not a multiple of the number of key-value pairs per delete tile, or there are less key-value pairs in the vector than the number of key-value pairs per delete tile, exit with an error message. Otherwise, perform compaction and flush on the key-value pairs in the vector.
    if (!head_level_1)
    {
        if (MemoryBuffer::verbosity == 2)
            std::cout << "NULL" << std::endl;

        if (vector_to_compact.size() % _env->getDeleteTileSize(level_to_flush_in) != 0 && vector_to_compact.size() / _env->getDeleteTileSize(level_to_flush_in) < 1)
        {
            std::cout << " ERROR " << std::endl;
            exit(1);
        }
        else
        {
            write_file_count += compactAndFlush(vector_to_compact, level_to_flush_in);
        }
    }

    // If there is at least one SST file at this level, perform sort-merge on the key-value pairs in the vector and the key-value pairs in the SST files at this level, and then perform compaction and flush on the merged key-value pairs.
    else
    {
        if (MemoryBuffer::verbosity == 2)
            std::cout << "head not null anymore" << std::endl;

        sortMerge(vector_to_compact, level_to_flush_in);

        if (MemoryBuffer::verbosity == 1)   //UNCOMMENT
        {
            std::cout << "\nprinting before compacting " << std::endl;
            for (int j = 0; j < vector_to_compact.size(); ++j)
            {
                std::cout << "< " << vector_to_compact[j].first.first << ",  " << vector_to_compact[j].first.second << " >"
                          << "\t" << std::endl;
                if (j%8 == 7) cout << std::endl;
            }
        }

        write_file_count += compactAndFlush(vector_to_compact, level_to_flush_in);
    }

    // Check and adjust the level saturation, and return the number of files written
    write_file_count += DiskMetaFile::checkAndAdjustLevelSaturation(level_to_flush_in);
    return write_file_count;
}


// conduct query driven compaction
// vector_to_compact: the range query result
int Utility::QueryDrivenCompaction(vector < pair < pair < long, long >, string > > vector_to_compact)
{
    // Initialize variables
    int write_file_count = 0;
    int last_level = DiskMetaFile::getTotalLevelCount();

    // Check if there are at least two levels in the database
    if (last_level < 2) {
        std::cout << "There is only 1 level in LSM Tree. No Query Driven Compaction will perform." << std::endl;
        return write_file_count;
    }

    // Loop through all levels except the first and last
    for (int i = 2; i < last_level; i++) {
        // Perform sort-merge-repartition on the key-value pairs in the vector at this level
        write_file_count += sortMergeRepartition(vector_to_compact, i);
    }

    // Perform sort and write on the key-value pairs in the vector at the last level
    write_file_count += sortAndWrite(vector_to_compact, last_level);

    // Return the number of resulting files
    return write_file_count;
}



// merge and sort vector_to_compact at level_to_flush_in
// This will merge all kv in SST files overlapping with vector_to_compact
void Utility::sortMerge(vector < pair < pair < long, long >, string > >& vector_to_compact, int level_to_sort)
{
    // Initialize variables
    SSTFile* moving_head = DiskMetaFile::getSSTFileHead(level_to_sort);
    long startval =  vector_to_compact[0].first.first;
    long endval =  vector_to_compact[vector_to_compact.size()-1].first.first;
    int match = 0;

    // Print the size of the vector before merging (for verbosity level 2)
    if (MemoryBuffer::verbosity == 2)
        std::cout << "Vector size before merging : " << vector_to_compact.size() << std::endl;

    // Loop through all SST files at the given level and merge overlapping key-value pairs into the vector
    while (moving_head)
    {
        // Check if this SST file is completely before or after the range of interest
        if (moving_head->min_sort_key > endval || moving_head->max_sort_key < startval )
        {
            moving_head = moving_head->next_file_ptr;
            continue;
        }

        // Loop through all tiles in this SST file
        for (const auto& delete_tile : moving_head->tile_vector) {
            // Loop through all pages in this tile
            for (const auto& page : delete_tile.page_vector) {
                // Loop through all key-value pairs in this page
                for (auto & m : page.kv_vector) {
                    // Check if this key has already been added to the vector
                    for(auto & p : vector_to_compact) {
                        if (p.first.first == m.first.first) {
                            match++;
                        }
                    }
                    // If the key has not been added to the vector, add it
                    if (match == 0)
                        vector_to_compact.push_back(m);
                    else
                        match = 0;
                }
            }
        }
        // Move to the next SST file at this level
        moving_head = moving_head->next_file_ptr;
    }

    // Print the size of the vector after merging (for verbosity level 2)
    if (MemoryBuffer::verbosity == 2)
        std::cout << "Vector size after merging : " << vector_to_compact.size() << std::endl;

    // Sort the vector by sort key
    std::sort(vector_to_compact.begin(), vector_to_compact.end(), Utility::sortbysortkey);
}


// merge and sort vector_to_compact at level_to_flush_in
// All kv inside range of vector_to_compact will be merged
// kv in SST files overlapping with vector_to_compact but outside range of vector_to_compact will be written back
// return the number of SST files written to the LSM tree during this process
int Utility::sortMergeRepartition(vector < pair < pair < long, long >, string > >& vector_to_compact, int level_to_flush_in)
{
    // Initialize variables
    SSTFile* level_head = DiskMetaFile::getSSTFileHead(level_to_flush_in);
    SSTFile* moving_head = level_head;
    long startval =  vector_to_compact[0].first.first;
    long endval =  vector_to_compact[vector_to_compact.size()-1].first.first;
    vector < pair < pair < long, long >, string > > vector_to_populate;
    vector < pair < pair < long, long >, string > > vector_overlapping;
    SSTFile* prev_head = nullptr;

    // Print the size of the vector before merging (for verbosity level 2)
    if (MemoryBuffer::verbosity == 2)
        std::cout << "Vector size before merging : " << vector_to_compact.size() << std::endl;

    // Loop through all SST files at the given level and merge overlapping key-value pairs into a separate vector
    while (moving_head)
    {
        // Check if this SST file is completely after the range of interest
        if (moving_head->min_sort_key > endval) {
            if (prev_head != nullptr) {
                prev_head->next_file_ptr = moving_head;
            }
            else {
                DiskMetaFile::setSSTFileHead(moving_head, level_to_flush_in);
            }
            break;
        }

        // Check if this SST file is completely before the range of interest
        if (moving_head->max_sort_key < startval)
        {
            prev_head = moving_head;
            moving_head = moving_head->next_file_ptr;
            continue;
        }

        // If this SST file overlaps with the range of interest, loop through all key-value pairs and add them to the corresponding vector
        if (prev_head != nullptr) {
            prev_head->next_file_ptr = nullptr;
        }
        for (const auto& delete_tile : moving_head->tile_vector) {
            for (const auto& page : delete_tile.page_vector) {
                for (auto & m : page.kv_vector) {
                    // if the kv pair is not overlapping with vector_to_compact, add it to vector_to_populate for future write back
                    if (m.first.first < startval || m.first.first > endval) {
                        vector_to_populate.push_back(m);
                    }
                    else {
                        vector_overlapping.push_back(m);
                    }
                }
            }
        }
        moving_head = moving_head->next_file_ptr;
    }

    // Check if there are key-value pairs to compact and key-value pairs to write back
    if (!vector_to_populate.empty() and !vector_overlapping.empty()) {
        // Sort the key-value pairs to keep by sort key
        std::sort(vector_to_populate.begin(), vector_to_populate.end(), Utility::sortbysortkey);

        // Check if the entire level is overlapping with vector_to_compact
        if (prev_head == nullptr && level_head == DiskMetaFile::getSSTFileHead(level_to_flush_in)) {
            DiskMetaFile::setSSTFileHead(nullptr, level_to_flush_in);
        }

        // Perform compaction and flush on the key-value pairs to write back
        int write_file_count = compactAndFlush(vector_to_populate, level_to_flush_in);
        return write_file_count;
    }
    else {
        // If there are no key-value pairs to write back, return 0
        return 0;
    }
}



// Class : WorkloadExecutor
int WorkloadExecutor::insert(long sortkey, long deletekey, string value)
{
  EmuEnv* _env = EmuEnv::getInstance();
  bool found = false;
  //For UPDATES in inserts
  for (int i = 0; i < MemoryBuffer::buffer.size(); ++i)
  {
    if (MemoryBuffer::buffer[i].first.first == sortkey)
    {
      MemoryBuffer::setCurrentBufferStatistics(0, (value.size() - MemoryBuffer::buffer[i].second.size()));
      MemoryBuffer::buffer[i].second = value;
      found = true;
      //std::cout << "key updated : " << key << std::endl;
      //MemoryBuffer::getCurrentBufferStatistics();

      total_insert_count++;
      buffer_update_count++;
      break;
    }
  }

  //For INSERTS in inserts
  if (!found)
  {
    MemoryBuffer::setCurrentBufferStatistics(1, (sizeof(sortkey) + sizeof(deletekey) + value.size()));
    MemoryBuffer::buffer.push_back(make_pair(make_pair(sortkey, deletekey), value));
    //std::cout << "key inserted : " << key << std::endl;
    //MemoryBuffer::getCurrentBufferStatistics();
    total_insert_count++;
    buffer_insert_count++;
  }

  int write_file_count = 0;
  if (MemoryBuffer::current_buffer_saturation >= MemoryBuffer::buffer_flush_threshold)
  {
    //MemoryBuffer::getCurrentBufferStatistics();
    // MemoryBuffer::printBufferEntries();
    // if(MemoryBuffer::verbosity == 2)
    //std::cout << "Buffer full :: Sorting buffer " ;

    //DiskMetaFile::printAllEntries(0);     //Comment

    if (MemoryBuffer::verbosity == 1)
    {
      std::cout << ":::: Buffer full :: Flushing buffer to Level 1 " << std::endl;
      MemoryBuffer::printBufferEntries();
    }

    write_file_count = MemoryBuffer::initiateBufferFlush(1);
    if (write_file_count)
    {
      if (MemoryBuffer::verbosity == 2)
        std::cout << "Buffer flushed :: Resizing buffer ( size = " << MemoryBuffer::buffer.size() << " ) ";
      MemoryBuffer::buffer.resize(0);
      if (MemoryBuffer::verbosity == 2)
        std::cout << ":::: Buffer resized ( size = " << MemoryBuffer::buffer.size() << " ) " << std::endl;
      MemoryBuffer::current_buffer_entry_count = 0;
      MemoryBuffer::current_buffer_saturation = 0;
      MemoryBuffer::current_buffer_size = 0;
    }
  }
  counter++;

  // if(counter % (_env->num_inserts/100) == 0 && _env->verbosity < 2)
  //     showProgress(_env->num_inserts, counter);

  return write_file_count;
}

int WorkloadExecutor::search(long key, int possible_level_of_occurrence)
{
  SSTFile *level_head_reference = DiskMetaFile::getSSTFileHead(possible_level_of_occurrence);
  // while (level_head_reference != NULL) {
  //   for(int i = 0; i<level_head_reference->file_instance.size(); ++i) {
  //     if(level_head_reference->file_instance[i].first == key) {
  //       //std::cout << "Match found for key " << key << std::endl;
  //       return 1;
  //     }
  //   }
  //   level_head_reference = level_head_reference->next_file_ptr;
  // }
  return 0;
}


int WorkloadExecutor::getWorkloadStatictics(EmuEnv *_env)
{
  std::cout << "************* PRINTING WORKLOAD STATISTICS *************" << std::endl;
  std::cout << "Total inserts in buffer = " << total_insert_count << " (unique inserts = " << buffer_insert_count << "; in-place updates = " << buffer_update_count << ")" << std::endl;

  std::cout << "Insert stats: ";
  int total_compactions = 0;
  long total_files_in_compcations = 0;
  for (int i = 0; i < 32; i++)
  {
    total_compactions += DiskMetaFile::compaction_through_sortmerge_counter[i];
    total_files_in_compcations += DiskMetaFile::compaction_file_counter[i];
  }
  //int disk_pages_per_file = _env->buffer_size_in_pages/_env->buffer_split_factor;
  int disk_pages_per_file = _env->buffer_size_in_pages; //Doubt
  std::cout << "#compactions = " << total_compactions << ", #files_in_compactions = " << total_files_in_compcations << ", #IOs = " << 2 * total_files_in_compcations * disk_pages_per_file
            << " (#IOs does not include IOs for only writing or pointer manipulation)" << std::endl;

  std::cout << "Disk space amplification = ";
  SSTFile *level_1_head = DiskMetaFile::getSSTFileHead(1);
  long total_entries_in_L1 = 0;
  long duplicate_key_count = 0;
  // while (level_1_head != NULL) {
  //   total_entries_in_L1 += level_1_head->file_instance.size();
  //   for(int i = 0; i<level_1_head->file_instance.size(); ++i) {
  //     for(int j=1; j<32; ++j) {
  //       if( level_1_head->file_instance[i].first <= DiskMetaFile::level_min_key[1]) {
  //         //std::cout << "key smaller than min of L2 \n";
  //         break;
  //       }
  //       if( level_1_head->file_instance[i].first <= DiskMetaFile::level_max_key[j]) {
  //         duplicate_key_count += search(level_1_head->file_instance[i].first, j+1);
  //         break;
  //       }
  //     }
  //   }
  //   level_1_head = level_1_head->next_file_ptr;
  // }
  float space_amplification = (float)duplicate_key_count / total_entries_in_L1;
  std::cout << space_amplification << " (entries in L1 = " << total_entries_in_L1 << " / duplicated entries = " << duplicate_key_count << ")" << std::endl;

  std::cout << "********************************************************" << std::endl;

  return 1;
}
