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

int Utility::compactAndFlush(vector < pair < pair < long, long >, string > > vector_to_compact, int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();
  
  int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;
  int file_count = vector_to_compact.size() / entries_per_file;
  if (MemoryBuffer::verbosity == 2)
    std::cout << "\nwriting " << file_count << " file(s)\n";

  SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
  SSTFile *moving_head = head_level_1;
  SSTFile *moving_head_prev = head_level_1;
  SSTFile *end_ptr = moving_head;

  if (!head_level_1)
  {
    for (int i = 0; i < file_count; i++)
    {
      vector<pair<pair<long, long>, string>> vector_to_populate_file;
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
      if (moving_head->min_sort_key >= vector_to_compact[vector_to_compact.size() - 1].first.first )
      {
        moving_head = moving_head->next_file_ptr;
        continue;
      }
      end_ptr = moving_head->next_file_ptr;
      moving_head = moving_head->next_file_ptr;
    }

    if (MemoryBuffer::verbosity == 2)
      cout << "Calculated end pointer" << endl;
    
    moving_head = head_level_1;
    int flag = 0; //1 means vector to be appended at the begining

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
        flag = 1; //vector to be appended very begining
      }

      for (int i = 0; i < file_count; i++)
      {
        vector<pair<pair<long, long>, string>> vector_to_populate_file;
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
          std::cout << "\npopulating file " << head_level_1 << std::endl;

        SSTFile *new_file = SSTFile::createNewSSTFile(level_to_flush_in);
        //SSTFile *moving_head = head_level_1;
        if (moving_head == DiskMetaFile::getSSTFileHead(level_to_flush_in) && i==0 && flag!=2)    //Further Optimize?
        {
          DiskMetaFile::setSSTFileHead(new_file, level_to_flush_in);
        }

        int status = SSTFile::PopulateFile(new_file, vector_to_populate_file, level_to_flush_in);
        new_file->next_file_ptr = end_ptr;
        if (flag == 0 || flag == 2)
        {
          moving_head_prev->next_file_ptr = new_file;
          moving_head_prev = new_file;
        }
        
        vector_to_populate_file.clear();
      }
      break;
    }
  }
}

int Utility::sortAndWrite(vector < pair < pair < long, long >, string > > vector_to_compact, int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();
  SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
  int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;

  std::sort(vector_to_compact.begin(), vector_to_compact.end(), Utility::sortbysortkey);
  long startval =  vector_to_compact[0].first.first;
  long endval =  vector_to_compact[vector_to_compact.size()-1].first.first;

  if (!head_level_1)
  {
    if (MemoryBuffer::verbosity == 2)
      std::cout << "NULL" << std::endl;
    if (vector_to_compact.size() % _env->delete_tile_size_in_pages != 0 && vector_to_compact.size() / _env->delete_tile_size_in_pages < 1)
    {
      std::cout << " ERROR " << std::endl;
      exit(1);
    }
    else
    {
      compactAndFlush(vector_to_compact, level_to_flush_in);
    }
  }

  else
  {
    if (MemoryBuffer::verbosity == 2)
      std::cout << "head not null anymore" << std::endl;
    SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
    SSTFile *moving_head = head_level_1;

    if (MemoryBuffer::verbosity == 2)
      std::cout << "Vector size before merging : " << vector_to_compact.size() << std::endl;
    
    while (moving_head)
    {
      if (moving_head->min_sort_key >= endval || moving_head->max_sort_key <= startval )
      {
        if (MemoryBuffer::verbosity == 2)
          cout << "Performed Optimization" << endl;
        moving_head = moving_head->next_file_ptr;
        continue;
      }
      
      for (int k = 0; k < moving_head->tile_vector.size(); k++)
      {
        DeleteTile delete_tile = moving_head->tile_vector[k];
        for (int l = 0; l < delete_tile.page_vector.size(); l++)
        {
          Page page = delete_tile.page_vector[l];
          for (int m = 0; m < page.kv_vector.size(); m++)
          {
            vector_to_compact.push_back(page.kv_vector[m]);
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }

    if (MemoryBuffer::verbosity == 2)
      std::cout << "Vector size after merging : " << vector_to_compact.size() << std::endl;

    std::sort(vector_to_compact.begin(), vector_to_compact.end(), Utility::sortbysortkey);
    int file_count = vector_to_compact.size() / entries_per_file;
    
    if (MemoryBuffer::verbosity == 2)
    {
      std::cout << "\nprinting before compacting " << std::endl;
      for (int j = 0; j < vector_to_compact.size(); ++j)
        std::cout << "< " << vector_to_compact[j].first.first << ",  " << vector_to_compact[j].first.second << " >"
                  << "\t" << std::endl;
    }
    
    compactAndFlush(vector_to_compact, level_to_flush_in);
  }
  int saturation = DiskMetaFile::checkAndAdjustLevelSaturation(level_to_flush_in);
  //DiskMetaFile::printAllEntries(0);
}

// Class : WorkloadExecutor

int WorkloadExecutor::insert(long sortkey, long deletekey, string value)
{
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

  if (MemoryBuffer::current_buffer_saturation >= MemoryBuffer::buffer_flush_threshold)
  {
    //MemoryBuffer::getCurrentBufferStatistics();
    // MemoryBuffer::printBufferEntries();
    // if(MemoryBuffer::verbosity == 2)
    //std::cout << "Buffer full :: Sorting buffer " ;

    if (MemoryBuffer::verbosity == 2)
    {
      std::cout << ":::: Buffer full :: Flushing buffer to Level 1 " << std::endl;
      MemoryBuffer::printBufferEntries();
    }

    //std::sort( MemoryBuffer::buffer.begin(), MemoryBuffer::buffer.end() );
    int status = MemoryBuffer::initiateBufferFlush(1);
    if (status)
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

  return 1;
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