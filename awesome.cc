/*
 *  Created on: May 14, 2019
 *  Author: Subhadeep
 */

#include <iostream>
#include <cmath>
#include <sys/time.h>
#include <vector>
#include <cstdlib>
#include <algorithm>
#include <iomanip>

#include "emu_environment.h"
#include "awesome.h"

using namespace std;
using namespace awesome;

/*Set up the singleton object with the experiment wide options*/
MemoryBuffer *MemoryBuffer::buffer_instance = 0;
long MemoryBuffer::max_buffer_size = 0;
float MemoryBuffer::buffer_flush_threshold = 0;
long MemoryBuffer::current_buffer_entry_count = 0;
long MemoryBuffer::current_buffer_size = 0;
float MemoryBuffer::current_buffer_saturation = 0;
//int MemoryBuffer::buffer_split_factor = 0;
int MemoryBuffer::buffer_flush_count = 0;
//MTIP
vector<pair<pair<long, long>, string>> MemoryBuffer::buffer;

int MemoryBuffer::verbosity = 0;

long WorkloadExecutor::buffer_insert_count = 0;
long WorkloadExecutor::buffer_update_count = 0;
long WorkloadExecutor::total_insert_count = 0;

//SSTFile* DiskMetaFile::head_level_1 = NULL;
SSTFile *DiskMetaFile::level_head[32] = {};
long DiskMetaFile::level_min_key[32] = {};
long DiskMetaFile::level_max_key[32] = {};
long DiskMetaFile::level_file_count[32] = {};
long DiskMetaFile::level_entry_count[32] = {};
long DiskMetaFile::level_max_size[32] = {};
long DiskMetaFile::level_current_size[32] = {};

long DiskMetaFile::global_level_file_counter[32] = {};
long DiskMetaFile::global_level_counter = 0;
float DiskMetaFile::disk_run_flush_threshold[32] = {};

int DiskMetaFile::compaction_counter[32] = {};
int DiskMetaFile::compaction_through_sortmerge_counter[32] = {};
int DiskMetaFile::compaction_through_point_manipulation_counter[32] = {};
int DiskMetaFile::compaction_file_counter[32] = {};

bool sortbysortkey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b);
bool sortbydeletekey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b);
int sortAndWrite(vector<pair<pair<long, long>, string>> file_to_sort, int level_to_flush_in);
int compactAndFlush(vector<pair<pair<long, long>, string>> vector_to_compact, int level_to_flush_in);
int PopulateFile(SSTFile *arg, vector<pair<pair<long, long>, string>> temp_vector, int level_to_flush_in);
int PopulateDeleteTile(SSTFile *arg, vector<pair<pair<long, long>, string>> temp_vector, int deletetileid, int level_to_flush_in);
int PrintAllEntries();
int generateMetaData(SSTFile *file, DeleteTile &deletetile, Page &page, long sort_key_to_insert, long delete_key_to_insert);

// CLASS : MemoryBuffer

MemoryBuffer::MemoryBuffer(EmuEnv *_env)
{
  max_buffer_size = _env->buffer_size;
  buffer_flush_threshold = _env->buffer_flush_threshold;
  verbosity = _env->verbosity;

  for (int i = 0; i < 32; ++i)
  {
    DiskMetaFile::disk_run_flush_threshold[i] = _env->disk_run_flush_threshold;
    DiskMetaFile::level_max_size[i] = _env->buffer_size * pow(_env->size_ratio, (i + 1));
  }
}

MemoryBuffer *MemoryBuffer::getBufferInstance(EmuEnv *_env)
{
  if (buffer_instance == 0)
    buffer_instance = new MemoryBuffer(_env);

  return buffer_instance;
}

int MemoryBuffer::setCurrentBufferStatistics(int entry_count_change, int entry_size_change)
{
  current_buffer_entry_count += entry_count_change;
  current_buffer_size += entry_size_change;
  current_buffer_saturation = (float)current_buffer_size / max_buffer_size;

  return 1;
}

int MemoryBuffer::getCurrentBufferStatistics()
{
  std::cout << "********** PRINTING CURRENT BUFFER STATISTICS **********" << std::endl;
  std::cout << "Current buffer entry count = " << current_buffer_entry_count << std::endl;
  std::cout << "Current buffer size = " << current_buffer_size << std::endl;
  std::cout << "Current buffer saturation = " << current_buffer_saturation << std::endl;
  std::cout << "Total buffer flushes  = " << buffer_flush_count << std::endl;
  std::cout << "********************************************************" << std::endl;
  return 1;
}

int MemoryBuffer::initiateBufferFlush(int level_to_flush_in)
{ //we can replace level_to_flush_in by 0

  int entries_per_file = MemoryBuffer::current_buffer_entry_count;
  sortAndWrite(MemoryBuffer::buffer, level_to_flush_in);

  MemoryBuffer::buffer_flush_count++;
  //DiskMetaFile::setMetaStatistics(level_to_flush_in);
  return 1;
}

int PopulateDeleteTile(SSTFile *file, vector<pair<pair<long, long>, string>> vector_to_populate_tile, int deletetileid, int level_to_flush_in)
{

  std::cout << "In PopulateDeleteTile() ... " << std::endl;

  EmuEnv *_env = EmuEnv::getInstance();
  int page_count = _env->delete_tile_size_in_pages;
  for (int i = 0; i < page_count; ++i)
  {
    vector<pair<pair<long, long>, string>> vector_to_populate_page;
    for (int j = 0; j < _env->entries_per_page; ++j)
    {
      vector_to_populate_page.push_back(vector_to_populate_tile[j]);
    }

    std::sort(vector_to_populate_page.begin(), vector_to_populate_page.end(), sortbysortkey);

    std::cout << "\npopulating pages :: vector length = " << vector_to_populate_tile.size() << std::endl;

    for (int j = 0; j < _env->entries_per_page; ++j)
    {
      long sort_key_to_insert = vector_to_populate_page[j].first.first;
      long delete_key_to_insert = vector_to_populate_page[j].first.second;

      int status = generateMetaData(file, file->tile_vector[deletetileid],
                                    file->tile_vector[deletetileid].page_vector[i], sort_key_to_insert, delete_key_to_insert);

      file->tile_vector[deletetileid].page_vector[i].kv_vector.push_back(make_pair(make_pair(
                                                                                       vector_to_populate_page[j].first.first, vector_to_populate_page[j].first.second),
                                                                                   vector_to_populate_page[j].second));

      std::cout << vector_to_populate_page[j].first.first << " " << vector_to_populate_page[j].first.second << std::endl;
    }

    vector_to_populate_tile.erase(vector_to_populate_tile.begin(), vector_to_populate_tile.begin() + _env->entries_per_page);

    std::cout << "populated pages\n"
              << std::endl;

    vector_to_populate_page.clear();
  }

  return 1;
}

int PopulateFile(SSTFile *file, vector<pair<pair<long, long>, string>> vector_to_populate_file, int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();
  int delete_tile_count = _env->buffer_size_in_pages / _env->delete_tile_size_in_pages;
  //cout<<"Vector Size: "<<vector_to_populate_file.size()<<std::endl;

  std::cout << "In PopulateFile() ... " << std::endl;

  for (int i = 0; i < delete_tile_count; i++)
  {
    vector<pair<pair<long, long>, string>> vector_to_populate_tile;
    for (int j = 0; j < _env->delete_tile_size_in_pages * _env->entries_per_page; ++j)
    {
      vector_to_populate_tile.push_back(vector_to_populate_file[j]);
    }
    std::sort(vector_to_populate_tile.begin(), vector_to_populate_tile.end(), sortbydeletekey);

    vector_to_populate_file.erase(vector_to_populate_file.begin(), vector_to_populate_file.begin() + _env->delete_tile_size_in_pages * _env->entries_per_page);

    // std::cout << "\nprinting after trimming " << std::endl;
    // for (int j = 0; j < vector_to_populate_file.size(); ++j)
    //   std::cout << "< " << vector_to_populate_file[j].first.first << ",  " << vector_to_populate_file[j].first.second << " >" << "\t";
    std::cout << "populating delete tile ... \n";
    int status = PopulateDeleteTile(file, vector_to_populate_tile, i, level_to_flush_in);
    vector_to_populate_tile.clear();
  }
  return 1;
}

int compactAndFlush(vector<pair<pair<long, long>, string>> vector_to_compact, int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();
  SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
  int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;
  int file_count = vector_to_compact.size() / entries_per_file;
  std::cout << "\nwriting " << file_count << " file(s)\n";
  for (int i = 0; i < file_count; i++)
  {
    vector<pair<pair<long, long>, string>> vector_to_populate_file;
    for (int j = 0; j < entries_per_file; ++j)
    {
      vector_to_populate_file.push_back(vector_to_compact[j]);
    }

    std::cout << "\nprinting before trimming " << std::endl;
    for (int j = 0; j < vector_to_compact.size(); ++j)
      std::cout << "< " << vector_to_compact[j].first.first << ",  " << vector_to_compact[j].first.second << " >"
                << "\t";

    vector_to_compact.erase(vector_to_compact.begin(), vector_to_compact.begin() + entries_per_file);

    std::cout << "\nprinting after trimming " << std::endl;
    for (int j = 0; j < vector_to_compact.size(); ++j)
      std::cout << "< " << vector_to_compact[j].first.first << ",  " << vector_to_compact[j].first.second << " >"
                << "\t";

    std::cout << "\npopulating file " << head_level_1 << std::endl;

    SSTFile *new_file = SSTFile::createNewSSTFile(level_to_flush_in);
    SSTFile *moving_head = head_level_1;

    if (i == 0)
    {
      head_level_1 = new_file;
      DiskMetaFile::setSSTFileHead(head_level_1, level_to_flush_in);
      int status = PopulateFile(new_file, vector_to_populate_file, level_to_flush_in);
    }

    else
    {
      while (moving_head->next_file_ptr)
      {
        moving_head = moving_head->next_file_ptr;
      }
      int status = PopulateFile(new_file, vector_to_populate_file, level_to_flush_in);
      moving_head->next_file_ptr = new_file;
    }

    vector_to_populate_file.clear();
  }
}

int sortAndWrite(vector<pair<pair<long, long>, string>> vector_to_compact, int level_to_flush_in)
{

  EmuEnv *_env = EmuEnv::getInstance();
  SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
  int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;

  if (!head_level_1)
  {
    std::cout << "NULL" << std::endl;

    std::sort(vector_to_compact.begin(), vector_to_compact.end(), sortbysortkey);
    if (vector_to_compact.size() % _env->delete_tile_size_in_pages != 0 && vector_to_compact.size() / _env->delete_tile_size_in_pages < 1)
    {
      std::cout << " ERROR " << std::endl;
      exit(1);
    }
    else
    {
      compactAndFlush(vector_to_compact,level_to_flush_in);
    }
    DiskMetaFile::global_level_counter++;
  }

  else
  {
    std::cout << "head not null anymore" << std::endl;
    SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
    SSTFile *moving_head = head_level_1;

    std::cout << "Vector size before merging : " << vector_to_compact.size() << std::endl;

    while (moving_head)
    {
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

    std::cout << "Vector size after merging : " << vector_to_compact.size() << std::endl;

    std::sort(vector_to_compact.begin(), vector_to_compact.end(), sortbysortkey);
    compactAndFlush(vector_to_compact,level_to_flush_in);
  }

  int status = PrintAllEntries();

}

int DiskMetaFile::initateCompaction(int compaction_mode)
{
  if (MemoryBuffer::verbosity == 2)
  {
    std::cout << "---------- Initiating Compaction Routine ----------" << std::endl;
    DiskMetaFile::printFileEntries();
  }

  SSTFile *head_L1 = DiskMetaFile::getSSTFileHead(1);

  if (DiskMetaFile::getSSTFileHead(2) == NULL)
  {
    if (MemoryBuffer::verbosity == 2)
      std::cout << "Creating new level." << std::endl;
    // Choose median file in Level 1
    SSTFile *predecessor_ptr = head_L1;
    SSTFile *traversing_ptr = predecessor_ptr->next_file_ptr;
    long median_index;
    if (DiskMetaFile::level_file_count[0] % 2 == 0)
      median_index = 1 + (long)DiskMetaFile::level_file_count[0] / 2; // this is not exactly the median index, but its following index ; this ensures that the first file in  L1 is never chosen for compaction, i.e., the head for L1 remains unaltered
    else
      median_index = (long)(DiskMetaFile::level_file_count[0] + 1) / 2; // median if file_count == odd

    for (long i = 0; i < median_index - 2; ++i)
    {
      predecessor_ptr = predecessor_ptr->next_file_ptr;
      traversing_ptr = predecessor_ptr->next_file_ptr;
    }
    predecessor_ptr->next_file_ptr = traversing_ptr->next_file_ptr;

    // Flush median file to Level 2
    // Resetting new level_head for the matching Level and resetting level-statistics
    traversing_ptr->next_file_ptr = NULL; // setting to NULL, as this is the first file in Level 2
    DiskMetaFile::setSSTFileHead(traversing_ptr, 2);
    DiskMetaFile::setMetaStatistics(2);
    // Resetting statistics for Level 1
    DiskMetaFile::setMetaStatistics(1);

    if (MemoryBuffer::verbosity == 2)
    {
      DiskMetaFile::printFileEntries();
      DiskMetaFile::getMetaStatistics();
    }
  }

  else
  {
    // Choose a file in Level 1 based on a selection criteria
    // SSTFile* predecessor_ptr = head_L1;
    // SSTFile* traversing_ptr = predecessor_ptr->next_file_ptr;

    // while(traversing_ptr != NULL) {
    //   long min_key_selected_file, max_key_selected_file;
    //   min_key_selected_file = traversing_ptr->file_instance[0].first;
    //   max_key_selected_file = traversing_ptr->file_instance[traversing_ptr->file_instance.size() - 1 ].first;

    //   if(MemoryBuffer::verbosity == 2)
    //     std::cout << "Searching levels for min key (" << min_key_selected_file << ") and max key (" << max_key_selected_file << ")" << std::endl;
    //   int min_key_level = DiskMetaFile::getKeyLevel(min_key_selected_file);
    //   int max_key_level = DiskMetaFile::getKeyLevel(max_key_selected_file);

    //   if(min_key_level == max_key_level) {
    //     if(MemoryBuffer::verbosity == 2)
    //       std::cout << "Min and Max keys are in same level :: Initiating compaction between Level 1 and Level " << min_key_level << std::endl;

    //     pair <long, long> matching_key_file = DiskMetaFile::getMatchingKeyFile(min_key_selected_file, max_key_selected_file, min_key_level);

    //     if(MemoryBuffer::verbosity == 2) {
    //       std::cout << "At Level " << min_key_level << ", Min key (" << min_key_selected_file << ") found in file " << matching_key_file.first;
    //       std::cout << " and Max key (" << max_key_selected_file << ") found in file " << matching_key_file.second << std::endl;
    //     }

    //     // Removing selected file from Level 1 and resetting Level 1 statistics
    //     predecessor_ptr->next_file_ptr = traversing_ptr->next_file_ptr;
    //     traversing_ptr->next_file_ptr = NULL;
    //     DiskMetaFile::setMetaStatistics(1);

    //     // Merging selected file from Level 1 with (all of) the matching Level
    //     SSTFile* matching_level_head_reference = DiskMetaFile::getSSTFileHead(min_key_level);
    //     vector < pair < long, string > > vector_to_populate_f;
    //     vector <long> duplicate_key_search_space;

    //     for(int i=0; i < traversing_ptr->file_instance.size(); ++i) {
    //       temp_vector.push_back( make_pair( traversing_ptr->file_instance[i].first, traversing_ptr->file_instance[i].second ) );
    //       duplicate_key_search_space.push_back(traversing_ptr->file_instance[i].first);
    //     }

    //     while(matching_level_head_reference != NULL) {
    //       for(int i=0; i < matching_level_head_reference->file_instance.size(); ++i) {
    //         long key = matching_level_head_reference->file_instance[i].first;
    //         if(!std::binary_search(duplicate_key_search_space.begin(), duplicate_key_search_space.end(), key)) {
    //           temp_vector.push_back( make_pair( matching_level_head_reference->file_instance[i].first, matching_level_head_reference->file_instance[i].second ) );
    //         }
    //       }
    //       matching_level_head_reference = matching_level_head_reference->next_file_ptr;
    //     }

    //     std::sort(temp_vector.begin(), temp_vector.end());

    //     // New content
    //     //int entries_per_file = MemoryBuffer::current_buffer_entry_count / MemoryBuffer::buffer_split_factor;
    //     int entries_per_file = MemoryBuffer::current_buffer_entry_count;

    //     SSTFile* new_matching_level_head_reference = SSTFile::createNewSSTFile(min_key_level);
    //     // std::cout << "Printing 1 :: " << head_level_1->file_id  << std::endl;

    //     SSTFile* current_file = new_matching_level_head_reference;
    //     for (int i=0; i < temp_vector.size(); ) {
    //       do {
    //         current_file->file_instance.push_back( make_pair( temp_vector[i].first, temp_vector[i].second ) );
    //         ++i;
    //         //std::cout << i << " : " << temp_vector[i].first << " ::\t";
    //       } while( i % entries_per_file != 0 && i < temp_vector.size());

    //       //std::cout << "i = " << i << " :: creating new SST File " << std::endl;
    //       if( i != temp_vector.size() ) {
    //         SSTFile* new_SSTfile = SSTFile::createNewSSTFile(min_key_level);
    //         current_file->next_file_ptr = new_SSTfile;
    //         current_file = new_SSTfile;
    //       }
    //     }
    //     // Resetting new level_head for the matching Level and resetting level-statistics
    //     DiskMetaFile::setSSTFileHead(new_matching_level_head_reference, min_key_level);
    //     DiskMetaFile::setMetaStatistics(min_key_level);

    //     //Setting compaction statistics
    //     DiskMetaFile::compaction_counter[min_key_level-1]++;
    //     if(matching_key_file.first == 0 && matching_key_file.second == 0) {
    //       if(MemoryBuffer::verbosity == 2)
    //         std::cout << "No compaction required, simple pointer manipulation (at the beginning) can realize the compaction!" << std::endl;
    //       DiskMetaFile::compaction_through_point_manipulation_counter[min_key_level-1]++;
    //     }
    //     else if(matching_key_file.first == 0 && matching_key_file.second !=0) {
    //       if(MemoryBuffer::verbosity == 2)
    //         std::cout << "Compaction required, involves" << matching_key_file.second + 1 << " files!" << std::endl;
    //       DiskMetaFile::compaction_file_counter[min_key_level-1] += (matching_key_file.second + 1);
    //       DiskMetaFile::compaction_through_sortmerge_counter[min_key_level-1]++;
    //     }
    //     else if(matching_key_file.first == -1 && matching_key_file.second == -1) {
    //       if(MemoryBuffer::verbosity == 2)
    //         std::cout << "No compaction required, simple pointer manipulation (at the end) can realize the compaction!" << std::endl;
    //       DiskMetaFile::compaction_through_point_manipulation_counter[min_key_level-1]++;
    //     }
    //     else if(matching_key_file.first != -1 && matching_key_file.second == -1) {
    //       DiskMetaFile::compaction_file_counter[min_key_level-1] += (DiskMetaFile::level_file_count[min_key_level-1] - matching_key_file.first + 2);
    //       DiskMetaFile::compaction_through_sortmerge_counter[min_key_level-1]++;
    //     }
    //     else {
    //       if(MemoryBuffer::verbosity == 2)
    //         std::cout << "Compaction required, involves" << matching_key_file.second - matching_key_file.first + 2 << " files!" << std::endl;
    //       DiskMetaFile::compaction_file_counter[min_key_level-1] += (matching_key_file.second - matching_key_file.first + 2);
    //       DiskMetaFile::compaction_through_sortmerge_counter[min_key_level-1]++;
    //     }

    //     if(MemoryBuffer::verbosity == 2) {
    //       DiskMetaFile::printFileEntries();
    //       DiskMetaFile::getMetaStatistics();
    //     }
    //     // Check if the selected Level has reached its capacity
    //     DiskMetaFile::checkAndAdjustLevelSaturation(min_key_level);

    //     break;
    //   }

    //   else {
    //     if(MemoryBuffer::verbosity == 2)
    //       std::cout << "Min key is in Level " << min_key_level << ", while max key is in Level " << max_key_level << " :: checking next file" << std::endl;
    //     if(traversing_ptr->next_file_ptr == NULL)
    //       exit(1);
    //     predecessor_ptr = predecessor_ptr->next_file_ptr;
    //     traversing_ptr = traversing_ptr->next_file_ptr;
    //   }
    // }
  }

  if (MemoryBuffer::verbosity == 2)
    std::cout << "++++++++++ Compaction Complete ++++++++++\n"
              << std::endl;

  return 1;
}

int DiskMetaFile::checkAndAdjustLevelSaturation(int level)
{
  SSTFile *level_head_reference = DiskMetaFile::getSSTFileHead(level);
  SSTFile *predecessor_ptr = level_head_reference;
  SSTFile *traversing_ptr = predecessor_ptr->next_file_ptr;

  if (DiskMetaFile::level_current_size[level - 1] >= DiskMetaFile::level_max_size[level - 1])
  {
    if (MemoryBuffer::verbosity == 2)
      std::cout << "Level " << level << " exceeded capacity after compaction :: Adjusting level size" << std::endl;

    while (traversing_ptr->next_file_ptr != NULL)
    {
      predecessor_ptr = predecessor_ptr->next_file_ptr;
      traversing_ptr = traversing_ptr->next_file_ptr;
    }

    SSTFile *next_level_head_reference = DiskMetaFile::getSSTFileHead(level + 1);
    if (MemoryBuffer::verbosity == 2)
      std::cout << "Fetching pointer for Level = " << level + 1 << std::endl;

    if (next_level_head_reference == NULL)
    {
      predecessor_ptr->next_file_ptr = NULL;
      std::cout << "Level " << level << " full :: Creating new Level " << level + 1 << std::endl;
      DiskMetaFile::setSSTFileHead(traversing_ptr, level + 1);
      // SSTFile* new_next_level_head = SSTFile::createNewSSTFile(level+1);
      // DiskMetaFile::setSSTFileHead(traversing_ptr, level+1);
    }
    else
    {
      predecessor_ptr->next_file_ptr = NULL;
      traversing_ptr->next_file_ptr = next_level_head_reference->next_file_ptr;
      DiskMetaFile::setSSTFileHead(traversing_ptr, level + 1);
    }

    DiskMetaFile::setMetaStatistics(level);
    DiskMetaFile::setMetaStatistics(level + 1);

    DiskMetaFile::checkAndAdjustLevelSaturation(level + 1);
  }

  return 1;
}

pair<long, long> DiskMetaFile::getMatchingKeyFile(long min_key, long max_key, int key_level)
{
  SSTFile *level_head = DiskMetaFile::getSSTFileHead(key_level);
  SSTFile *traversing_ptr = level_head;
  pair<long, long> matching_file = make_pair(-99, -99);
  int file_number = 0;

  // while(traversing_ptr != NULL) {
  //   if(traversing_ptr->file_instance[0].first > min_key ) {
  //     matching_file.first = file_number;
  //     break;
  //   }
  //   else if(min_key >= traversing_ptr->file_instance[0].first && min_key <= traversing_ptr->file_instance[traversing_ptr->file_instance.size()-1].first ) {
  //     ++file_number;
  //     matching_file.first = file_number;
  //     break;
  //   }
  //   else {
  //     traversing_ptr = traversing_ptr->next_file_ptr;
  //     ++file_number;
  //   }
  // }
  // if(!traversing_ptr)
  //   matching_file.first = -1;

  // traversing_ptr = level_head;
  // file_number = 0;
  // while(traversing_ptr != NULL) {
  //   if(traversing_ptr->file_instance[0].first > max_key ) {
  //     matching_file.second = file_number;
  //     break;
  //   }
  //   else if(max_key >= traversing_ptr->file_instance[0].first && max_key <= traversing_ptr->file_instance[traversing_ptr->file_instance.size()-1].first ) {
  //     ++file_number;
  //     matching_file.second = file_number;
  //     break;
  //   }
  //   else {
  //     traversing_ptr = traversing_ptr->next_file_ptr;
  //     ++file_number;
  //   }
  // }
  if (!traversing_ptr)
    matching_file.second = -1;

  return matching_file;
}

int DiskMetaFile::getCurrentLevelCount()
{
  int level_count = 0;
  for (int i = 0; i < 32; ++i)
  {
    if (DiskMetaFile::level_head[i] != NULL)
      level_count++;
    else
      return level_count;
  }
  return -1;
}

int DiskMetaFile::getKeyLevel(long key)
{
  int level_count = DiskMetaFile::getCurrentLevelCount();
  for (int i = 1; i < level_count; ++i)
  {
    if (DiskMetaFile::level_max_key[i] >= key)
      return i + 1;
  }
  return level_count;
}

// Class : WorkloadExecutor

int WorkloadExecutor::insert(long sortkey, long deletekey, string value)
{
  bool found = false;
  //std::cout << sortkey << std::endl;

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

  // std::cout << "Printing 2 :: " << DiskMetaFile::head_level_1->file_id << std::endl ;
  //found = false;

  return 1;
}

int MemoryBuffer::printBufferEntries()
{
  long size = 0;
  std::cout << "Printing sorted buffer (only keys): ";
  //std::cout << MemoryBuffer::buffer.size() << std::endl;
  for (int i = 0; i < MemoryBuffer::buffer.size(); ++i)
  {
    std::cout << "< " << MemoryBuffer::buffer[i].first.first << ",  " << MemoryBuffer::buffer[i].first.second << " >"
              << "\t";
    size += (2 * sizeof(MemoryBuffer::buffer[i].first.first) + MemoryBuffer::buffer[i].second.size());
  }
  std::cout << std::endl;
  //std::cout << "Buffer size = " << size << std::endl;

  return 1;
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

SSTFile *SSTFile::createNewSSTFile(int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();

  SSTFile *new_file = new SSTFile();
  new_file->max_sort_key = -1;
  new_file->max_delete_key = -1;
  new_file->min_sort_key = -1;
  new_file->min_delete_key = -1;

  new_file->tile_vector = DeleteTile::createNewDeleteTiles(_env->delete_tile_size_in_pages);

  int page_per_delete_tile = _env->buffer_size_in_pages / _env->delete_tile_size_in_pages;
  for (int i = 0; i < new_file->tile_vector.size(); ++i)
    new_file->tile_vector[i].page_vector = Page::createNewPages(page_per_delete_tile);

  new_file->file_level = level_to_flush_in;
  new_file->next_file_ptr = NULL;
  DiskMetaFile::global_level_file_counter[level_to_flush_in]++;
  new_file->file_id = "L" + std::to_string(level_to_flush_in) + "F" + std::to_string(DiskMetaFile::global_level_file_counter[level_to_flush_in]);
  //std::cout << "Creating new file !!" << std::endl;

  return new_file;
}

//MTIP

vector<Page> Page::createNewPages(int page_count)
{
  //EmuEnv* _env = EmuEnv::getInstance();
  vector<Page> pages(page_count);
  for (int i = 0; i < pages.size(); i++)
  {
    vector<pair<pair<long, long>, string>> kv_vect;
    pages[i].min_sort_key = -1;
    pages[i].max_sort_key = -1;

    pages[i].kv_vector = kv_vect;
  }

  return pages;
}

vector<DeleteTile> DeleteTile::createNewDeleteTiles(int delete_tile_size_in_pages)
{
  EmuEnv *_env = EmuEnv::getInstance();
  vector<DeleteTile> delete_tiles(delete_tile_size_in_pages);
  for (int i = 0; i < delete_tiles.size(); i++)
  {
    delete_tiles[i].max_sort_key = -1;
    delete_tiles[i].min_sort_key = -1;
    delete_tiles[i].max_delete_key = -1;
    delete_tiles[i].min_delete_key = -1;
    delete_tiles[i].page_vector = Page::createNewPages(_env->buffer_size_in_pages);
  }

  //new_delete_tile.page_vector =
  return delete_tiles;
}

// int DiskMetaFile::setTestFileHead(TestFile arg, int level) {
//   DiskMetaFile::level_head[level]=arg;
//   return 1;

// }

// TestFile DiskMetaFile::getTestFileHead(TestFile arg, int level) {
//   return DiskMetaFile::level_head[level];
// }

int DiskMetaFile::setSSTFileHead(SSTFile *arg, int level_to_flush_in)
{
  // std::cout << "Printing 2 :: " << arg->file_id  << std::endl ;
  DiskMetaFile::level_head[level_to_flush_in] = arg;
  //std::cout << "Printing 3 :: " << DiskMetaFile::head_level_1->file_id  << std::endl ;

  return 1;
}

SSTFile *DiskMetaFile::getSSTFileHead(int level)
{
  return DiskMetaFile::level_head[level];
}

int DiskMetaFile::setMetaStatistics(int level)
{
  SSTFile *level_head = DiskMetaFile::getSSTFileHead(level);

  if (level_head == NULL)
    std::cout << "ERROR in here" << std::endl;

  else
  {
    SSTFile *traversing_ptr = level_head;
    // DiskMetaFile::level_min_key[level-1] = level_head->file_instance[0].first;
    // int entry_size = sizeof(long) + level_head->file_instance[0].second.size();
    // DiskMetaFile::level_file_count[level-1] = 0;
    // DiskMetaFile::level_entry_count[level-1] = 0;

    // while(traversing_ptr != NULL) {
    //   DiskMetaFile::level_file_count[level-1] ++;
    //   DiskMetaFile::level_entry_count[level-1] += traversing_ptr->file_instance.size();

    //   if(traversing_ptr->next_file_ptr == NULL ) {
    //     DiskMetaFile::level_max_key[level-1] = traversing_ptr->file_instance[ traversing_ptr->file_instance.size() - 1 ].first;
    //   }
    //   traversing_ptr = traversing_ptr->next_file_ptr;
    // }
    // DiskMetaFile::level_current_size[level-1] = DiskMetaFile::level_entry_count[level-1] * entry_size;
  }

  return 1;
}

int DiskMetaFile::getMetaStatistics()
{
  std::cout << "**************************** PRINTING META FILE STATISTICS ****************************" << std::endl;
  std::cout << "L\tmin_key\t\tmax_key\t\tfile_count\tentry_count\tlevel_size" << std::endl;
  for (int i = 0; i < 32 && DiskMetaFile::level_head[i]; ++i)
  {
    std::cout << i + 1 << "\t" << setfill(' ') << setw(8) << DiskMetaFile::level_min_key[i] << "\t"
              << setfill(' ') << setw(8) << DiskMetaFile::level_max_key[i] << "\t\t" << DiskMetaFile::level_file_count[i] << "\t\t"
              << DiskMetaFile::level_entry_count[i] << "\t\t" << DiskMetaFile::level_current_size[i] << std::endl;
  }
  std::cout << "***************************************************************************************" << std::endl;

  // Printing COMPACTION statistics -- these stats are not set by setMetaStatistics()
  int total_compactions = 0;
  int total_compactions_through_sortmerge = 0;
  int total_compactions_through_pointer_manipulation = 0;
  int total_files_in_compaction = 0;
  std::cout << "L\t#compactions\t#compactions_through_sortmerge\t#compactions_through_pointer_manipulation\t#files_in_compaction\t#bytes_in_compaction" << std::endl;
  for (int i = 1; i < 32 && DiskMetaFile::level_head[i]; ++i)
  {
    total_compactions += DiskMetaFile::compaction_counter[i];
    total_files_in_compaction += DiskMetaFile::compaction_file_counter[i];
    total_compactions_through_sortmerge += DiskMetaFile::compaction_through_sortmerge_counter[i];
    total_compactions_through_pointer_manipulation += DiskMetaFile::compaction_through_point_manipulation_counter[i];

    std::cout << i + 1 << "\t" << setfill(' ') << setw(8) << DiskMetaFile::compaction_counter[i] << "\t\t"
              << setfill(' ') << setw(8) << DiskMetaFile::compaction_through_sortmerge_counter[i] << "\t\t\t"
              << setfill(' ') << setw(8) << DiskMetaFile::compaction_through_point_manipulation_counter[i] << "\t\t\t\t"
              << setfill(' ') << setw(8) << DiskMetaFile::compaction_file_counter[i] << "\t\t\t"
              << setfill(' ') << setw(12) << DiskMetaFile::compaction_file_counter[i] * 4 * 128 << std::endl;
  }
  std::cout << "Total"
            << "\t" << setfill(' ') << setw(8) << total_compactions << "\t\t"
            << setfill(' ') << setw(8) << total_compactions_through_sortmerge << "\t\t\t"
            << setfill(' ') << setw(8) << total_compactions_through_pointer_manipulation << "\t\t\t\t"
            << setfill(' ') << setw(8) << total_files_in_compaction << "\t\t\t"
            << setfill(' ') << setw(12) << (long)total_files_in_compaction * 4 * 128 << std::endl;
  return 1;
}

int DiskMetaFile::printFileEntries()
{
  std::cout << "**************************** PRINTING FILE ENTRIES ****************************" << std::endl;
  for (int i = 0; i < 32 && DiskMetaFile::level_head[i]; ++i)
  {
    std::cout << "Level " << i + 1 << std::endl;
    // for(SSTFile* traversing_ptr = level_head[i]; traversing_ptr != NULL; traversing_ptr = traversing_ptr->next_file_ptr ) {
    //   std::cout << traversing_ptr->file_id << " : ";
    //   for (long j=0; j<traversing_ptr->file_instance.size(); ++j) {
    //     std::cout << traversing_ptr->file_instance[j].first << " ";
    //   }
    //   std::cout << std::endl;
    // }
  }
  std::cout << "*******************************************************************************" << std::endl;

  return 1;
}

int PrintAllEntries()
{
  std::cout << "*******PRINT ALL****************" << std::endl;

  for (int i = 0; i < DiskMetaFile::global_level_counter; i++)
  {

    std::cout << "Level count: " << DiskMetaFile::global_level_counter << std::endl;

    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i + 1);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      // std::cout << "File : " << j << " Min Sort Key : " << moving_head -> min_sort_key << " Max Sort Key : " << moving_head -> max_sort_key << std::endl;
      // std::cout << "File : " << j << " Min Delete Key : " << moving_head -> min_delete_key << " Max Delete Key : " << moving_head -> max_delete_key << std::endl;
      for (int k = 0; k < moving_head->tile_vector.size(); k++)
      {

        DeleteTile delete_tile = moving_head->tile_vector[k];
        // std::cout << "Delete Tile : " << k << " Min Sort Key : " << delete_tile.min_sort_key << " Max Sort Key : " << delete_tile.max_sort_key << std::endl;
        // std::cout << "Delete Tile : " << k << " Min Delete Key : " << delete_tile.min_delete_key << " Max Delete Key : " << delete_tile.max_delete_key << std::endl;
        for (int l = 0; l < delete_tile.page_vector.size(); l++)
        {
          Page page = delete_tile.page_vector[l];
          //std::cout << "Page : " << l << " Min Sort Key : " << page.min_sort_key << " Max Sort Key : " << page.max_sort_key << std::endl;

          for (int m = 0; m < page.kv_vector.size(); m++)
          {
            std::cout << "Level : " << i << "\tFile: " << moving_head->file_id << "\t\tDelete tile : " << k << "\t\tPage : "
                      << l << "\tSort Key: " << page.kv_vector[m].first.first
                      << "\t\tDelete Key:   " << page.kv_vector[m].first.second << "\tValue: "
                      << page.kv_vector[m].second << std::endl;
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }
  }
  return 1;
}

//This is used to sort the whole file based on sort key
bool sortbysortkey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b)
{
  return (a.first.first < b.first.first);
}
//This is used to sort the whole file based on delete key
bool sortbydeletekey(const pair<pair<long, long>, string> &a, const pair<pair<long, long>, string> &b)
{
  return (a.first.second < b.first.second);
}

int generateMetaData(SSTFile *file, DeleteTile &deletetile, Page &page, long sort_key_to_insert, long delete_key_to_insert)
{
  if (file->min_sort_key < 0 || sort_key_to_insert < file->min_sort_key)
  {
    file->min_sort_key = sort_key_to_insert;
  }

  if (file->max_sort_key < 0 || sort_key_to_insert > file->max_sort_key)
  {
    file->max_sort_key = sort_key_to_insert;
  }

  if (file->min_delete_key < 0 || delete_key_to_insert < file->min_delete_key)
  {
    file->min_delete_key = delete_key_to_insert;
  }

  if (file->max_delete_key < 0 || delete_key_to_insert > file->max_delete_key)
  {
    file->max_delete_key = delete_key_to_insert;
  }

  if (deletetile.min_sort_key < 0 || sort_key_to_insert < deletetile.min_sort_key)
  {
    deletetile.min_sort_key = sort_key_to_insert;
  }

  if (deletetile.max_sort_key < 0 || sort_key_to_insert > deletetile.max_sort_key)
  {
    deletetile.max_sort_key = sort_key_to_insert;
  }

  if (deletetile.min_delete_key < 0 || delete_key_to_insert < deletetile.min_delete_key)
  {
    deletetile.min_delete_key = delete_key_to_insert;
  }

  if (deletetile.max_delete_key < 0 || delete_key_to_insert > deletetile.max_delete_key)
  {
    deletetile.max_delete_key = delete_key_to_insert;
  }

  if (page.min_sort_key < 0 || sort_key_to_insert < page.min_sort_key)
  {
    page.min_sort_key = sort_key_to_insert;
  }

  if (page.max_sort_key < 0 || sort_key_to_insert > page.max_sort_key)
  {
    page.max_sort_key = sort_key_to_insert;
  }
  return 1;
}