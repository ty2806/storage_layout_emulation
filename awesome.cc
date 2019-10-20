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
int generateMetaData(SSTFile *file, DeleteTile &deletetile, Page &page, long sort_key_to_insert, long delete_key_to_insert);

// CLASS : MemoryBuffer

MemoryBuffer::MemoryBuffer(EmuEnv *_env)
{
  max_buffer_size = _env->buffer_size;
  buffer_flush_threshold = _env->buffer_flush_threshold;
  verbosity = _env->verbosity;

  for (int i = 1; i < 32; ++i)
  {
    DiskMetaFile::disk_run_flush_threshold[i] = _env->disk_run_flush_threshold;
    DiskMetaFile::level_max_size[i] = _env->buffer_size * pow(_env->size_ratio, i);
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
{
  int entries_per_file = MemoryBuffer::current_buffer_entry_count;
  sortAndWrite(MemoryBuffer::buffer, level_to_flush_in);
  MemoryBuffer::buffer_flush_count++;
  return 1;
}

//Class DiskMetaFile

int DiskMetaFile::getLevelFileCount(int level)
{
  SSTFile *level_head = DiskMetaFile::getSSTFileHead(level);
  SSTFile *moving_head = level_head;
  long level_size_in_files = 0;
  while (moving_head)
  {
    level_size_in_files++;
    moving_head = moving_head->next_file_ptr;
  }
  return level_size_in_files;
}

long DiskMetaFile::getLevelEntryCount(int level)
{
  SSTFile *level_head = DiskMetaFile::getSSTFileHead(level);
  SSTFile *moving_head = level_head;
  long level_size_in_entries = 0;
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
          level_size_in_entries++;
        }
      }
    }
    moving_head = moving_head->next_file_ptr;
  }
  return level_size_in_entries;
}

int DiskMetaFile::getTotalLevelCount()
{
  int level_count = 0;
  for (int i = 1; i <= 32; i++)
  {
    SSTFile *level_head = DiskMetaFile::getSSTFileHead(i);
    if (level_head)
    {
      level_count++;
    }
    else
    {
      return level_count;
    }
  }
}

int DiskMetaFile::checkAndAdjustLevelSaturation(int level)
{
  int entry_count_in_level = getLevelEntryCount(level);
  EmuEnv *_env = EmuEnv::getInstance();
  int max_entry_count_in_level = DiskMetaFile::level_max_size[level] / _env->entry_size;
  if (entry_count_in_level >= max_entry_count_in_level)
  {
    if (MemoryBuffer::verbosity == 2)
      std::cout << "Saturation Reached......" << endl;
    //Push the first file into the next level
    //Setting current level's head to the second file
    SSTFile *level_head = DiskMetaFile::getSSTFileHead(level);
    DiskMetaFile::setSSTFileHead(level_head->next_file_ptr, level);

    //Converting to vector (only the first file)
    vector<pair<pair<long, long>, string>> vector_to_compact;

    for (int k = 0; k < level_head->tile_vector.size(); k++)
    {
      DeleteTile delete_tile = level_head->tile_vector[k];
      for (int l = 0; l < delete_tile.page_vector.size(); l++)
      {
        Page page = delete_tile.page_vector[l];
        for (int m = 0; m < page.kv_vector.size(); m++)
        {
          vector_to_compact.push_back(page.kv_vector[m]);
        }
      }
    }
    //Sending it to next level.
    sortAndWrite(vector_to_compact, level + 1);
  }
  else
  {
    return 1;
  }
}

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

int PopulateDeleteTile(SSTFile *file, vector<pair<pair<long, long>, string>> vector_to_populate_tile, int deletetileid, int level_to_flush_in)
{
  if (MemoryBuffer::verbosity == 2)
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

    if (MemoryBuffer::verbosity == 2)
    {
      std::cout << "\npopulating pages :: vector length = " << vector_to_populate_tile.size() << std::endl;
      std::cout << "\nEntries per page = " << _env->entries_per_page << std::endl;
    }

    for (int j = 0; j < _env->entries_per_page; ++j)
    {
      long sort_key_to_insert = vector_to_populate_page[j].first.first;
      long delete_key_to_insert = vector_to_populate_page[j].first.second;

      int status = generateMetaData(file, file->tile_vector[deletetileid],
                                    file->tile_vector[deletetileid].page_vector[i], sort_key_to_insert, delete_key_to_insert);

      file->tile_vector[deletetileid].page_vector[i].kv_vector.push_back(make_pair(make_pair(
                                                                                       vector_to_populate_page[j].first.first, vector_to_populate_page[j].first.second),
                                                                                   vector_to_populate_page[j].second));

      if (MemoryBuffer::verbosity == 2)
        std::cout << vector_to_populate_page[j].first.first << " " << vector_to_populate_page[j].first.second << std::endl;
    }

    vector_to_populate_tile.erase(vector_to_populate_tile.begin(), vector_to_populate_tile.begin() + _env->entries_per_page);

    if (MemoryBuffer::verbosity == 2)
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

  if (MemoryBuffer::verbosity == 2)
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
    if (MemoryBuffer::verbosity == 2)
      std::cout << "populating delete tile ... \n";
    int status = PopulateDeleteTile(file, vector_to_populate_tile, i, level_to_flush_in);
    vector_to_populate_tile.clear();
  }
  return 1;
}

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

  return delete_tiles;
}

SSTFile *SSTFile::createNewSSTFile(int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();

  SSTFile *new_file = new SSTFile();
  new_file->max_sort_key = -1;
  new_file->max_delete_key = -1;
  new_file->min_sort_key = -1;
  new_file->min_delete_key = -1;

  int delete_tile_count_in_a_file = _env->buffer_size_in_pages / _env->delete_tile_size_in_pages;

  new_file->tile_vector = DeleteTile::createNewDeleteTiles(delete_tile_count_in_a_file);

  for (int i = 0; i < new_file->tile_vector.size(); ++i)
    new_file->tile_vector[i].page_vector = Page::createNewPages(_env->delete_tile_size_in_pages);

  new_file->file_level = level_to_flush_in;
  new_file->next_file_ptr = NULL;
  DiskMetaFile::global_level_file_counter[level_to_flush_in]++;
  new_file->file_id = "L" + std::to_string(level_to_flush_in) + "F" + std::to_string(DiskMetaFile::global_level_file_counter[level_to_flush_in]);
  //std::cout << "Creating new file !!" << std::endl;

  return new_file;
}

int compactAndFlush(vector<pair<pair<long, long>, string>> vector_to_compact, int level_to_flush_in)
{
  EmuEnv *_env = EmuEnv::getInstance();
  SSTFile *head_level_1 = DiskMetaFile::getSSTFileHead(level_to_flush_in);
  int entries_per_file = _env->entries_per_page * _env->buffer_size_in_pages;
  int file_count = vector_to_compact.size() / entries_per_file;
  if (MemoryBuffer::verbosity == 2)
    std::cout << "\nwriting " << file_count << " file(s)\n";
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
    if (MemoryBuffer::verbosity == 2)
      std::cout << "NULL" << std::endl;

    std::sort(vector_to_compact.begin(), vector_to_compact.end(), sortbysortkey);
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

    std::sort(vector_to_compact.begin(), vector_to_compact.end(), sortbysortkey);
    int file_count = vector_to_compact.size() / entries_per_file;

    compactAndFlush(vector_to_compact, level_to_flush_in);
  }
  int saturation = DiskMetaFile::checkAndAdjustLevelSaturation(level_to_flush_in);
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

int DiskMetaFile::getMetaStatistics()
{
  std::cout << "**************************** PRINTING META FILE STATISTICS ****************************" << std::endl;
  std::cout << "L\tfile_count\tentry_count\t" << std::endl;
  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    std::cout << i << "\t" << setfill(' ') << setw(8) << DiskMetaFile::getLevelFileCount(i) << "\t\t"
              << DiskMetaFile::getLevelEntryCount(i) << std::endl;
  }
  std::cout << "***************************************************************************************" << std::endl;
}

// int DiskMetaFile::getMetaStatistics()
// {
//   std::cout << "**************************** PRINTING META FILE STATISTICS ****************************" << std::endl;
//   std::cout << "L\tmin_key\t\tmax_key\t\tfile_count\tentry_count\tlevel_size" << std::endl;
//   for (int i = 1; i < 32 && DiskMetaFile::level_head[i]; ++i)
//   {
//     std::cout << i << "\t" << setfill(' ') << setw(8) << DiskMetaFile::level_min_key[i] << "\t"
//               << setfill(' ') << setw(8) << DiskMetaFile::level_max_key[i] << "\t\t" << DiskMetaFile::level_file_count[i] << "\t\t"
//               << DiskMetaFile::level_entry_count[i] << "\t\t" << DiskMetaFile::level_current_size[i] << std::endl;
//   }
//   std::cout << "***************************************************************************************" << std::endl;

//   // Printing COMPACTION statistics -- these stats are not set by setMetaStatistics()
//   int total_compactions = 0;
//   int total_compactions_through_sortmerge = 0;
//   int total_compactions_through_pointer_manipulation = 0;
//   int total_files_in_compaction = 0;
//   std::cout << "L\t#compactions\t#compactions_through_sortmerge\t#compactions_through_pointer_manipulation\t#files_in_compaction\t#bytes_in_compaction" << std::endl;
//   for (int i = 1; i < 32 && DiskMetaFile::level_head[i]; ++i)
//   {
//     total_compactions += DiskMetaFile::compaction_counter[i];
//     total_files_in_compaction += DiskMetaFile::compaction_file_counter[i];
//     total_compactions_through_sortmerge += DiskMetaFile::compaction_through_sortmerge_counter[i];
//     total_compactions_through_pointer_manipulation += DiskMetaFile::compaction_through_point_manipulation_counter[i];

//     std::cout << i + 1 << "\t" << setfill(' ') << setw(8) << DiskMetaFile::compaction_counter[i] << "\t\t"
//               << setfill(' ') << setw(8) << DiskMetaFile::compaction_through_sortmerge_counter[i] << "\t\t\t"
//               << setfill(' ') << setw(8) << DiskMetaFile::compaction_through_point_manipulation_counter[i] << "\t\t\t\t"
//               << setfill(' ') << setw(8) << DiskMetaFile::compaction_file_counter[i] << "\t\t\t"
//               << setfill(' ') << setw(12) << DiskMetaFile::compaction_file_counter[i] * 4 * 128 << std::endl;
//   }
//   std::cout << "Total"
//             << "\t" << setfill(' ') << setw(8) << total_compactions << "\t\t"
//             << setfill(' ') << setw(8) << total_compactions_through_sortmerge << "\t\t\t"
//             << setfill(' ') << setw(8) << total_compactions_through_pointer_manipulation << "\t\t\t\t"
//             << setfill(' ') << setw(8) << total_files_in_compaction << "\t\t\t"
//             << setfill(' ') << setw(12) << (long)total_files_in_compaction * 4 * 128 << std::endl;
//   return 1;
// }

int DiskMetaFile::printAllEntries()
{
  std::cout << "**************************** PRINTING ALL ENTRIES ****************************" << std::endl;
  EmuEnv *_env = EmuEnv::getInstance();
  std::cout << "Buffer size in pages (P): " << _env->buffer_size_in_pages << std::endl;
  std::cout << "Entries per pages (B): " << _env->entries_per_page << std::endl;
  std::cout << "Entry Size (E): " << _env->entry_size << std::endl;
  std::cout << "Buffer Size (PBE): " << _env->buffer_size << std::endl;
  std::cout << "Delete Tile Size in Pages (H): " << _env->delete_tile_size_in_pages << std::endl;
  std::cout << "Size Ratio: " << _env->size_ratio << std::endl;

  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;

    std::cout << "\033[1;31mLevel : " << i << "\033[0m" << std::endl;
    while (moving_head)
    {
      // std::cout << "File : " << j << " Min Sort Key : " << moving_head -> min_sort_key << " Max Sort Key : " << moving_head -> max_sort_key << std::endl;
      // std::cout << "File : " << j << " Min Delete Key : " << moving_head -> min_delete_key << " Max Delete Key : " << moving_head -> max_delete_key << std::endl;
      std::cout << "\033[1;34m\tFile : " << moving_head->file_id << " (Min_Sort_Key : " << moving_head -> min_sort_key 
      << ", Max_Sort_Key : " << moving_head -> max_sort_key << ", Min_Delete_Key : " << moving_head -> min_delete_key 
      << ", Max_Delete_Key : " << moving_head -> max_delete_key << ")" << "\033[0m" << std::endl;
      for (int k = 0; k < moving_head->tile_vector.size(); k++)
      {
        DeleteTile delete_tile = moving_head->tile_vector[k];
        std::cout << "\033[1;32m\t\tDelete Tile : " << k << " (Min_Sort_Key : " << delete_tile.min_sort_key 
        << ", Max_Sort_Key : " << delete_tile.max_sort_key << ", Min_Delete_Key : " << delete_tile.min_delete_key 
        << ", Max_Delete_Key : " << delete_tile.max_delete_key << ")" << "\033[0m" << std::endl;
        for (int l = 0; l < delete_tile.page_vector.size(); l++)
        {
          Page page = delete_tile.page_vector[l];
          std::cout << "\033[1;33m\t\t\tPage : " << l << " (Min_Sort_Key : " << page.min_sort_key 
          << ", Max_Sort_Key : " << page.max_sort_key << ")" << "\033[0m" << std::endl;

          for (int m = 0; m < page.kv_vector.size(); m++)
          {
            std::cout << "\t\t\t\tEntry : " << m << " (Sort_Key : " << page.kv_vector[m].first.first
            << ", Delete_Key : " << page.kv_vector[m].first.second << ")" << std::endl;
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }
  }
  std::cout << "***************************************************************************************" << std::endl;
  return 1;
}