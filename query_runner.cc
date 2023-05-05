/*
 *  Created on: Jan 28, 2020
 *  Author: Papon
 */

#include <iostream>
#include <cmath>
#include <sys/time.h>
#include <vector>
#include <cstdlib>
#include <algorithm>
#include <iomanip>
#include <fstream>
#include <random>

#include "emu_environment.h"
#include "query_runner.h"
#include "tree_builder.h"
#include "workload_generator.h"
#include "workload_executor.h"


using namespace std;
using namespace tree_builder;
using namespace workload_exec;

int Query::complete_delete_count = 0;
int Query::not_possible_delete_count = 0;
int Query::partial_delete_count = 0;

int Query::range_occurances = 0;
int Query::secondary_range_occurances = 0;

long Query::sum_page_id = 0;
long Query::found_count = 0;
long Query::not_found_count = 0;

std::default_random_engine generator(1);
uint32_t counter = 0;

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

void Query::checkDeleteCount (int deletekey)
{
  Query::complete_delete_count = 0;
  Query::not_possible_delete_count = 0;
  Query::partial_delete_count = 0;

  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      for (int k = 0; k < moving_head->tile_vector.size(); k++)
      {
        DeleteTile delete_tile = moving_head->tile_vector[k];
        for (int l = 0; l < delete_tile.page_vector.size(); l++)
        {
          Page page = delete_tile.page_vector[l];
          if (page.max_delete_key > 0)
          {
            if (page.max_delete_key < deletekey) {
              complete_delete_count++;
            }
            else if (page.min_delete_key > deletekey) {
              not_possible_delete_count++;
            }
            else {
              partial_delete_count++;
            }
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }

  }
  // std::cout << "(Delete Query)" << std::endl;
  // std::cout << "Compelte Possible Delete Count : " << complete_delete_count << std::endl;
  // std::cout << "Partial Possible Delete Count : " << partial_delete_count << std::endl;
  // std::cout << "Impossible Delete Count : " << not_possible_delete_count << std::endl;
  // std::cout << std::endl;
}

void Query::delete_query_experiment()
{
  EmuEnv* _env = EmuEnv::getInstance();
  // int selectivity[3] = {7, 30, 60};
  double selectivity[20] = {100000, 50000, 10000, 7500, 5000, 2500, 1000, 750, 500, 250};
  double increment = 0.01;
  int j = 10;
  // for (float i = 0.005 ; i < 1; i+= 0.005)
  // {
  //   selectivity[j] = (100/i);
  //   j++;
  // }
  for (float i = 1 ; i <= 10; i++)
  {
    selectivity[j] = (100/i);
    j++;
  }
  int delete_key1;

  fstream fout1;
  fout1.open("out_delete_srq.csv", ios::out | ios::app);
  fout1 << "SRQ Count" << ", " << "Fraction" << "," << "Delete Key" << "," << "Full Drop" << "," << "Partial Drop" << "," << "Impossible Drop" << "\n";

  for (int i = 0 ; i < 20; i++)
  {
    delete_key1 = _env->num_inserts/selectivity[i];
    Query::checkDeleteCount(delete_key1);
    fout1 << _env->srq_count << "," << "1/"+ to_string(selectivity[i])  << "," << delete_key1 << "," << Query::complete_delete_count << "," << Query::partial_delete_count 
      << "," << Query::not_possible_delete_count << endl;
  }

  fout1.close();
}

// QueryDrivenCompactionSelectivity sets the percentage of range query participating in the query driven compaction
// It always takes the center part of the range query
// e.g. a range query has range 0 to 1000 and QueryDrivenCompactionSelectivity 0.5, then range 250 to 750 will be used for query driven compaction
void Query::range_query_compaction_experiment(float selectivity, string file, int insertion, double QueryDrivenCompactionSelectivity)
{
  EmuEnv* _env = EmuEnv::getInstance();
  int range_iterval_1, range_query_start_1, range_query_end_1;

  fstream fout2;
  fout2.open(file, ios::out | ios::app);
  fout2.seekp(0, std::ios::end);
  bool is_empty = (fout2.tellp() == 0);
  if (is_empty) {
    fout2 << "SRQ Count" << ", " << "Selectivity" << "," << "Range Start" << "," << "Range End" << "," << "Occurrences" << "," << "write file count, insert time, QueryDrivenCompactionSelectivity" << "\n";
  }
  if (_env->correlation == 0)
  {
    range_iterval_1 = WorkloadGenerator::KEY_DOMAIN_SIZE * selectivity / 100;
    std::uniform_int_distribution<int> distribution(0, WorkloadGenerator::KEY_DOMAIN_SIZE-range_iterval_1-1);
    range_query_start_1 = distribution(generator);
    range_query_end_1 = range_query_start_1 + range_iterval_1;
  }
  else
  {
    range_iterval_1 = _env->num_inserts * selectivity / 100;
    range_query_start_1 = _env->num_inserts / 2 - range_iterval_1 / 2;
    range_query_end_1 = _env->num_inserts / 2 + range_iterval_1 / 2;
  }
  int write_file_count = Query::rangeQuery(range_query_start_1, range_query_end_1, QueryDrivenCompactionSelectivity);
  fout2 << _env->srq_count << "," << selectivity << "%" << "," << range_query_start_1 << "," << range_query_end_1 << "," << Query::range_occurances << "," << write_file_count << "," << insertion << ',' << QueryDrivenCompactionSelectivity << endl;
  
  fout2.close();

}

void Query::vanilla_range_query (int lowerlimit, int upperlimit) {

  range_occurances = 0;

  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      if (moving_head->min_sort_key > upperlimit)
        break;
      if (moving_head->max_sort_key < lowerlimit ) {
        moving_head = moving_head->next_file_ptr;
        continue;
      } 
      else {
        for (int k = 0; k < moving_head->tile_vector.size(); k++)
        {
          DeleteTile delete_tile = moving_head->tile_vector[k];
          if (delete_tile.min_sort_key > upperlimit || delete_tile.max_sort_key < lowerlimit) {
            continue;
          }
          else {
            for (int l = 0; l < delete_tile.page_vector.size(); l++)
            {
              Page page = delete_tile.page_vector[l];
              if (page.min_sort_key > 0)
              {
                if (page.min_sort_key > upperlimit || page.max_sort_key < lowerlimit) {
                continue;
                }
              }
            }
          }
        }
        range_occurances++;
      }
      moving_head = moving_head->next_file_ptr;
    }
  }
  // std::cout << "(Range Query)" << std::endl;
  // std::cout << "Pages traversed : " << range_occurances << std::endl << std::endl;
}

void Query::range_query_experiment(float selectivity)
{
  EmuEnv* _env = EmuEnv::getInstance();
  int range_iterval_1, range_query_start_1, range_query_end_1;

  fstream fout2;

  fout2.open("no_compaction_sequential.csv", ios::out | ios::app);
  bool is_empty = (fout2.tellp() == 0);
  if (is_empty) {
    fout2 << "SRQ Count" << ", " << "Selectivity" << "," << "Range Start" << "," << "Range End" << "," << "Occurrences" << "\n";
  }

  if (_env->correlation == 0)
  {
    range_iterval_1 = WorkloadGenerator::KEY_DOMAIN_SIZE * selectivity / 100;
    std::uniform_int_distribution<int> distribution(0, WorkloadGenerator::KEY_DOMAIN_SIZE-range_iterval_1-1);
    range_query_start_1 = distribution(generator);
    range_query_end_1 = range_query_start_1 + range_iterval_1;
  }
  else
  {
    range_iterval_1 = _env->num_inserts * selectivity / 100;
    range_query_start_1 = _env->num_inserts / 2 - range_iterval_1 / 2;
    range_query_end_1 = _env->num_inserts / 2 + range_iterval_1 / 2;
  }
  Query::vanilla_range_query(range_query_start_1, range_query_end_1);
  fout2 << _env->srq_count << "," << selectivity << "%" << "," << range_query_start_1 << "," << range_query_end_1 << "," << Query::range_occurances << endl;
  
  fout2.close();
}


void Query::point_query_experiment(float selectivity, double QueryDrivenCompactionSelectivity, int insert_time)
{
  EmuEnv* _env = EmuEnv::getInstance();
  fstream fout2;

  fout2.open("pq_compaction_non_sequential.csv", ios::out | ios::app);
  bool is_empty = (fout2.tellp() == 0);
  if (is_empty) {
    fout2 << "Selectivity, QueryDrivenCompactionSelectivity, insert time, read_file_count" << "\n";
  }

  for (int i = 0; i < 500; i++) {
    unsigned long long randomKey = rand() %  WorkloadGenerator::KEY_DOMAIN_SIZE;
    //std::cout << "Generated Random Key" << randomKey << std::endl;
    int read_file_count = Query::pointQuery(randomKey);
    fout2 << selectivity << "," << QueryDrivenCompactionSelectivity << "," << insert_time << "," << read_file_count << "\n";
  }
  
  fout2.close();

}

// this function runs a range query based on lower and upper bounds and QueryDrivenCompactionSelectivity
// QueryDrivenCompactionSelectivity sets the percentage of range query participating in the query driven compaction
// It always takes the center part of the range query
// e.g. a range query has range 0 to 1000 and QueryDrivenCompactionSelectivity 0.5, then range 250 to 750 will be used for query driven compaction
int Query::rangeQuery (int lowerlimit, int upperlimit, double QueryDrivenCompactionSelectivity) {

    // Initialize variables
    range_occurances = 0;
    vector < pair < pair < long, long >, string > > vector_to_compact;
    int range = upperlimit - lowerlimit;

    // Calculate the lower and upper bounds for the center of the range according to QueryDrivenCompactionSelectivity
    int centerLowerBound = lowerlimit + static_cast<int>(range * (1.0 - QueryDrivenCompactionSelectivity) / 2);
    int centerUpperBound = centerLowerBound + static_cast<int>(range * QueryDrivenCompactionSelectivity);

    // Loop through all levels of the database
    for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++) {
        // Get the head of the SST file for this level
        SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
        SSTFile *moving_head = level_i_head;

        // Loop through all SST files in this level
        while (moving_head) {
            // Check if this SST file is beyond the range we are interested in
            if (moving_head->min_sort_key > upperlimit) {
                break;
            }
            // Check if this SST file is completely before the range we are interested in
            if (moving_head->max_sort_key < lowerlimit ) {
                moving_head = moving_head->next_file_ptr;
                continue;
            }
            else {
                // Loop through all tiles in this SST file
                for (const auto& delete_tile : moving_head->tile_vector) {
                    // Check if this tile is beyond the range we are interested in
                    if (delete_tile.min_sort_key > upperlimit || delete_tile.max_sort_key < lowerlimit) {
                        continue;
                    }
                    else {
                        // Loop through all pages in this tile
                        for (const auto& page : delete_tile.page_vector) {
                            // Check if this page is beyond the range we are interested in
                            if (page.min_sort_key > 0) {
                                if (page.min_sort_key > upperlimit || page.max_sort_key < lowerlimit) {
                                    continue;
                                }
                                else {
                                    // Loop through all key-value pairs in this page
                                    for (auto & m : page.kv_vector) {
                                        // Check if the key is within range query compaction range
                                        if (m.first.first >= centerLowerBound and m.first.first <= centerUpperBound and i >= 2 and i < DiskMetaFile::getTotalLevelCount()) {
                                            // Check if we have already seen this key in a previous SST file
                                            int match = 0;
                                            for(auto & p : vector_to_compact) {
                                                if (p.first.first == m.first.first) {
                                                    match++;
                                                    break;
                                                }
                                            }
                                            // If we have not seen this key before, add it to the vector to be compacted
                                            if (match == 0)
                                                vector_to_compact.push_back(m);
                                            else
                                                match = 0;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // Increment the number of times we have encountered the range in this level
                range_occurances++;
            }
            // Move to the next SST file in this level
            moving_head = moving_head->next_file_ptr;
        }
    }

    // Only do query driven compaction if there are at least two levels in the LSM Tree
    if (DiskMetaFile::getTotalLevelCount() < 2) {
        std::cout << "There is only 1 level in LSM Tree. No Query Driven Compaction will perform." << std::endl;
        return 0;
    }

    // Check if there are any key-value pairs to compact
    if (!vector_to_compact.empty()) {
        // Perform query-driven compaction on the key-value pairs we found and sort the resulting vector by sort key
        cout << "range query completes. Starting query driven compaction. vector size:"<< vector_to_compact.size() << endl;
        std::sort(vector_to_compact.begin(), vector_to_compact.end(), Utility::sortbysortkey);
        int write_file_count = Utility::QueryDrivenCompaction(vector_to_compact);
        return write_file_count;
    }
    else {
        cout << "Nothing to compact in range query " << lowerlimit << " to " << upperlimit << endl;
        return 0;
    }

}

int Query::pointQuery (int key)
{
    int read_file_count = 0;
  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      if (moving_head->min_sort_key > key)
        break;
      if (moving_head->min_sort_key <= key && key <= moving_head->max_sort_key) {
          read_file_count += 1;
        for (int k = 0; k < moving_head->tile_vector.size(); k++)
        {
          DeleteTile delete_tile = moving_head->tile_vector[k];
          if (delete_tile.min_sort_key > key)
            break;
          if (delete_tile.min_sort_key <= key && key <= delete_tile.max_sort_key) {
            for (int l = 0; l < delete_tile.page_vector.size(); l++)
            {
              Page page = delete_tile.page_vector[l];
              if (page.min_sort_key <= key && key <= page.max_sort_key) {
                for (int m = 0; m < page.kv_vector.size(); m++) {
                  if (key == page.kv_vector[m].first.first) {
                    return read_file_count;
                  }
                }
              }
            }
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }
  }
  return read_file_count;
}
