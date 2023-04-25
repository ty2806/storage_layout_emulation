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

void Query::range_query_compaction_experiment(float selectivity, string file)
{
  EmuEnv* _env = EmuEnv::getInstance();
  int range_iterval_1, range_query_start_1, range_query_end_1;
  double QueryDrivenCompactionSelectivity = 1;

  fstream fout2;
  fout2.open(file, ios::out | ios::app);
  fout2.seekp(0, std::ios::end);
  bool is_empty = (fout2.tellp() == 0);
  if (is_empty) {
    fout2 << "SRQ Count" << ", " << "Selectivity" << "," << "Range Start" << "," << "Range End" << "," << "Occurrences" << "," << "write file count" << "\n";
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
  fout2 << _env->srq_count << "," << selectivity << "%" << "," << range_query_start_1 << "," << range_query_end_1 << "," << Query::range_occurances << "," << write_file_count << endl;
  
  fout2.close();

}

void Query::range_query_experiment()
{
  EmuEnv* _env = EmuEnv::getInstance();
  float selectivity[5] = {0.1, 1, 10, 20, 100};
  int range_iterval_1, range_query_start_1, range_query_end_1;
  double QueryDrivenCompactionSelectivity = 1;

  fstream fout2;
  fout2.open("out_range_srq.csv", ios::out | ios::app);
  fout2 << "SRQ Count" << ", " << "Selectivity" << "," << "Range Start" << "," << "Range End" << "," << "Occurrences" << "," << "write file count" << "\n";

  for (int i = 0; i < 5 ; i++ )
  {
    if (_env->correlation == 0)
    {
      range_iterval_1 = WorkloadGenerator::KEY_DOMAIN_SIZE * selectivity[i] / 100;
      range_query_start_1 = WorkloadGenerator::KEY_DOMAIN_SIZE / 2 - range_iterval_1 / 2;
      range_query_end_1 = WorkloadGenerator::KEY_DOMAIN_SIZE / 2 + range_iterval_1 / 2;
    }
    else
    {
      range_iterval_1 = _env->num_inserts * selectivity[i] / 100;
      range_query_start_1 = _env->num_inserts / 2 - range_iterval_1 / 2;
      range_query_end_1 = _env->num_inserts / 2 + range_iterval_1 / 2;
    }
    int write_file_count = Query::rangeQuery(range_query_start_1, range_query_end_1, QueryDrivenCompactionSelectivity);
    fout2 << _env->srq_count << "," << selectivity[i] << "%" << "," << range_query_start_1 << "," << range_query_end_1 << "," << Query::range_occurances << "," << write_file_count << endl;
  }
  fout2.close();
}

void Query::sec_range_query_experiment()
{
  EmuEnv* _env = EmuEnv::getInstance();
  int range_iterval_1, range_query_start_1, range_query_end_1;
  float selectivity[35] = {0.0001, 0.0005, 0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.1, 1, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};

  fstream fout3;
  fout3.open("out_sec_range_srq.csv", ios::out | ios::app);
  fout3 << "SRQ Count" << ", " << "Selectivity" << "," <<  "Sec Range Start" << "," << "Sec Range End" << "," << "Occurrences" << "\n";

  for (int i = 0; i < 35 ; i++ )
  {
    range_iterval_1 = _env->num_inserts * selectivity[i]/100;  
    range_query_start_1 = _env->num_inserts/2 - range_iterval_1/2;
    range_query_end_1 = _env->num_inserts/2 + range_iterval_1/2;
    Query::secondaryRangeQuery(range_query_start_1, range_query_end_1);
    fout3 << _env->srq_count << "," << selectivity[i] << "%" << "," << range_query_start_1 << "," << range_query_end_1 << "," << Query::secondary_range_occurances << endl;
  }

  fout3.close();
}

void Query::point_query_experiment()
{
  EmuEnv* _env = EmuEnv::getInstance();
  int point_query_iteration1 = 100000;
  fstream fout4;
  fout4.open("out_point.csv", ios::out | ios::app);

  Query::pointQueryRunner(point_query_iteration1);
  fout4 << _env->delete_tile_size_in_pages << "," << point_query_iteration1 << "," << Query::sum_page_id << "," << Query::sum_page_id/(Query::found_count * 1.0) << "," << Query::found_count << "," << Query::not_found_count << endl;
  fout4.close();

}

void Query::new_point_query_experiment ()
{
  EmuEnv* _env = EmuEnv::getInstance();
  fstream fout4;
  fout4.open("out_point_nonempty_srq.csv", ios::out | ios::app);
  fout4 << "SRQ Count" << ", " << "Iterations" << "," <<  "Sum_Page_Id" << "," << "Avg_Page_Id" << "," << "Found" << "," << "Not Found" << "\n";

  counter = 0;
  sum_page_id = 0;
  found_count = 0;
  not_found_count = 0;
  for (int i = 0; i < _env->pq_count ; i++) {
    unsigned long long randomKey = rand() % _env->num_inserts;
    //std::cout << "Generated Random Key" << randomKey << std::endl;
    randomKey = WorkloadGenerator::inserted_keys[randomKey];
    int pageId = Query::pointQuery(randomKey);
    if(pageId < 0) 
    {
      not_found_count++;
    }
    else 
    {
      //cout << pageId << endl;
      sum_page_id += pageId;
      found_count++;
    }
    counter++;

    if(!(counter % (_env->pq_count/100))){
      showProgress(_env->pq_count, counter);
    }
  }
  fout4 << _env->srq_count << "," << _env->pq_count << "," << Query::sum_page_id << "," << Query::sum_page_id/(Query::found_count * 1.0) << "," << Query::found_count << "," << Query::not_found_count << endl;
  fout4.close();
}

int Query::rangeQuery (int lowerlimit, int upperlimit, double QueryDrivenCompactionSelectivity) {

  range_occurances = 0;
  vector < pair < pair < long, long >, string > > vector_to_compact;
  int range = upperlimit - lowerlimit;
  int centerLowerBound = lowerlimit + static_cast<int>(range * (1.0 - QueryDrivenCompactionSelectivity) / 2);
  int centerUpperBound = centerLowerBound + static_cast<int>(range * QueryDrivenCompactionSelectivity);
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
        for (const auto& delete_tile : moving_head->tile_vector)
        {
          if (delete_tile.min_sort_key > upperlimit || delete_tile.max_sort_key < lowerlimit) {
            continue;
          }
          else {
            for (const auto& page : delete_tile.page_vector)
            {
              if (page.min_sort_key > 0)
              {
                if (page.min_sort_key > upperlimit || page.max_sort_key < lowerlimit) {
                continue;
                }
                else {
                    for (auto & m : page.kv_vector)
                    {
                        if (m.first.first >= centerLowerBound and m.first.first <= centerUpperBound) {
                            if (i >= 2 and i < DiskMetaFile::getTotalLevelCount()) {
                                vector_middle.push_back(m);
                            }
                            for(auto & p : vector_to_compact) {
                                if (p.first.first == m.first.first) {
                                    match++;
                                    break;
                                }
                            }
                            if (match == 0)
                                vector_to_compact.push_back(m);
                            else
                                match = 0;
                        }
                    }
                  range_occurances++;
                }
              }
            }
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }
  }
    // std::cout << "(Range Query)" << std::endl;
    // std::cout << "Pages traversed : " << range_occurances << std::endl << std::endl;
  if (!vector_to_compact.empty() and !vector_middle.empty()) {
      cout << "range query completes. Starting query driven compaction. vector size:"<< vector_to_compact.size() << endl;
      std::sort(vector_to_compact.begin(), vector_to_compact.end(), Utility::sortbysortkey);
      int write_file_count = Utility::QueryDrivenCompaction(vector_to_compact);
      return write_file_count;
  }
  else {
      cout << "Nothing to find in range query " << lowerlimit << " to " << upperlimit << endl;
      return 0;
  }

}

void Query::secondaryRangeQuery (int lowerlimit, int upperlimit) {

  secondary_range_occurances = 0;

  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      if (moving_head->min_delete_key > upperlimit || moving_head->max_delete_key < lowerlimit ) {
        moving_head = moving_head->next_file_ptr;
        continue;
      } 
      else {
        for (int k = 0; k < moving_head->tile_vector.size(); k++)
        {
          DeleteTile delete_tile = moving_head->tile_vector[k];
          if (delete_tile.min_delete_key > upperlimit || delete_tile.max_delete_key < lowerlimit) {
            continue;
          }
          else {
            for (int l = 0; l < delete_tile.page_vector.size(); l++)
            {
              Page page = delete_tile.page_vector[l];
              if (page.min_delete_key > 0)
              {
                if (page.min_delete_key > upperlimit || page.max_delete_key < lowerlimit) {
                  continue;
                }
                else {
                  secondary_range_occurances++;
                }
              }
            }
          }
        }
      }
      moving_head = moving_head->next_file_ptr;
    }
  }
  // std::cout << "(Secondary Range Query)" << std::endl;
  // std::cout << "Pages traversed : " << secondary_range_occurances << std::endl << std::endl;
}

int Query::pointQuery (int key)
{
  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      if (moving_head->min_sort_key > key)
        break;
      if (moving_head->min_sort_key <= key && key <= moving_head->max_sort_key) {
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
                    return l + 1;
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
  return -1;
}

void Query::pointQueryRunner (int iterations)
{
  counter = 0;
  sum_page_id = 0;
  found_count = 0;
  not_found_count = 0;
  for (int i = 0; i < iterations ; i++) {
    unsigned long long randomKey = rand() %  WorkloadGenerator::KEY_DOMAIN_SIZE;
    //std::cout << "Generated Random Key" << randomKey << std::endl;
    int pageId = Query::pointQuery(randomKey);
    if(pageId < 0) 
    {
      not_found_count++;
    }
    else 
    {
      //cout << pageId << endl;
      sum_page_id += pageId;
      found_count++;
    }
    counter++;

    if(!(counter % (iterations/100))){
      // std::cout << "baal " << iterations << " " << counter;
      showProgress(iterations, counter);
    }
  }
    // std::cout << "(Point Query)" << std::endl;
    // std::cout << "Total sum of found pageIDs : " <<  sumPageId << std::endl;
    // std::cout << "Total number of found pageIDs : " <<  foundCount << std::endl;
    // std::cout << "Total number of found average pageIDs : " <<  sumPageId/(foundCount * 1.0) << std::endl;
    // std::cout << "Total number of not found pages : " <<  notFoundCount << std::endl << std::endl;
}

