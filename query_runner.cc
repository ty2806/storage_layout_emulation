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

#include "emu_environment.h"
#include "query_runner.h"
#include "tree_builder.h"
#include "workload_generator.h"


using namespace std;
using namespace tree_builder;

int Query::complete_delete_count = 0;
int Query::not_possible_delete_count = 0;
int Query::partial_delete_count = 0;

int Query::range_occurances = 0;
int Query::secondary_range_occurances = 0;

long Query::sum_page_id = 0;
long Query::found_count = 0;
long Query::not_found_count = 0;

void initData ()
{
  Query::complete_delete_count = 0;
  Query::not_possible_delete_count = 0;
  Query::partial_delete_count = 0;
  Query::range_occurances = 0;
  Query::secondary_range_occurances = 0;
  Query::sum_page_id = 0;
  Query::found_count = 0;
  Query::not_found_count = 0;
}

int Query::checkDeleteCount (int deletekey)
{
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
      moving_head = moving_head->next_file_ptr;
    }

  }
  // std::cout << "(Delete Query)" << std::endl;
  // std::cout << "Compelte Possible Delete Count : " << complete_delete_count << std::endl;
  // std::cout << "Partial Possible Delete Count : " << partial_delete_count << std::endl;
  // std::cout << "Impossible Delete Count : " << not_possible_delete_count << std::endl;
  // std::cout << std::endl;
}



int Query::rangeQuery (int lowerlimit, int upperlimit) {

  range_occurances = 0;

  for (int i = 1; i <= DiskMetaFile::getTotalLevelCount(); i++)
  {
    SSTFile *level_i_head = DiskMetaFile::getSSTFileHead(i);
    SSTFile *moving_head = level_i_head;
    while (moving_head)
    {
      if (moving_head->min_sort_key > upperlimit || moving_head->max_sort_key < lowerlimit ) {
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
              if (page.min_sort_key > upperlimit || page.max_sort_key < lowerlimit) {
                continue;
              }
              else {
                range_occurances++;
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
}

int Query::secondaryRangeQuery (int lowerlimit, int upperlimit) {

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
      if (moving_head->min_sort_key <= key && key <= moving_head->max_sort_key) {
        for (int k = 0; k < moving_head->tile_vector.size(); k++)
        {
          DeleteTile delete_tile = moving_head->tile_vector[k];
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

int Query::pointQueryRunner (int iterations)
{
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
  }
    // std::cout << "(Point Query)" << std::endl;
    // std::cout << "Total sum of found pageIDs : " <<  sumPageId << std::endl;
    // std::cout << "Total number of found pageIDs : " <<  foundCount << std::endl;
    // std::cout << "Total number of found average pageIDs : " <<  sumPageId/(foundCount * 1.0) << std::endl;
    // std::cout << "Total number of not found pages : " <<  notFoundCount << std::endl << std::endl;
}

