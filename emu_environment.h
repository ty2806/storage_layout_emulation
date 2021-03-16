/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */

#ifndef EMU_ENVIRONMENT_H_
#define EMU_ENVIRONMENT_H_
 

#include <iostream>

using namespace std;


class EmuEnv
{
private:
  EmuEnv(); 
  static EmuEnv *instance;

public:
  static EmuEnv* getInstance();

  int size_ratio; // T

  int buffer_size_in_pages; // P
  int entries_per_page; // B
  int entry_size; // E
  long buffer_size; // M = P*B*E ; in Bytes

  int delete_tile_size_in_pages; // h
  long file_size; // B/s ; in Bytes
  int correlation; // c

  float buffer_flush_threshold;
  float disk_run_flush_threshold;

  long long num_inserts;

  int verbosity;
  int lethe_new;  // 0 for classical lethe, 1 for new lethe
  int variable_delete_tile_size_in_pages[30] = {-1, 1, 2, 3, 4, 5};

  int getDeleteTileSize(int level);

};

#endif /*EMU_ENVIRONMENT_H_*/

