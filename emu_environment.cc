#include <iostream>
#include <cmath>
#include <sys/time.h>
#include "emu_environment.h"

 
/*Set up the singleton object with the experiment wide options*/
EmuEnv* EmuEnv::instance = 0;


EmuEnv::EmuEnv() 
{
  size_ratio = 2;

  buffer_size_in_pages = 128;
  entries_per_page = 128;
  entry_size = 128; // in Bytes 
  buffer_size = buffer_size_in_pages * entries_per_page * entry_size; // M = P*B*E = 128 * 128 * 128 B = 2 MB 

  delete_tile_size_in_pages = 2; // h
  file_size = buffer_size; // file_size = M/s = 2 MB / 8 = 256 KB

  buffer_flush_threshold = disk_run_flush_threshold = 1;

  num_inserts = 0;

  verbosity=0;
  
}

EmuEnv* EmuEnv::getInstance()
{
  if (instance == 0)
    instance = new EmuEnv();

  return instance;
}
