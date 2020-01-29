/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep
 */
 
#include <sstream>
#include <iostream>
#include <cstdio>
#include <sys/time.h>
#include <cmath>
#include <unistd.h>
#include <assert.h>
#include <fstream>

#include "args.hxx"
#include "emu_environment.h"
#include "awesome.h"
#include "workload_generator.h"

using namespace std;
using namespace awesome;

/*
 * DECLARATIONS
*/

//long inserts(EmuEnv* _env);
int parse_arguments2(int argc, char *argvx[], EmuEnv* _env);
void printEmulationOutput(EmuEnv* _env);

//int run_workload(read, pread, rread, write, update, delete, skew, others);
int runWorkload(EmuEnv* _env);


int main(int argc, char *argvx[]) {

  // check emu_environment.h for the contents of EmuEnv and also the definitions of the singleton experimental environment 
  EmuEnv* _env = EmuEnv::getInstance();
  
  //parse the command line arguments
  if (parse_arguments2(argc, argvx, _env)){
    exit(1);
  }
  
  
  
  // Issuing INSERTS
  if (_env->num_inserts > 0) 
  {
    // if (_env->verbosity >= 1) 
      std::cerr << "Issuing inserts ... " << std::endl << std::flush; 
    
    WorkloadGenerator workload_generator;
    workload_generator.generateWorkload((long)_env->num_inserts, (long)_env->entry_size);

    //int s = run_workload(read, pread, rread, write, update, del, skew, others); 
    int s = runWorkload(_env); 
    
    DiskMetaFile::printAllEntries();
    MemoryBuffer::getCurrentBufferStatistics();
    DiskMetaFile::getMetaStatistics();
    std::cout << std::endl;
    DiskMetaFile::checkDeleteCount(700);
    //std::cout << "\n(Point Lookup) Found at : " << DiskMetaFile::pointQuery(540) << std::endl << std::endl;
    DiskMetaFile::rangeQuery(2000,5000);
    DiskMetaFile::secondaryRangeQuery(200,500);

    //srand(time(0));
    long sumPageId = 0;
    long foundCount = 0;
    long notFoundCount = 0;

    for (int i = 0; i < 100000 ; i++) {
      unsigned long long randomKey = rand() %  100000;
      //std::cout << "Generated Random Key" << randomKey << std::endl;
      int pageId = DiskMetaFile::pointQuery(randomKey);
      if(pageId < 0) {
        notFoundCount++;
      }
      else {
        //cout << pageId << endl;
        sumPageId += pageId;
        foundCount++;
      }
    }
    std::cout << "Total sum of found pageIDs : " <<  sumPageId << std::endl;
    std::cout << "Total number of found pageIDs : " <<  foundCount << std::endl;
    std::cout << "Total number of found average pageIDs : " <<  sumPageId/(foundCount * 1.0) << std::endl;
    std::cout << "Total number of not found pages : " <<  notFoundCount << std::endl;
  
    //WorkloadExecutor::getWorkloadStatictics(_env);

    //assert(_env->num_inserts == inserted); 
  }

  printEmulationOutput(_env);

  return 0;
}


/*
 * DEFINITIONS 
 * 
 */

int runWorkload(EmuEnv* _env) {

  MemoryBuffer* buffer_instance = MemoryBuffer::getBufferInstance(_env);


  awesome::WorkloadExecutor workload_executer;
  // reading from file
  ifstream workload_file;
  workload_file.open("workload.txt");
  assert(workload_file);
  while(!workload_file.eof()) {
    char instruction;
    long sortkey;
    long deletekey;
    string value;
    workload_file >> instruction >> sortkey >> deletekey >> value;
    switch (instruction)
    {
    case 'I':
      //std::cout << instruction << " " << sortkey << " " << deletekey << " " << value << std::endl;
      workload_executer.insert(sortkey, deletekey, value);

      break;
    
    //default:
      //break;
    }
    instruction='\0';
  }

return 1;
}


int parse_arguments2(int argc,char *argvx[], EmuEnv* _env) {
  args::ArgumentParser parser("OSM-Tree Creator.", "");

  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);

  args::ValueFlag<int> size_ratio_cmd(group1, "T", "The number of unique inserts to issue in the experiment [def: 2]", {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(group1, "P", "The number of unique inserts to issue in the experiment [def: 128]", {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(group1, "B", "The number of unique inserts to issue in the experiment [def: 128]", {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(group1, "E", "The number of unique inserts to issue in the experiment [def: 128 B]", {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(group1, "M", "The number of unique inserts to issue in the experiment [def: 2 MB]", {'M', "memory_size"});
  args::ValueFlag<int> delete_tile_size_in_pages_cmd(group1, "delete_tile_size_in_pages", "The number of unique inserts to issue in the experiment [def: 2]", {'h', "delete_tile_size_in_pages"});
  args::ValueFlag<long> file_size_cmd(group1, "file_size", "The number of unique inserts to issue in the experiment [def: 256 KB]", {"file_size"});
  args::ValueFlag<int> num_inserts_cmd(group1, "#inserts", "The number of unique inserts to issue in the experiment [def: 0]", {'i', "num_inserts"});
  args::ValueFlag<int> verbosity_cmd(group1, "verbosity", "The verbosity level of execution [0,1,2; def:0]", {'V', "verbosity"});

  try
  {
      parser.ParseCLI(argc, argvx);
  }
  catch (args::Help&)
  {
      std::cout << parser;
      exit(0);
      // return 0;
  }
  catch (args::ParseError& e)
  {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }
  catch (args::ValidationError& e)
  {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }

  _env->size_ratio = size_ratio_cmd ? args::get(size_ratio_cmd) : 2;
  _env->buffer_size_in_pages = buffer_size_in_pages_cmd ? args::get(buffer_size_in_pages_cmd) : 128;
  _env->entries_per_page = entries_per_page_cmd ? args::get(entries_per_page_cmd) : 128;
  _env->entry_size = entry_size_cmd ? args::get(entry_size_cmd) : 128;
  _env->buffer_size = buffer_size_cmd ? args::get(buffer_size_cmd) : _env->buffer_size_in_pages * _env->entries_per_page * _env->entry_size;
  _env->delete_tile_size_in_pages = delete_tile_size_in_pages_cmd ? args::get(delete_tile_size_in_pages_cmd) : 2;
  _env->file_size = file_size_cmd ? args::get(file_size_cmd) : _env->buffer_size;
  _env->num_inserts = num_inserts_cmd ? args::get(num_inserts_cmd) : 0;
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : 0;

  return 0;
}


void printEmulationOutput(EmuEnv* _env) {
  //double percentage_non_zero_result_queries = ceil((100 * _env->nonzero_to_zero_ratio) / (_env->nonzero_to_zero_ratio + 1.0));
  //if (_env->verbosity >= 1) 
  std::cout << "T, P, B, E, M, h, file_size, #I \n";
  std::cout << _env->size_ratio << ", ";
  std::cout << _env->buffer_size_in_pages << ", ";  
  std::cout << _env->entries_per_page << ", ";
  std::cout << _env->entry_size << ", ";
  std::cout << _env->buffer_size << ", ";
  std::cout << _env->delete_tile_size_in_pages << ", ";
  std::cout << _env->file_size << ", ";
  std::cout << _env->num_inserts ;

  std::cout << std::endl;
}