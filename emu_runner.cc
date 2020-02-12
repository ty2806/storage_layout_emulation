/*
 *  Created on: May 13, 2019
 *  Author: Subhadeep, Papon
 */
 
#include <sstream>
#include <iostream>
#include <cstdio>
#include <sys/time.h>
#include <cmath>
#include <unistd.h>
#include <assert.h>
#include <fstream>
#include <stdlib.h>

#include "args.hxx"
#include "emu_environment.h"
#include "tree_builder.h"
#include "workload_executor.h"
#include "workload_generator.h"
#include "query_runner.h"

using namespace std;
using namespace tree_builder;
using namespace workload_exec;

/*
 * DECLARATIONS
*/
int Query::delete_key = 700;
int Query::range_start_key = 2000;
int Query::range_end_key = 5000;
int Query::sec_range_start_key = 200;
int Query::sec_range_end_key = 500;
int Query::iterations_point_query = 100000;

//long inserts(EmuEnv* _env);
int parse_arguments2(int argc, char *argvx[], EmuEnv* _env);
void printEmulationOutput(EmuEnv* _env);

//int run_workload(read, pread, rread, write, update, delete, skew, others);
int runWorkload(EmuEnv* _env);


int main(int argc, char *argvx[]) {

  // check emu_environment.h for the contents of EmuEnv and also the definitions of the singleton experimental environment 
  EmuEnv* _env = EmuEnv::getInstance();
  fstream fout;
  
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

    fout.open("out.csv", ios::out);

    fout << " " << ", " 
          << "Delete" << ", " << ", " << ", " << ", " 
          << "Range" << ", " << ", "
          << "Sec Range" << ", " << ", "
          << "Point" << ", " << ", " << ", "
          << "\n";
    

    fout << "Delete tile size" << ", " 
          << "(Delete) KEY" << ", " 
          << "(Delete) Full Drop" << ", " 
          << "(Delete) Partial Drop" << ", " 
          << "(Delete) Impossible Drop" << ", " 
          << "(Range) KEYs" << ", " 
          << "(Range) Occurances" << ", " 
          << "(Sec Range) KEYs" << ", " 
          << "(Sec Range) Occurances" << ", " 
          << "(Point) Iterations" << ", " 
          << "(Point) Sum_Page_Id" << ", " 
          << "(Point) Avg_Page_Id" << ", " 
          << "(Point) Found" << ", " 
          << "(Point) Not Found" << "\n";
    
    int only_file_meta_data = 0;

    if(_env->delete_tile_size_in_pages > 0)
    {
      int s = runWorkload(_env);
      if (MemoryBuffer::verbosity == 1 || MemoryBuffer::verbosity == 2 || MemoryBuffer::verbosity == 3)
      {
        DiskMetaFile::printAllEntries(only_file_meta_data);
        MemoryBuffer::getCurrentBufferStatistics();
        DiskMetaFile::getMetaStatistics();
      }

      Query::checkDeleteCount(Query::delete_key);
      Query::rangeQuery(Query::range_start_key, Query::range_end_key);
      Query::secondaryRangeQuery(Query::sec_range_start_key, Query::sec_range_end_key);
      Query::pointQueryRunner(Query::iterations_point_query);

      cout << "H" << "\t" 
        << "DKey" << "\t" 
        << "Full_Drop" << "\t" 
        << "Partial_Drop" << "\t" 
        << "No_Drop" << "\t\t" 
        << "RKEYs" << "\t" 
        << "Occurance" << "\t" 
        << "SRKEYs" << "\t" 
        << "Occurance" << "\t" 
        << "P_iter" << "\t\t" 
        << "Sumpid" << "\t\t" 
        << "Avgpid" << "\t\t" 
        << "Found" << "\t\t" 
        << "NtFound" << "\n";

      cout << _env->delete_tile_size_in_pages;
      cout.setf(ios::right);
      cout.precision(4);
      cout.width(10);
      cout << Query::delete_key;
      cout.width(11);
      cout << Query::complete_delete_count;
      cout.width(18);
      cout << Query::partial_delete_count;
      cout.width(12);
      cout << Query::not_possible_delete_count;
      cout.width(12);
      cout << Query::range_start_key << " " << Query::range_end_key;
      cout.width(9);
      cout << Query::range_occurances;
      cout.width(11);
      cout << Query::sec_range_start_key << " " << Query::sec_range_end_key;
      cout.width(9);
      cout << Query::secondary_range_occurances;
      cout.width(15);
      cout << Query::iterations_point_query;
      cout.width(16);
      cout << Query::sum_page_id;
      cout.width(16);
      cout << Query::sum_page_id/(Query::found_count * 1.0);
      cout.width(15);
      cout << Query::found_count;
      cout.width(17);
      cout << Query::not_found_count << "\n";

      if (MemoryBuffer::verbosity == 1 || MemoryBuffer::verbosity == 2 || MemoryBuffer::verbosity == 3)
        printEmulationOutput(_env);

    }
    else
    {
      for (int i = 1; i <= _env->buffer_size_in_pages; i = i*2)
      {
        cout << "Running for delete tile size: " << i << " ..." << endl;
        _env->delete_tile_size_in_pages = i;
        int s = runWorkload(_env);
        if (MemoryBuffer::verbosity == 1 || MemoryBuffer::verbosity == 2 || MemoryBuffer::verbosity == 3)
        {
          DiskMetaFile::printAllEntries(only_file_meta_data);
          MemoryBuffer::getCurrentBufferStatistics();
          DiskMetaFile::getMetaStatistics();
        }

        Query::checkDeleteCount(Query::delete_key);
        Query::rangeQuery(Query::range_start_key, Query::range_end_key);
        Query::secondaryRangeQuery(Query::sec_range_start_key, Query::sec_range_end_key);
        Query::pointQueryRunner(Query::iterations_point_query);

        fout << _env->delete_tile_size_in_pages << ", " 
          << Query::delete_key << ", " 
          << Query::complete_delete_count << ", " 
          << Query::partial_delete_count << ", " 
          << Query::not_possible_delete_count << ", " 
          << Query::range_start_key << " " << Query::range_end_key << ", " 
          << Query::range_occurances << ", " 
          << Query::sec_range_start_key << " " << Query::sec_range_end_key << ", " 
          << Query::secondary_range_occurances << ", " 
          << Query::iterations_point_query << ", " 
          << Query::sum_page_id << ", " 
          << Query::sum_page_id/(Query::found_count * 1.0) << ", " 
          << Query::found_count << ", " 
          << Query::not_found_count << "\n";
        
        Query::complete_delete_count = 0;
        Query::not_possible_delete_count = 0;
        Query::partial_delete_count = 0;
        Query::range_occurances = 0;
        Query::secondary_range_occurances = 0;
        Query::sum_page_id = 0;
        Query::found_count = 0;
        Query::not_found_count = 0;
        
        if (MemoryBuffer::verbosity == 1 || MemoryBuffer::verbosity == 2 || MemoryBuffer::verbosity == 3)
        printEmulationOutput(_env);
      }
    }
    fout.close();
    //srand(time(0));
    //WorkloadExecutor::getWorkloadStatictics(_env);
    //assert(_env->num_inserts == inserted); 
  }
  return 0;
}

int runWorkload(EmuEnv* _env) {

  MemoryBuffer* buffer_instance = MemoryBuffer::getBufferInstance(_env);
  WorkloadExecutor workload_executer;
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
  args::ArgumentParser parser("KIWI Emulator", "");

  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);

  args::ValueFlag<int> size_ratio_cmd(group1, "T", "The number of unique inserts to issue in the experiment [def: 2]", {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(group1, "P", "The number of unique inserts to issue in the experiment [def: 128]", {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(group1, "B", "The number of unique inserts to issue in the experiment [def: 128]", {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(group1, "E", "The number of unique inserts to issue in the experiment [def: 128 B]", {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(group1, "M", "The number of unique inserts to issue in the experiment [def: 2 MB]", {'M', "memory_size"});
  args::ValueFlag<int> delete_tile_size_in_pages_cmd(group1, "delete_tile_size_in_pages", "The number of unique inserts to issue in the experiment [def: -1]", {'h', "delete_tile_size_in_pages"});
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
  _env->delete_tile_size_in_pages = delete_tile_size_in_pages_cmd ? args::get(delete_tile_size_in_pages_cmd) : -1;
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