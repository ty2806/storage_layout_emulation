/*
 *  Created on: Jan 28, 2019
 *  Author: Papon
 */


#include <iostream>

using namespace std;


class Query
{
private:
  
public:
    static int delete_key;
    static int range_start_key;
    static int range_end_key;
    static int sec_range_start_key;
    static int sec_range_end_key;
    static int iterations_point_query;

    static int complete_delete_count;
    static int not_possible_delete_count;
    static int partial_delete_count;

    static int range_occurances;
    static void checkDeleteCount (int deletekey);
    static int pointQuery (int key);
    static int rangeQuery (int lowerlimit, int upperlimit, double QueryDrivenCompactionSelectivity);
    static void vanilla_range_query (int lowerlimit, int upperlimit);
    static void range_query_compaction_experiment(float selectivity, string file, int insertion, double QueryDrivenCompactionSelectivity);
    static void point_query_experiment(float selectivity, double QueryDrivenCompactionSelectivity, int insert_time);

};
