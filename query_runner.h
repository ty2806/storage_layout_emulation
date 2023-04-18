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
    static int secondary_range_occurances;

    static long sum_page_id;
    static long found_count;
    static long not_found_count;

    static void checkDeleteCount (int deletekey);
    static int pointQuery (int key);
    static void pointQueryRunner (int iterations);
    static int rangeQuery (int lowerlimit, int upperlimit, double QueryDrivenCompactionSelectivity);
    static void secondaryRangeQuery (int lowerlimit, int upperlimit);

    static void range_query_experiment();
    static void sec_range_query_experiment();
    static void delete_query_experiment();
    static void point_query_experiment();
    static void new_point_query_experiment();

};
