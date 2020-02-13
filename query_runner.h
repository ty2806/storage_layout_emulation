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

    static int checkDeleteCount (int deletekey);
    static int pointQuery (int key);
    static int pointQueryRunner (int iterations);
    static int rangeQuery (int lowerlimit, int upperlimit);
    static int secondaryRangeQuery (int lowerlimit, int upperlimit);

    static int range_query_experiment();
    static int sec_range_query_experiment();
    static int delete_query_experiment();
    static int point_query_experiment();

};
