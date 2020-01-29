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

    static int checkDeleteCount (int deletekey);
    static int pointQuery (int key);
    static int rangeQuery (int lowerlimit, int upperlimit);
    static int secondaryRangeQuery (int lowerlimit, int upperlimit);

};
