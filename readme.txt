classical
./emu_runner -i 1000000 -P 4096 -B 4 -E 1024 -T 10 -h 1 -c 0 -D 1000 -S 10000 -F 1000000 -s 1000 -f 10000 -N 1000000
./emu_runner -i 10000 -P 100 -B 4 -E 1024 -T 10 -h 1 -c 0 -D 1000 -S 10 -F 10002

i = 1000000 [Number of inserts]
P = 4096 [Buffer size in pages]
B = 4 [Entries in page].
E = 1024 [Entry size in bytes]
T = 10 [Size Ratio]
h = 1 [Delete tile size in pages. If h not specified (-1), the code will be in experiment mode. It will run for all values of h from 1 to P (multiplicative factor 2). Also, if h is not specified, the values D, S, F, s, f and N are don't care. They are tuned inside the code based on experiment selectivity]

c = 0 means no correlation in data (Which is default). 1 means sort key = delete key
D = 1000 [Delete all keys that have delete key < 1000]
S = 10000 [Start key for primary range query]
F = 1000000 [End key for primary range query]
s = 1000 [Start key for secondary range query]
f = 10000 [End key for secondary range query]
N = 1000000 [Iterations for point query]

Another tuning parameter is the KEY_DOMAIN_SIZE in workload_generator.cc 

Small running command:

./emu_runner -i 50 -P 4 -B 2 -E 32 -T 2 -h 1 -V 1


------------------------------------------------------
Lethe+

X: Same h across tree or different h [0 for classical lethe, 1 for Kiwi-optimal, 2 for Kiwi+]

./emu_runner -i 5000000 -P 256 -B 4 -E 1024 -T 10 -X 2 -I 1 -J 1000000 -K 1000000 -L 1000
./emu_runner -i 500 -P 8 -B 4 -E 1024 -T 2 -X 2 -I 1 -J 100 -K 100 -L 2

I = 1 [count of secondary range delete query]
J = 1000000 [count of empty point queries]
K = 1000000 [count of non-empty point queries]
L = 10000 [count of short range queries]

------------------------------------------------------
Range Query Driven Compaction

Implementation:
Query::rangeQuery: conduct range query and invoke query driven compaction given a range
Utility::QueryDrivenCompaction: conduct query driven compaction given the range query result
Utility::sortMergeRepartition: conduct sort merge and repartition for middle levels during query driven compaction

Utility::sortAndWrite: conduct sort merge and compaction. It invokes Utility::sortMerge and Utility::compactAndFlush
Utility::sortMerge: conduct sort merge
Utility::compactAndFlush: conduct compaction and write SST files back to a level

./emu_runner -i 100000 -P 20 -B 2 -E 1024 -T 5 -h 1 -c 0 -D 1000 -S 10 -F 10000

The insertions are bounded by 100k, and the insert times and experiments are adjusted in int main() of emu_runner accordingly. 
- vanilla write count for 5 or 10 times insertions (line 101 - 134)
- vanilla sequential evaluation for different selectivities (line 137 - 175)
- Sequential Evaluation with Compaction for different selectivities and QueryDrivenCompactionSelectivities (line 178 - 221)
- Non Sequential Evaluation with Compaction for different selectivities and QueryDrivenCompactionSelectivities (line 224 - 273)

