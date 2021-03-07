# CS3223: Select-Project-Join (SPJ) Query Engine
## Specifications
> External sort algorithm was used to implement Sort Merge Join, DISTINCT and ORDERBY.
* [Block Nested Loops Join](/COMPONENT/src/qp/operators/BlockNestedJoin.java)
* [External Sort](COMPONENT/src/qp/operators/ExternalSort.java)
* [Sort Merge Join](COMPONENT/src/qp/operators/SortMergeJoin.java)
* [DISTINCT](COMPONENT/src/qp/operators/Distinct.java)
* [ORDERBY](COMPONENT/src/qp/operators/Orderby.java)
* [Aggregate Functions](COMPONENT/src/qp/operators/Project.java)
* Others
  * [Aggregate Value (MIN, MAX, COUNT, AVG, SUM)](COMPONENT/src/qp/utils/AggregateValue.java)
  * [Plan Cost](COMPONENT/src/qp/optimizer/PlanCost.java)
  * [BatchList](COMPONENT/src/qp/utils/BatchList.java)

## Details
### BatchList
* BatchList class stores an ArrayList of Batches which represents the buffer pages in memory.
### Block Nested Loops Join
* BNJ makes use of BatchList (ArrayList of Batches) as the “Block” for the outer relation during the join. The Block is initialized with B-2 pages, where B represents the number of buffers pages made available to the join in the Physical Plan. The tricky thing about the implementation is that the Output Batch can become full at any point in the next() function. Subsequent calls to next() have to figure out where the function last exited and continue from there. A plethora of flags and counters are used to cleverly keep track of this.
### Sort Merge Join
* Like BNJ the Output Batch can fill up anytime in the SMJ as well. It handles the issue in a similar fashion. 
* When 2 tuples match the join condition, we add them to either the TempLeft and TempRight files respectively. After ensuring that both the left and right do not have any more tuples with the same values on their join column, we then proceed to perform the join and write them to Output. The TempLeft and TempRight files incur additional I/O cost (on top of what the lecture says) - but they’re necessary to handle the edge case where backtracking operation crosses page boundaries. This also takes additional buffer space (on top of 3). The TempLeft takes upto 1 additional buffer space, and so does the TempRight. Hence, in the plan cost, we made sure that using SMJ would not be feasible if there are less than 5 buffers available.

### External Sort
* The External Sort is initialized with a list of attributes to sort by, in order of priority. Then it runs the 2 stages: 
  1. Generate sorted runs by populating the available buffer pages and running internal sort. (See `generateSortedRuns(...)` function)
  2. Implement multi-way merge sort algorithm by using B-1 TupleReader instances (each run gets 1 buffer page). We then compare the head of the runs and write the winning candidate to disk (using 1 TupleWriter instance). Recursively run this stage until the last pass (i.e. 1 file is remaining) See `mergeSortedRuns(...)` function

### DISTINCT
* The DISTINCT function is run after Projection. It uses - and only incurs the cost of - the External Sort.
We attempted a Hash Based Distinct Function, but it did not support recursive repartitioning in case of overflow. You can check the partial implementation in [DistinctByHash.java](COMPONENT/src/qp/operators/DistinctByHash.java).

### ORDERBY
* The ORDERBY function is run after the Joins. It uses the External Sort by passing in a direction which tells External Sort if it should sort the tuples in an ascending or descending order. The parser was modified following the instructions in the course webpage to enable this behaviour.

### Aggregate 
* The Aggregate functions are implemented within the [AggregateValue.java](COMPONENT/classes/qp/utils/AggregateValue.class) and are utilized within Project.java. That is, the aggregation happens only during Projection. As such, it does not incur additional cost. Two drawbacks of this approach are: 
You can’t mix aggregate functions with non-aggregate functions.
You can’t use aggregate functions anywhere except in the SELECT clause. 
Another additional caveat is that the Aggregate Functions use INTEGERS instead of FLOAT. This causes truncation of certain results. This can be quickly patched by changing the data types. 

## Bugs In Given Implementation
### Incorrect Page Nested Join Cost Calculation (Bug Fixed)
* The plan cost of nested join is given as joincost = leftpages * rightpages. However, the correct cost is leftpages + leftpages * rightpages instead.

### Ability to Go Over Buffer Space Limit (Limitation - Mitigated)
* The given implementation disregards the number of buffers available (configuration) and lets the developer exceed the limit. As an example, we can read all the tables into memory without any explicit errors. To guard against this, we created a BatchList utility class to ensure that we abide by the number of buffers that we have. We also did a thorough analysis of the number of buffer pages used in each function. Furthermore, we used TupleWriter to ensure overflowing batches were always written to disk and TupleReader to ensure reading files was done one page at a time.  

### Poor Constraint Enforcement (Spotted - Not Fixed)
* During Sample Data Creation, the FK constraints are not respected. Furthermore, Composite Primary Keys were not possible, and multiple primary keys in the same table were treated as separate PKey Columns. These bugs were spotted but not fixed. 

## Experiments
* We tested the correctness of our queries by running our sample queries on PostgreSQL. (See [here](testcases/../COMPONENT/testcases/utils/docker-compose.yml) for the PostgreSQL setup)
* Each sample query can be found in [here](COMPONENT/testcases/airlineDB) with their corresponding `.out` files that contain the results.
### Experiment 1 
* [query1.sql](COMPONENT/testcases/airlineDB/query1.sql)
* [query2.sql](COMPONENT/testcases/airlineDB/query2.sql)
* [query3.sql](COMPONENT/testcases/airlineDB/query3.sql)
### Experiment 2
* [query21.sql](COMPONENT/testcases/airlineDB/query21.sql)
### Report
* Check out our experimental results [here](CS3223-report.pdf)!





