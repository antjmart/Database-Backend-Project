## Project 4 Report


### 1. Basic information
- Team #:
- Github Repo Link: https://github.com/antjmart/cs122c-winter25-antjmart
- Student 1 UCI NetID: ajmarti8
- Student 1 Name: Anthony Martinez
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

I created a new catalog table called Indices. It has these columns: (table-id, TypeInt), (attribute-name, TypeVarChar),
and (file-name, TypeVarChar). Each record in Indices corresponds to a B+ tree index file. The table id refers
to the unique ID of the associated table an index is for. The attribute name is the attribute the index file is
built on. And finally, the file name is the name of the file the B+ tree index is found in, ending in .idx. To access an
index file, the table id is retrieved from Tables, and then scans over Indices table to find a match with
the attribute name.

### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

Upon construction, the filter sets up condition, sets up other variables, and grabs attributes from underlying iterator.
When looking for the next tuple, the filter gets the next tuple, if there is one, from underlying iterator. Then, it checks
if the tuple meets the condition, getting another tuple from the iterator if the condition is failed. To check the condition,
first a loop through the tuple is done to find the lhsAttr the condition is for. In any case if the lhsAttr value is null,
the condition automatically fails. If the rhs of the condition is just a value, the most common case, then both the
lhsAttr value and the rhsValue object are converted to relevant data type (int, float, std::string), where it is returned
whether the matching comparison (based on condition operator) is true or not. If the rhs of the condition was another
attribute, then the value is found during the initial loop, followed by the same type conversion and operator comparison
to get the final boolean result.

### 4. Project
- Describe how your project works.

Upon construction, Project saves the underlying iterator, saves attribute list from that iterator, saves a hash set of the
attribute names to be projected, and then saves a vector of just the projected attributes in same order as given
attribute names. For getting a tuple, first a tuple is fetched, if not at EOF, from the underlying iterator. Then, iterate
through the tuple, keeping an unordered_map mapping each attribute to be projected to a pointer pointing at byte location
in the tuple. Next, allocate a byte array. Loop through each projected attribute and get byte location from the map, setting its null bit if null, or appending
its value to the new byte array. At the end, copy new byte array to the data array parameter.

### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

I maintain three unordered_maps, one for each data type (int, float, std::string) that maps a key to vector of tuple byte arrays.
One is used based on the joining attribute key type, the other two remain empty. For memory limiting, a byte limit is calculated
by multiplying PAGE_SIZE by the numPages parameter. When getting the next tuple, first check if there was a vector
of tuples from one of the unordered_maps that was already being looped through. If so, combine the next LHS tuple in
that vector with the current RHS tuple and return. If not, then next check if any memory bytes are used storing tuples from
the LHS. If not, then its time to get the next batch of LHS iterator tuples. Tuples keep getting retrieved from the LHS
and added to the corresponding key vector in corresponding map until either EOF is reached or the memory byte limit is reached
(size of each retrieved tuple is kept track of and added to byte usage). If no tuples from LHS were saved, then EOF of LHS and therefore the BNLJoin
is reached. After the key mapping process from LHS, iterate through tuples from the RHS iterator until a key match
to the corresponding key map is found. Store a pointer to that vector, then construct tuple from first LHS tuple in vector with
RHS tuple, and return. Further calls for getNextTuple will keep going through that vector until its covered. If RHS is fully
scanned, then all bytes are deallocated from the maps, then looping back to load in more tuples from the LHS.

### 6. Index Nested Loop Join
- Describe how your index nested loop join works.

To get the next tuple, first it is checked if there is a next tuple to get from the RHS index scanner. If so, loop through
and determine the size of the RHS tuple, then construct new tuple that combines current LHS tuple with the RHS tuple and return. If
no more RHS tuples to go through, then keep retrieving tuples from LHS (iterate through and set the key value) until getting
one that does not have a null value for the joining attribute. Then, set the RHS index scanner with both low and high key
as the LHS key value for equality. Loop back through to start iterating through RHS tuples again. Eventually once EOF on
the LHS tuples are hit, then EOF is reached for INLJoin.

### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.

Upon construction, get attribute vector from underlying iterator then loop through attributes, saving the index corresponding
to aggregate attribute. Then, call method that corresponds to aggregate operator. For any of the operators, a byte array
storing the result is allocated, and first byte representing nulls is always set to zero, and the first call to getNextTuple() will retrieve the array. The getNextAttribute() method
sets the vector with just one attribute that has the modified name with name of operator and parentheses. Each operator will keep getting
tuples from underlying iterator (ignoring the aggregate attribute value from tuple if null) until EOF is reached, then aggregate
value is stored in the byte array. MIN and MAX keep track of outlier value, COUNT increments for each considered tuple, SUM adds
up all the values, AVG sums up values then divides by number of summed values.

- Describe how your group-based aggregation works. (If you have implemented this feature)

Similar construction process as simple aggregation, but also saves index corresponding to grouping attribute. Instead of just one
byte array storing the aggregate result, a vector of byte arrays for each unique value of the grouping attribute is kept instead.
During aggregate calculation process, a mapping of grouping attribute value to its aggregation is maintained. The aggregation value
is either the min, max, count, sum, or avg (in this case, both a mapping to sum aggregate and count aggregate is kept, avg calculation
done at the end when allocating byte arrays) of a given grouping attribute value. The type of grouping and aggregate attributes
are checked to ensure the unordered_map has correct types. Each aggregation method keeps retrieving tuples from the underlying
iterator until EOF. A tuple is ignored if either aggregate or grouping attribute value is null. Once all aggregation is finished, updating
the group mappings along the way, then store an allocated byte array (containing all-zero null byte, followed by group value, followed by
aggregate value) for each group value from the unordered_map. getAttributes() differs from basic aggregation by first adding the
grouping attribute. Each call to getNextTuple() will return next byte array in the vector of byte arrays.

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.

N/A, no additions

- Other implementation details:



### 10. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A, solo project

### 11. Other (optional)
- Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)

N/A

- Feedback on the project to help improve the project. (optional)

I think it would be good to have more testcases offered on aggregation, both simple and group aggregation. Right now,
only a few type of aggregates are thoroughly tested, so it would be nice to have more extensive testing on all five
aggregate types on both simple and group aggregation.