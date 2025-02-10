## Project 2 Report


### 1. Basic information
 - Team #: 8
 - Github Repo Link: https://github.com/antjmart/cs122c-winter25-antjmart
 - Student 1 UCI NetID: ajmarti8
 - Student 1 Name: Anthony Martinez
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

Tables table follows the design in lecture notes. It has table id, table name, file name, and I added
a fourth field called is-system-table. This is an integer that is either equal to 0 or 1. If it is 1, it
is a system table that cannot be directly modified
by functions such as insertTuple, deleteTuple, deleteTable, etc.
My columns table follows the lecture slides format exactly, no changes. It has table-id, column-name,
column-type, column-length, and column-position.

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

Added a singular tombstone checking byte at the beginning of record that is either 0 or 1. New layout:

{tombstone check byte}{4 bytes for record field count}{null flag bytes}{2 bytes for each record field, stores
pointer offset value}{actual field data}

- Describe how you store a null field.

N/A no change from P1

- Describe how you store a VarChar field.

N/A no change from P1

- Describe how your record design satisfies O(1) field access.

N/A no change from P1

### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

N/A no changes from P1

- Explain your slot directory design if applicable.

N/A no changes from P1

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

N/A no changes from P1

- Show your hidden page(s) format design if applicable

N/A no changes from P1

### 6. Describe the following operation logic.
- Delete a record

Since updating records introduces tombstones, I first start at the provided record id and iterate through
records (if they're tombstones) until I find the record that actually stores the desired record data
to delete. During iterating through tombstones, it stops once the tombstone check byte is 0. Also, before
moving on to next record spot, this tombstone is deleted. Deletion process for tombstone or regular record:
set the corresponding slot offset and length values to zero to signify a now open slot; next, use the length of
the record to be deleted to  shift to the left all records that followed the deleted record. This can be done
by checking the offset of each slot, and then update those offset values if the shift affects the record.

- Update a record

Just like in delete record, I follow the same iteration process through tombstones to find the actual record data
I want to update. The difference is that I do not delete the tombstones as I iterate. The length of the new record
data is calculated. If the length is equal to the current record's length, then the data is simply swapped. If the new
length is less, then following records on the page are all shifted left the difference between the lengths. The new
data is then placed into the shortened space. If the new length is longer but enough free space in the page, then
following records on the page are shifted right the length difference, new record goes in lengthened space. If
there is not enough free space to fit the longer record, then the new record data is inserted elsewhere. The
record id of that new location is then stored in the old record's 6 bytes following the tombstone byte (which will now be 1).

- Scan on normal records

My scanning algorithm is the same regardless of what has been done to the records. It starts from the first page
and first slot. For each page, the slot count is fetched which determines the maximum amount of record slots to be
checked on that page. The iterator iterates through each slot number, resetting back to 1 and incrementing the page
number when it hits the maximum slot number. A file handle object associated with the scan iterator provides
the page count so that it knows when it has reached the EOF. For each record id that is scanned, it is skipped
if it's unused (zero length) or a tombstone. If an actual record, then the record data is checked against the
condition. If accepted, then the iteration returns and the iterator internally stores where it left off.

- Scan on deleted records

The scan iterator skips over empty slots with zero length, so a deleted record would not be scanned. If a new record
takes that unused slot, then the scanner will find the new record.

- Scan on updated records

The scanner will skip over potential tombstones that link to an updated record, but it will process
the actual record data when it is reached (the non-tombstone).

### 7. Implementation Detail
- Other implementation details goes here.

Most RelationManager functions will fetch the attributes, creating a record descriptor, corresponding to
the table name. This is done by scanning Tables for the table id, then scanning Columns looking for records
with the correct table id. Then with the attributes, RBFM functions are called to place in the tuple data.

### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A

### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)

N/A

- Feedback on the project to help improve the project. (optional)

More coding examples potentially. Some of the concepts, especially schema versioning are very difficult
and could be helped with some conceptual or code examples to get the right mindset. Also, the testing and utility
functions should be cleaned up so students do not get valgrind warnings from them.