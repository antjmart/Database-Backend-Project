## Project 1 Report


### 1. Basic information
 - Team #: 8
 - Github Repo Link: https://github.com/antjmart/cs122c-winter25-antjmart
 - Student 1 UCI NetID: ajmarti8
 - Student 1 Name: Anthony Martinez
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format
- Show your record format design.

[Number of Fields: 2 bytes][Null Flags: ? bytes][Offset Pointers: 2 bytes for each field][Non-Null Field Entries: ? Bytes]

- Describe how you store a null field.

Null fields are not directly stored in the record. Instead, they have a corresponding offset
pointer that "points" to where the field would be inserted if it was not null. Pointers are kept even
for null values in order to keep number of field pointers consistent no matter how many nulls, helping
with O(1) field access.

- Describe how you store a VarChar field.

The field entry for a VarChar in my record takes up 4 + (length of varchar value) bytes, minimum amount
needed for the field entry. The first 4 bytes are for integer value specifying length of the varchar value, then the
characters are stored after. This field's corresponding offset pointer in the record points to start of the 4 bytes
for length.

- Describe how your record design satisfies O(1) field access.

My record has a pointer for each field. First, the null bit is checked for a field. If null,
then null value is returned. If not null, then go forward 2 * i from the end of null bytes, i being field num starting
from 0, in order to reach pointer for the field. Then, take that offset pointer to jump directly to
entry for that field.

### 3. Page Format
- Show your page format design.

Hidden Page: pageCount[space]readPageCounter[space]writePageCounter[space]appendPageCounter[empty bytes for rest of page]

Page with Records:
[Records][Free Space][Slot Directory][2-byte N value for number of records][2-byte F value for number of free bytes]


- Explain your slot directory design if applicable.

Each entry in slot directory is 4 bytes, 2 bytes for record's offset value (from start of page) followed by
2 bytes for record's length in bytes. Entries for slot numbers starts from right to left.
First entry is right before the N value spot, second entry is right before first entry, and so on.

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

It is assumed that all free space follows the records and precedes slot directory along with N and F values. The F value on a page reflects this amount
of free bytes. First, the last page is checked. If the required space for the record (its length plus 4 bytes for slot
directory entry) is less than or equal to the F value, then it fits on the page. If this is false, then
loop sequentially through each page, starting from page zero and ending before the last page. Each page has the same
check with F value done, and once that check is passed that page gets selected. If all pages are checked and determined
not spacious enough, then page number will be equal to the page count for a new page to appended that contains this
record. Page count is incremented.

- How many hidden pages are utilized in your design?

1 per file

- Show your hidden page(s) format design if applicable

Hidden Page: pageCount[space]readPageCounter[space]writePageCounter[space]appendPageCounter[empty bytes for rest of page]

### 5. Implementation Detail
- Other implementation details goes here.

FileHandle carries a file stream object, it is opened when the handle is associated to a file,
closed when the file gets closed by the PagedFileManager. Page writes are manually flushed to avoid repetitive
file stream open and closing. When a file handle is associated and opens a file stream, its variables are retrieved
from the hidden page. If no hidden page yet, then its initialized with all zeroes. 

### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A

### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)