## Project 3 Report


### 1. Basic information
- Team #: 8
- Github Repo Link: https://github.com/antjmart/cs122c-winter25-antjmart
- Student 1 UCI NetID: ajmarti8
- Student 1 Name: Anthony Martinez
- Student 2 UCI NetID (if applicable):
- Student 2 Name (if applicable):


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 



### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
  
  - entries on leaf nodes:



### 4. Page Format
- Show your internal-page (non-leaf node) design.
  
  Node Page: 1 byte leaf check, 2 bytes offset to end of entries, 4 bytes for first "less than" pointer, rest of page
is key entries that include page pointers


- Show your leaf-page (leaf node) design.
  Leaf Page: 1 byte leaf check, 4 bytes next page, 2 bytes offset to end of entries, rest of page is key entries


### 5. Describe the following operation logic.
- Split

Create a new page, that new page's next pointer is the splitting page's next pointer. Split page's
next pointer now the new page. Splitting page is given enough entries to be at half capacity, then gives
the rest to new page. If splitting a leaf, middle value goes to new page and pushed up. If splitting a
non-leaf, middle value only gets pushed up.

- Rotation (if applicable)



- Merge/non-lazy deletion (if applicable)



- Duplicate key span in a page

If equal to a key in the page, simply insert there. I chose to insert before equal keys, not after.

- Duplicate key span multiple pages (if applicable)

Put the record id into the key so that all comparisons and tree nodes include both the key values and
record ids. For this, I defined a template Key class with predefined comparison operators that compare
both key and RID.

### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.



- Other implementation details:



### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.

All solo work

### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)
