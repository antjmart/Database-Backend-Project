This is a project that showcases development of the backend of a simple relational database system.
Overall, the system features these five main components, each with their own class or set of classes in designated files.

### Components:
 - PagedFileManager
 - RecordBasedFileManager
 - RelationManager
 - IndexManager
 - QueryEngine

The PagedFileManager handles creation, deletion, opening, and closing files, and all updates to files are done by pages, which are 4 KB in the project.

The RecordBasedFileManager also handles files but can insert, update, and remove records from file pages, which correlate to record entries for the database.

The IndexManager manages files that contain B+ tree indexes on single table attributes.

The RelationManager uses functionality from both the RecordBasedFileManager and IndexManager to upkeep and manage logical tables for the database, storing records
for a table in its corresponding file, and keeping track of index files pertaining to a specific table.

The QueryEngine includes many classes that represent various relational algebra operations such as projection, selection, joins, and aggregation. This component layer runs the logic
of what SQL commands would be requesting.

This project does not include a frontend that processes SQL commands, but the functionality in all the classes may be used to simulate the actions that various SQL commands would invoke.

Simple features of database backend: only integers, reals, and varchar data types supported; only B+ tree indexes, single column attributes only; only simple joins such as inner join supported

The src/ directory contains all header and source .cc files pertaining to the five components. The test/ folder contains various testcases that were used to evaluate the project code, and these tests can be compiled using the cmake build presets. Other files and folders are resource and setup files. The classes in all the source code may be used in other programs to test out their functionality and simulate database backend functionality that SQL commands may need. There is currently no frontend interface to compile for processing SQL commands.
