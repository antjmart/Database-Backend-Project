## Debugger and Valgrind Report

### 1. Basic information
 - Team #: 8
 - Github Repo Link: https://github.com/antjmart/cs122c-winter25-antjmart
 - Student 1 UCI NetID: ajmarti8
 - Student 1 Name: Anthony Martinez
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Using a Debugger
- Describe how you use a debugger (gdb, or lldb, or CLion debugger) to debug your code and show screenshots. 
For example, using breakpoints, step in/step out/step over, evaluate expressions, etc. 

I used a mix of gdb sequential steps and some standard print statements in more obscure parts of my code.
I tended to place these before sets of statements, right before loops, in the middle of loops,
and at the end of functions. This allowed me to figure out where my functions were returning
bad error codes. Mainly, the GTests were used to fully determine
if my functionality for things were correct. Clicking to create red dot breakpoints for gdb was most useful
and some print statements helped when just testing a couple lines.

### 3. Using Valgrind
- Describe how you use Valgrind to detect memory leaks and other problems in your code and show screenshot of the Valgrind report.

I built the gtest files and ran those executables with
valgrind to ensure memory was used correctly. The built-in Valgrind Clion tool
runs the gtests, and the flags are
--leak-check=full --show-leak-kinds=all --track-origins=yes --verbose

PFM Tests Run with Valgrind Memcheck:

![img.png](img.png)
![img_1.png](img_1.png)

Clean memory usage for all the PFM tests, no valgrind feedback.

RBFM Tests Run with Valgrind Memcheck:

![img_2.png](img_2.png)
![img_3.png](img_3.png)

No memory errors from my source code, clean allocation and deallocation.
Minor issues in the provided testing libraries where free gets used on a
pointer that was allocated with new, but these issues have nothing
to do with my code.

RelationManager Tests Run with Valgrind Memcheck:

![rmvalgrind.png](rmvalgrind.png)

Clean memory usage, no lost bytes or lost allocations. Some warnings
about mixing use of new and free operators in the provided testing library
files, but nothing that my code has any impact on.