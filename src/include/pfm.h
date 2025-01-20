#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <fstream>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // file stream for reading and writing
        std::fstream file;

        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        unsigned pageCount;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor
        FileHandle & operator = (const FileHandle & other);                 // Assigns counters, not open file

        RC initFileHandle(const std::string &fileName);                     // Pull info from file's first page
        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables

        void detachFile();                                                  // Update open file hidden page then detach

    private:
        void createHiddenPage();                                            // Helper function for creating hidden page
    };

} // namespace PeterDB

#endif // _pfm_h_