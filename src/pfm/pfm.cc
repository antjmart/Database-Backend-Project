#include "src/include/pfm.h"
#include <fstream>
#include <iostream>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        std::ifstream fileCheck(fileName);
        if (fileCheck.is_open()) {
            fileCheck.close();
            return -1;  // error if file already exists
        }
        std::ofstream newFile(fileName);
        if (!newFile.is_open()) return -1; // check if file created successfully

        // initialize hidden page counters
        newFile << "0 0 0 0\n";
        // close unneeded file stream, return success
        newFile.close();
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // error code if fileHandle associated to file already
        if (fileHandle.filename != "") return -1;
        // either success or failure when file handle opens up the given file
        return fileHandle.initFileHandle(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return -1;
    }

    FileHandle::FileHandle() {
        filename = "";
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        pageCount = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::initFileHandle(const std::string &fileName) {
        // attempt to open the file
        std::ifstream file(fileName);
        // return error code if opening non-existent file
        if (!file.is_open()) return -1;

        filename = fileName;
        // grab counter values from hidden first page of file
        file.seekg(0, std::ios::beg);
        file >> pageCount;
        file >> readPageCounter;
        file >> writePageCounter;
        file >> appendPageCounter;
        file.close();
        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // open up input stream for reading
        std::ifstream file(filename);
        // ensure file exists
        if (!file.is_open()) return -1;
        // ensure page exists
        if (pageNum >= pageCount) return -1;

        // seek to page we want, read page into data pointer
        file.seekg((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.read(static_cast<char *>(data), PAGE_SIZE);

        // increment counter, return successfully
        file.close();
        ++readPageCounter;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // open up file stream for writing
        std::fstream file(filename);
        // ensure file exists
        if (!file.is_open()) return -1;
        // ensure page exists
        if (pageNum >= pageCount) return -1;

        // seek to the page, write in data
        file.seekp((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.write(static_cast<const char *>(data), PAGE_SIZE);

        // increment counter, return successfully
        file.close();
        ++writePageCounter;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        // open up file stream for writing
        std::fstream file(filename);
        // ensure file exists
        if (!file.is_open()) return -1;

        // seek to new page position, write in data
        file.seekp(++pageCount * PAGE_SIZE, std::ios::beg);
        file.write(static_cast<const char *>(data), PAGE_SIZE);

        // increment counter, return successfully
        ++appendPageCounter;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return pageCount;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB