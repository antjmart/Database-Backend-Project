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
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return -1;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        fileName = "";
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // open file, return error code if file does not exist
        std::ifstream file(fileName);
        if (!file.is_open()) return -1;

        // seek to page we want, read page into data pointer
        file.seekg((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.read((char *)data, PAGE_SIZE);

        // increment counter, return successfully
        ++readPageCounter;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // open file, return error code if file doesn't exist
        std::ofstream file(fileName);
        if (!file.is_open()) return -1;

        // seek to the page, write in data
        file.seekp((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.write((const char *)data, PAGE_SIZE);

        // increment counter, return successfully
        ++writePageCounter;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        return -1;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB