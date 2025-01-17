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
        pageCount = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::initFileHandle(const std::string &fileName) {
        // associate this file handle to file requested by PagedFileManager
        file.open(fileName);
        // return error code if opening non-existent file
        if (!file.is_open()) return -1;

        // grab counter values from hidden first page of file
        file >> pageCount;
        file >> readPageCounter;
        file >> writePageCounter;
        file >> appendPageCounter;
        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // ensure file is actually open
        if (!file.is_open()) return -1;
        // ensure page exists
        if (pageNum >= pageCount) return -1;

        // seek to page we want, read page into data pointer
        file.seekg((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.read(static_cast<char *>(data), PAGE_SIZE);

        // increment counter, return successfully
        ++readPageCounter;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        // ensure file is actually open
        if (!file.is_open()) return -1;
        // ensure page exists
        if (pageNum >= pageCount) return -1;

        // seek to the page, write in data
        file.seekp((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.write(static_cast<const char *>(data), PAGE_SIZE);

        // increment counter, return successfully
        ++writePageCounter;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        // ensure file is actually open
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