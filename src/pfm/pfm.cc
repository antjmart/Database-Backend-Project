#include "src/include/pfm.h"
#include <fstream>
#include <iostream>
#include <cstdio>

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
        if (!newFile.good()) return -1; // check if file created successfully

        // close unneeded file stream, return success
        newFile.close();
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        RC removeStatus = remove(fileName.c_str());
        return removeStatus == 0 ? 0 : -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // error code if fileHandle associated to file already
        if (fileHandle.file.is_open()) return -1;
        // either success or failure when file handle opens up the given file
        return fileHandle.initFileHandle(fileName);
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        // error if fileHandle not associated to open file
        if (!fileHandle.file.is_open()) return -1;
        // disassociate file handle
        fileHandle.detachFile();
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        pageCount = 0;
    }

    FileHandle::FileHandle(const FileHandle & fh) {
        // file stream will not be copied, assignment is for copying counters
        readPageCounter = fh.readPageCounter;
        writePageCounter = fh.writePageCounter;
        appendPageCounter = fh.appendPageCounter;
        pageCount = fh.pageCount;
    }

    FileHandle::~FileHandle() = default;

    FileHandle & FileHandle::operator = (const FileHandle & other) {
        // file stream will not be copied, assignment is for copying counters
        readPageCounter = other.readPageCounter;
        writePageCounter = other.writePageCounter;
        appendPageCounter = other.appendPageCounter;
        pageCount = other.pageCount;
        return *this;
    }

    void FileHandle::createHiddenPage() {
        file.seekp(0, std::ios::beg);
        file << "0 0 0 0";
        for (int bytesWritten = 7; bytesWritten < PAGE_SIZE; ++bytesWritten)
            file << '\0';
        file.flush();
    }

    RC FileHandle::initFileHandle(const std::string &fileName) {
        // attempt to open the file
        file.open(fileName, std::ios::in | std::ios::out | std::ios::binary);
        // return error code if opening non-existent file
        if (!file.is_open()) return -1;

        // initialize hidden page if empty file
        file.seekg(0, std::ios::end);
        if (file.tellg() == 0) createHiddenPage();

        // grab counter values from hidden first page of file
        file.seekg(0, std::ios::beg);
        file >> pageCount;
        file >> readPageCounter;
        file >> writePageCounter;
        file >> appendPageCounter;

        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
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
        // ensure page exists
        if (pageNum >= pageCount) return -1;

        // seek to the page, write in data
        file.seekp((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        file.write(static_cast<const char *>(data), PAGE_SIZE);
        file.flush();

        // increment counter, return successfully
        ++writePageCounter;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        // seek to new page position, write in data
        file.seekp(++pageCount * PAGE_SIZE, std::ios::beg);
        file.write(static_cast<const char *>(data), PAGE_SIZE);
        file.flush();

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

    void FileHandle::detachFile() {
        file.seekp(0, std::ios::beg);
        file << pageCount << ' ' << readPageCounter << ' ' << writePageCounter << ' ' << appendPageCounter << " \n";
        file.close();
    }
} // namespace PeterDB