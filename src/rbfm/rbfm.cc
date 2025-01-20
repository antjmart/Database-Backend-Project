#include "src/include/rbfm.h"
#include <cstdlib>
#include <cstring>
#include <iostream>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle);
    }

    unsigned short RecordBasedFileManager::calcRecordSpace(const std::vector<Attribute> &recordDescriptor, const void * data) {
        // need 4 bytes for record directory entry, 2 for offset and 2 for length
        // need 2 more for number of non-null fields
        unsigned short recordSpace = 6;
        unsigned numFields = recordDescriptor.size();
        unsigned nullFlagBytes = numFields / 8;
        if (nullFlagBytes % 8 != 0) ++nullFlagBytes;
        recordSpace += nullFlagBytes;

        // each non-null field needs 2 bytes for offset pointer, plus data type size
        unsigned fieldsRemaining = numFields;
        for (unsigned i = 0, flagByte = 0; i < nullFlagBytes; ++i, flagByte = 0, fieldsRemaining -= 8) {
            // get set of 8 null flag bits
            memcpy(&flagByte, (const char *)data + i, 1);

            for (unsigned j = 0; j < 8 && j < fieldsRemaining; ++j)
                if (((flagByte >> j) & 1) == 0)  // null bit flag of zero means space is needed
                    recordSpace += 2 + recordDescriptor[i * 8 + j].length;
        }

        return recordSpace;
    }

    unsigned short RecordBasedFileManager::putRecordInEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, unsigned short recordSpace) {
        return 0;
    }

    unsigned short RecordBasedFileManager::putRecordInNonEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, unsigned short recordSpace) {
        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        void * pageData = malloc(PAGE_SIZE);
        unsigned pageNum;  // page which record will be inserted to, starts with last page
        unsigned freeIndex = PAGE_SIZE / sizeof(unsigned short) - 1;
        unsigned short recordSpace = calcRecordSpace(recordDescriptor, data);
        unsigned short slotNum;

        if (fileHandle.pageCount == 0) {
            pageNum = 0;  // no need to check pages
            slotNum = putRecordInEmptyPage(recordDescriptor, data, pageData, recordSpace);
        } else {
            unsigned short freeSpace;
            pageNum = fileHandle.pageCount - 1;
            if (fileHandle.readPage(pageNum, pageData) == -1) return -1;
            freeSpace = ((unsigned short *)pageData)[freeIndex];

            // if not enough space, need to start iterating through other pages
            if (recordSpace > freeSpace) {
                for (pageNum = 0; pageNum < fileHandle.pageCount - 1; ++pageNum) {
                    if (fileHandle.readPage(pageNum, pageData) == -1) return -1;
                    freeSpace = ((unsigned short *)pageData)[freeIndex];
                    if (recordSpace <= freeSpace) break;  // stop searching if enough space
                }

                if (pageNum == fileHandle.pageCount - 1) {
                    free(pageData);
                    pageData = malloc(PAGE_SIZE);
                    ++pageNum;
                }
            }

            // now that pageNum is determined, must call function to construct new page data
            if (pageNum >= fileHandle.pageCount)
                slotNum = putRecordInEmptyPage(recordDescriptor, data, pageData, recordSpace);
            else
                slotNum = putRecordInNonEmptyPage(recordDescriptor, data, pageData, recordSpace);
        }

        // set the record id, then write back updated page with inserted record
        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        RC writeStatus;
        if (pageNum >= fileHandle.pageCount)
            writeStatus = fileHandle.appendPage(pageData);
        else
            writeStatus = fileHandle.writePage(pageNum, pageData);
        free(pageData);
        return writeStatus;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

