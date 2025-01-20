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
        // need 2 more for number of fields
        unsigned short recordSpace = 6;
        unsigned numFields = recordDescriptor.size();
        unsigned nullFlagBytes = numFields / 8;
        if (nullFlagBytes % 8 != 0) ++nullFlagBytes;
        recordSpace += nullFlagBytes;

        // each non-null field needs 2 bytes for offset pointer, plus data type size
        unsigned fieldsRemaining = numFields;
        unsigned dataPos = nullFlagBytes;
        for (unsigned i = 0, flagByte = 0; i < nullFlagBytes; ++i, flagByte = 0, fieldsRemaining -= 8) {
            // get set of 8 null flag bits
            memcpy(&flagByte, (const char *)data + i, 1);

            for (unsigned j = 0; j < 8 && j < fieldsRemaining; ++j, recordSpace += 2)
                if (((flagByte >> (7 - j)) & 1) == 0) {
                    // null bit flag of zero means space is needed
                    Attribute attr{recordDescriptor[i * 8 + j]};
                    if (attr.type != TypeVarChar) {
                        recordSpace += attr.length;
                        dataPos += attr.length;
                    } else {
                        // must get 4-byte length value from varchar field to know needed space
                        unsigned varcharLen = 0;
                        memcpy(&varcharLen, (const char *)data + dataPos, 4);
                        recordSpace += 4 + varcharLen;
                        dataPos += 4 + varcharLen;
                    }
                }
        }

        return recordSpace;
    }

    void RecordBasedFileManager::embedRecord(unsigned short offset, const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData) {
        char * recoStart = (char *)pageData + offset;
        char * recoPos = recoStart;  // starting position of record entry in pageData
        unsigned nullFlagBytes = recordDescriptor.size() / 8;
        if (nullFlagBytes % 8 != 0) ++nullFlagBytes;  // number of bytes used for the null flags
        unsigned short numFields = recordDescriptor.size();  // number of fields

        memcpy(recoPos, &numFields, 2);  // first put number of fields
        recoPos += 2;
        memcpy(recoPos, data, nullFlagBytes);  // get null flag bits
        recoPos += nullFlagBytes;

        unsigned short currFieldOffset = 2 + nullFlagBytes + 2 * numFields;  // field offsets go from record start
        unsigned fieldsRemaining = recordDescriptor.size();
        unsigned short dataPos = nullFlagBytes;  // this keeps track of how far into data byte stream to be
        for (unsigned i = 0, flagByte = 0; i < nullFlagBytes; ++i, flagByte = 0, fieldsRemaining -= 8) {
            // get set of 8 null flag bits
            memcpy(&flagByte, (const char *)data + i, 1);

            for (unsigned j = 0; j < 8 && j < fieldsRemaining; ++j) {
                memcpy(recoPos, &currFieldOffset, 2);  // set directory pointer for field
                recoPos += 2;

                if (((flagByte >> (7 - j)) & 1) == 0) {
                    // null bit flag of zero means field length must be considered
                    Attribute attr{recordDescriptor[i * 8 + j]};
                    if (attr.type != TypeVarChar) {
                        // copy field data to the record byte stream at current pointer offset
                        memcpy(recoStart + currFieldOffset, (const char *)data + dataPos, attr.length);
                        currFieldOffset += attr.length;
                        dataPos += attr.length;
                    } else {
                        // must get 4-byte length value from varchar field to know needed space
                        unsigned varcharLen = 0;
                        memcpy(&varcharLen, (const char *)data + dataPos, 4);
                        // copy field data to the record byte stream at current pointer offset
                        memcpy(recoStart + currFieldOffset, (const char *)data + dataPos, 4 + varcharLen);
                        currFieldOffset += 4 + varcharLen;
                        dataPos += 4 + varcharLen;
                    }
                }
            }
        }
    }

    unsigned short RecordBasedFileManager::putRecordInEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, unsigned short recordSpace) {
        unsigned short initFreeSpace = PAGE_SIZE - recordSpace - 4;  // 4 bytes, 2 for N value, 2 for F value
        char * firstByte = (char *)pageData;
        memcpy(firstByte + (PAGE_SIZE - 2), &initFreeSpace, 2);  // adds F value for free space
        unsigned short N = 1;
        memcpy(firstByte + (PAGE_SIZE - 4), &N, 2);  // adds N value for number of records

        // create record directory entry for new record
        unsigned short offset = 0;
        unsigned short length = recordSpace - 4;
        memcpy(firstByte + (PAGE_SIZE - 6), &length, 2);
        memcpy(firstByte + (PAGE_SIZE - 8), &offset, 2);

        // record itself is placed into page
        embedRecord(offset, recordDescriptor, data, pageData);
        return 1;
    }

    unsigned short RecordBasedFileManager::putRecordInNonEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, unsigned short recordSpace) {
        char * firstByte = (char *)pageData;

        // get current free space value, update it, then write it back
        unsigned short freeSpace;
        memcpy(&freeSpace, firstByte + (PAGE_SIZE - 2), 2);
        freeSpace -= recordSpace;
        memcpy(firstByte + (PAGE_SIZE - 2), &freeSpace, 2);  // adds F value for free space

        // get current N value, increase by 1, write it back
        unsigned short N;
        memcpy(&N, firstByte + (PAGE_SIZE - 4), 2);
        ++N;
        memcpy(firstByte + (PAGE_SIZE - 4), &N, 2);

        // calculate the offset that this new record will have into the page using last record's values
        unsigned short offset;
        if (N == 1)
            offset = 0;
        else {
            memcpy(&offset, firstByte + (PAGE_SIZE - 4 - 4 * (N - 1)), 2);
            unsigned short len;
            memcpy(&len, firstByte + (PAGE_SIZE - 4 - 4 * (N - 1) + 2), 2);
            offset += len;
        }

        // create record directory entry for new record
        unsigned short length = recordSpace - 4;
        memcpy(firstByte + (PAGE_SIZE - 4 - 4 * N + 2), &length, 2);
        memcpy(firstByte + (PAGE_SIZE - 4 - 4 * N), &offset, 2);

        // record itself is placed into page, N is the returned slot number
        embedRecord(offset, recordDescriptor, data, pageData);
        return N;
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
        void * pageData = malloc(PAGE_SIZE);
        if (fileHandle.readPage(rid.pageNum, pageData) == -1) return -1;

        // pull offset and length of record from on-page directory
        unsigned short recoOffset;
        memcpy(&recoOffset, (const char *)pageData + (PAGE_SIZE - 4 - 4 * rid.slotNum), 2);
        unsigned short recoLen;
        memcpy(&recoLen, (const char *)pageData + (PAGE_SIZE - 4 - 4 * rid.slotNum + 2), 2);

        const char * recoStart = (const char *)pageData + recoOffset;
        unsigned short bytesFromStart = 2;  // start looking after the initial 2-byte len number

        // calculate number of null flag bytes, copy those bytes from the record
        unsigned nullFlagBytes = recordDescriptor.size() / 8;
        if (nullFlagBytes % 8 != 0) ++nullFlagBytes;
        memcpy(data, recoStart + bytesFromStart, nullFlagBytes);
        data = (char *)data + nullFlagBytes;
        // now skip over the null bytes and all the directory bytes
        bytesFromStart += nullFlagBytes + 2 * recordDescriptor.size();

        // copy rest of record into data variable
        memcpy(data, recoStart + bytesFromStart, recoLen - bytesFromStart);
        return 0;
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

