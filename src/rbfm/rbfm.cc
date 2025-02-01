#include "src/include/rbfm.h"
#include <cstring>
#include <iostream>

constexpr PeterDB::SizeType BYTES_FOR_SLOT_DIR_OFFSET = 2;
constexpr PeterDB::SizeType BYTES_FOR_SLOT_DIR_LENGTH = 2;
constexpr PeterDB::SizeType BYTES_FOR_SLOT_DIR_ENTRY = BYTES_FOR_SLOT_DIR_LENGTH + BYTES_FOR_SLOT_DIR_OFFSET;
constexpr PeterDB::SizeType BYTES_FOR_RECORD_FIELD_COUNT = 2;
constexpr int INT_BYTES = 4;
constexpr int BITS_IN_BYTE = 8;
constexpr PeterDB::SizeType BYTES_FOR_POINTER_TO_RECORD_FIELD = 2;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_SLOT_COUNT = 2;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_FREE_SPACE = 2;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_STATS = BYTES_FOR_PAGE_SLOT_COUNT + BYTES_FOR_PAGE_FREE_SPACE;


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

    SizeType RecordBasedFileManager::nullBytesNeeded(SizeType numFields) {
        SizeType nullBytes = numFields / BITS_IN_BYTE;
        return (numFields % BITS_IN_BYTE == 0) ? nullBytes : nullBytes + 1;
    }

    bool RecordBasedFileManager::nullBitOn(unsigned char nullByte, int bitNum) {
        // bitNum starts from 1
        return (1 & (nullByte >> (BITS_IN_BYTE - bitNum))) == 1;
    }

    void RecordBasedFileManager::getFreeSpace(SizeType * freeSpace, const void * pageData) {
        memmove(freeSpace, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_FREE_SPACE), BYTES_FOR_PAGE_FREE_SPACE);
    }

    void RecordBasedFileManager::setFreeSpace(SizeType * freeSpace, void * pageData) {
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_FREE_SPACE), freeSpace, BYTES_FOR_PAGE_FREE_SPACE);
    }

    void RecordBasedFileManager::getSlotCount(SizeType * slotCount, const void * pageData) {
        memmove(slotCount, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS), BYTES_FOR_PAGE_SLOT_COUNT);
    }

    void RecordBasedFileManager::setSlotCount(SizeType * slotCount, void * pageData) {
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS), slotCount, BYTES_FOR_PAGE_SLOT_COUNT);
    }

    void RecordBasedFileManager::getFreeSpaceAndSlotCount(SizeType * freeSpace, SizeType * slotCount, const void * pageData) {
        memmove(freeSpace, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_FREE_SPACE), BYTES_FOR_PAGE_FREE_SPACE);
        memmove(slotCount, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS), BYTES_FOR_PAGE_SLOT_COUNT);
    }

    void RecordBasedFileManager::setFreeSpaceAndSlotCount(SizeType * freeSpace, SizeType * slotCount, void * pageData) {
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_FREE_SPACE), freeSpace, BYTES_FOR_PAGE_FREE_SPACE);
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS), slotCount, BYTES_FOR_PAGE_SLOT_COUNT);
    }

    void RecordBasedFileManager::getSlotOffset(SizeType * offset, SizeType slotNum, const void * pageData) {
        memmove(offset, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum), BYTES_FOR_SLOT_DIR_OFFSET);
    }

    void RecordBasedFileManager::setSlotOffset(SizeType * offset, SizeType slotNum, void * pageData) {
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum), offset, BYTES_FOR_SLOT_DIR_OFFSET);
    }

    void RecordBasedFileManager::getSlotLen(SizeType * len, SizeType slotNum, const void * pageData) {
        memmove(len, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum + BYTES_FOR_SLOT_DIR_OFFSET), BYTES_FOR_SLOT_DIR_LENGTH);
    }

    void RecordBasedFileManager::setSlotLen(SizeType * len, SizeType slotNum, void * pageData) {
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum + BYTES_FOR_SLOT_DIR_OFFSET), len, BYTES_FOR_SLOT_DIR_LENGTH);
    }

    void RecordBasedFileManager::getSlotOffsetAndLen(SizeType * offset, SizeType * len, SizeType slotNum, const void * pageData) {
        memmove(offset, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum), BYTES_FOR_SLOT_DIR_OFFSET);
        memmove(len, static_cast<const char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum + BYTES_FOR_SLOT_DIR_OFFSET), BYTES_FOR_SLOT_DIR_LENGTH);
    }

    void RecordBasedFileManager::setSlotOffsetAndLen(SizeType * offset, SizeType * len, SizeType slotNum, void * pageData) {
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum), offset, BYTES_FOR_SLOT_DIR_OFFSET);
        memmove(static_cast<char *>(pageData) + (PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * slotNum + BYTES_FOR_SLOT_DIR_OFFSET), len, BYTES_FOR_SLOT_DIR_LENGTH);
    }

    SizeType RecordBasedFileManager::assignSlot(const void * pageData) {
        SizeType slotCount;
        getSlotCount(&slotCount, pageData);

        for (SizeType slot = 1, len; slot <= slotCount; ++slot) {
            getSlotLen(&len, slot, pageData);
            if (len == 0) return slot;
        }

        return slotCount + 1;
    }

    SizeType RecordBasedFileManager::calcRecordSpace(const std::vector<Attribute> &recordDescriptor, const void * data) {
        // need bytes for record directory entry, for offset and length
        // need more for number of fields
        SizeType recordSpace = BYTES_FOR_SLOT_DIR_ENTRY + BYTES_FOR_RECORD_FIELD_COUNT;
        SizeType nullFlagBytes = nullBytesNeeded(recordDescriptor.size());
        recordSpace += nullFlagBytes;

        // each non-null field needs 2 bytes for offset pointer, plus data type size
        int fieldsRemaining = recordDescriptor.size();
        SizeType dataPos = nullFlagBytes;
        unsigned char flagByte;
        for (SizeType i = 0; i < nullFlagBytes; ++i, fieldsRemaining -= BITS_IN_BYTE) {
            // get a set of null flag bits
            memmove(&flagByte, static_cast<const char *>(data) + i, 1);

            for (int j = 1; j <= BITS_IN_BYTE && j <= fieldsRemaining; ++j, recordSpace += BYTES_FOR_POINTER_TO_RECORD_FIELD)
                if (!nullBitOn(flagByte, j)) {
                    // non-null attribute value means space is needed
                    Attribute attr{recordDescriptor[i * BITS_IN_BYTE + j - 1]};
                    if (attr.type != TypeVarChar) {
                        recordSpace += attr.length;
                        dataPos += attr.length;
                    } else {
                        // must get int sized length value from varchar field to know needed space
                        int varcharLen = 0;
                        memmove(&varcharLen, static_cast<const char *>(data) + dataPos, INT_BYTES);
                        recordSpace += INT_BYTES + varcharLen;
                        dataPos += INT_BYTES + varcharLen;
                    }
                }
        }

        return recordSpace;
    }

    void RecordBasedFileManager::embedRecord(SizeType offset, const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData) {
        char * recoStart = static_cast<char *>(pageData) + offset;
        char * recoPos = recoStart;  // starting position of record entry in pageData
        SizeType numFields = recordDescriptor.size();  // number of fields
        SizeType nullFlagBytes = nullBytesNeeded(numFields);  // number of bytes used for the null flags

        memmove(recoPos, &numFields, BYTES_FOR_RECORD_FIELD_COUNT);  // first put number of fields
        recoPos += BYTES_FOR_RECORD_FIELD_COUNT;
        memmove(recoPos, data, nullFlagBytes);  // get null flag bits
        recoPos += nullFlagBytes;

        SizeType currFieldOffset = BYTES_FOR_RECORD_FIELD_COUNT + nullFlagBytes + BYTES_FOR_POINTER_TO_RECORD_FIELD * numFields;  // field offsets go from record start
        int fieldsRemaining = numFields;
        SizeType dataPos = nullFlagBytes;  // this keeps track of how far into data byte stream to be
        unsigned char flagByte;
        for (SizeType i = 0; i < nullFlagBytes; ++i, fieldsRemaining -= BITS_IN_BYTE) {
            // get byte of null flag bits
            memmove(&flagByte, static_cast<const char *>(data) + i, 1);

            for (int j = 1; j <= BITS_IN_BYTE && j <= fieldsRemaining; ++j) {
                memmove(recoPos, &currFieldOffset, BYTES_FOR_POINTER_TO_RECORD_FIELD);  // set directory pointer for field
                recoPos += BYTES_FOR_POINTER_TO_RECORD_FIELD;

                if (!nullBitOn(flagByte, j)) {
                    // non-null attribute value means field length must be considered
                    Attribute attr{recordDescriptor[i * BITS_IN_BYTE + j - 1]};
                    if (attr.type != TypeVarChar) {
                        // copy field data to the record byte stream at current pointer offset
                        memmove(recoStart + currFieldOffset, static_cast<const char *>(data) + dataPos, attr.length);
                        currFieldOffset += attr.length;
                        dataPos += attr.length;
                    } else {
                        // must get int sized length value from varchar field to know needed space
                        int varcharLen = 0;
                        memmove(&varcharLen, static_cast<const char *>(data) + dataPos, INT_BYTES);
                        // copy field data to the record byte stream at current pointer offset
                        int fieldBytes = INT_BYTES + varcharLen;
                        memmove(recoStart + currFieldOffset, static_cast<const char *>(data) + dataPos, fieldBytes);
                        currFieldOffset += fieldBytes;
                        dataPos += fieldBytes;
                    }
                }
            }
        }
    }

    SizeType RecordBasedFileManager::putRecordInEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, SizeType recordSpace) {
        SizeType initFreeSpace = PAGE_SIZE - recordSpace - BYTES_FOR_PAGE_STATS;  // bytes for N value and F value
        SizeType N = 1;
        setFreeSpaceAndSlotCount(&initFreeSpace, &N, pageData);  // adds N value for number of records, F value for free space

        // create record directory entry for new record
        SizeType offset = 0;
        SizeType length = recordSpace - BYTES_FOR_SLOT_DIR_ENTRY;
        setSlotOffsetAndLen(&offset, &length, 1, pageData);

        // record itself is placed into page
        embedRecord(offset, recordDescriptor, data, pageData);
        return 1;
    }

    SizeType RecordBasedFileManager::putRecordInNonEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, SizeType recordSpace) {
        // get current free space value, update it, then write it back
        SizeType freeSpace;
        getFreeSpace(&freeSpace, pageData);
        freeSpace -= recordSpace;
        setFreeSpace(&freeSpace, pageData);  // adds F value for free space

        // get current N value, increase by 1, write it back
        SizeType N;
        getSlotCount(&N, pageData);
        ++N;
        setSlotCount(&N, pageData);

        // calculate the offset that this new record will have into the page using last record's values
        SizeType offset;
        if (N == 1)
            offset = 0;
        else {
            SizeType len;
            getSlotOffsetAndLen(&offset, &len, N - 1, pageData);
            offset += len;
        }

        // create record directory entry for new record
        SizeType length = recordSpace - BYTES_FOR_SLOT_DIR_ENTRY;
        setSlotOffsetAndLen(&offset, &length, N, pageData);

        // record itself is placed into page, N is the returned slot number
        embedRecord(offset, recordDescriptor, data, pageData);
        return N;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        char * pageData = new char[PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        unsigned pageNum;  // page which record will be inserted to, starts with last page
        SizeType recordSpace = calcRecordSpace(recordDescriptor, data);
        SizeType slotNum;

        if (fileHandle.pageCount == 0) {
            pageNum = 0;  // no need to check pages
            slotNum = putRecordInEmptyPage(recordDescriptor, data, pageData, recordSpace);
        } else {
            SizeType freeSpace;
            pageNum = fileHandle.pageCount - 1;
            if (fileHandle.readPage(pageNum, pageData) == -1) {delete[] pageData; return -1;}
            getFreeSpace(&freeSpace, pageData);

            // if not enough space, need to start iterating through other pages
            if (recordSpace > freeSpace) {
                for (pageNum = 0; pageNum < fileHandle.pageCount - 1; ++pageNum) {
                    if (fileHandle.readPage(pageNum, pageData) == -1) {delete[] pageData; return -1;}
                    getFreeSpace(&freeSpace, pageData);
                    if (recordSpace <= freeSpace) break;  // stop searching if enough space
                }

                if (pageNum == fileHandle.pageCount - 1) {
                    memset(pageData, 0, PAGE_SIZE);
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
        delete[] pageData;
        return writeStatus;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char * pageData = new char[PAGE_SIZE];
        if (fileHandle.readPage(rid.pageNum, pageData) == -1) {delete[] pageData; return -1;}

        // pull offset and length of record from on-page directory
        SizeType recoOffset, recoLen;
        getSlotOffsetAndLen(&recoOffset, &recoLen, rid.slotNum, pageData);

        const char * recoStart = pageData + recoOffset;
        SizeType bytesFromStart = BYTES_FOR_RECORD_FIELD_COUNT;  // start looking after the initial 2-byte len number

        // calculate number of null flag bytes, copy those bytes from the record
        SizeType nullFlagBytes = nullBytesNeeded(recordDescriptor.size());
        memmove(data, recoStart + bytesFromStart, nullFlagBytes);
        data = static_cast<char *>(data) + nullFlagBytes;

        // now skip over the null bytes and all the directory bytes
        bytesFromStart += nullFlagBytes + BYTES_FOR_POINTER_TO_RECORD_FIELD * recordDescriptor.size();

        // copy rest of record into data variable
        memmove(data, recoStart + bytesFromStart, recoLen - bytesFromStart);
        delete[] pageData;
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        // calculate number of bytes for null flags
        SizeType nullFlagBytes = nullBytesNeeded(recordDescriptor.size());

        // skip over null flag bytes to get to useful data
        const char * dataPos = static_cast<const char *>(data) + nullFlagBytes;
        int fieldsRemaining = recordDescriptor.size();

        // attribute variables for saving attribute information in the loops
        Attribute attr;
        SizeType attrNum = 0;

        unsigned char flagByte;
        for (SizeType i = 0; i < nullFlagBytes; ++i, fieldsRemaining -= BITS_IN_BYTE) {
            memmove(&flagByte, static_cast<const char *>(data) + i, 1);

            for (int j = 1; j <= BITS_IN_BYTE && j <= fieldsRemaining; ++j) {
                attrNum = i * BITS_IN_BYTE + j - 1;
                attr = recordDescriptor[attrNum];
                out << attr.name << ": ";

                if (!nullBitOn(flagByte, j)) {
                    // null bit flag of zero means non-null
                    if (attr.type == TypeInt) {
                        int val;
                        memmove(&val, dataPos, attr.length);
                        out << val;
                        dataPos += attr.length;
                    } else if (attr.type == TypeReal) {
                        float val;
                        memmove(&val, dataPos, attr.length);
                        out << val;
                        dataPos += attr.length;
                    } else {
                        // must get length value from varchar field to know needed space
                        int varcharLen = 0;
                        memmove(&varcharLen, dataPos, INT_BYTES);
                        dataPos += INT_BYTES;

                        char * val = new char[varcharLen + 1];
                        memmove(val, dataPos, varcharLen);
                        val[varcharLen] = '\0';
                        out << val;
                        dataPos += varcharLen;
                        delete[] val;
                    }
                } else {
                    // null value for attribute
                    out << "NULL";
                }

                if (attrNum >= recordDescriptor.size() - 1)
                    out << '\n';
                else
                    out << ", ";
            }
        }

        return 0;
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

