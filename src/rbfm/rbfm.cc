#include "src/include/rbfm.h"
#include <cstring>
#include <iostream>

constexpr PeterDB::SizeType BYTES_FOR_SLOT_DIR_OFFSET = 2;
constexpr PeterDB::SizeType BYTES_FOR_SLOT_DIR_LENGTH = 2;
constexpr PeterDB::SizeType BYTES_FOR_SLOT_DIR_ENTRY = BYTES_FOR_SLOT_DIR_LENGTH + BYTES_FOR_SLOT_DIR_OFFSET;
constexpr PeterDB::SizeType BYTES_FOR_RECORD_FIELD_COUNT = 4;
constexpr int INT_BYTES = 4;
constexpr int BITS_IN_BYTE = 8;
constexpr PeterDB::SizeType BYTES_FOR_POINTER_TO_RECORD_FIELD = 2;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_SLOT_COUNT = 2;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_FREE_SPACE = 2;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_STATS = BYTES_FOR_PAGE_SLOT_COUNT + BYTES_FOR_PAGE_FREE_SPACE;
constexpr PeterDB::SizeType TOMBSTONE_BYTE = 1;
constexpr PeterDB::SizeType BYTES_FOR_PAGE_NUM = 4;
constexpr PeterDB::SizeType BYTES_FOR_SLOT_NUM = 2;
constexpr PeterDB::SizeType MAX_RECORD_SIZE = PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY;

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

    bool RecordBasedFileManager::fitsOnPage(SizeType recordSpace, const void * pageData) {
        // get this page's free space value
        SizeType freeSpace;
        getFreeSpace(&freeSpace, pageData);

        // fitsOnPage is true when there's enough free space, false when there's not even enough space for bare record
        if (recordSpace <= freeSpace) return true;
        if (recordSpace - BYTES_FOR_SLOT_DIR_ENTRY > freeSpace) return false;

        SizeType slotsOnPage;
        getSlotCount(&slotsOnPage, pageData);
        // if a slot can be reused to save slot dir entry bytes, then record will fit on page
        return assignSlot(pageData) <= slotsOnPage;
    }

    SizeType RecordBasedFileManager::calcRecordSpace(const std::vector<Attribute> &recordDescriptor, const void * data) {
        // need bytes for record directory entry, for offset and length
        // need more for number of fields, and byte for tombstone check
        SizeType recordSpace = BYTES_FOR_SLOT_DIR_ENTRY + BYTES_FOR_RECORD_FIELD_COUNT + TOMBSTONE_BYTE;
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
        unsigned numFields = recordDescriptor.size();  // number of fields
        SizeType nullFlagBytes = nullBytesNeeded(numFields);  // number of bytes used for the null flags

        memset(recoPos, 0, TOMBSTONE_BYTE);  // set tombstone byte to zero
        ++recoPos;
        memmove(recoPos, &numFields, BYTES_FOR_RECORD_FIELD_COUNT);  // first put number of fields
        recoPos += BYTES_FOR_RECORD_FIELD_COUNT;
        memmove(recoPos, data, nullFlagBytes);  // get null flag bits
        recoPos += nullFlagBytes;

        SizeType currFieldOffset = TOMBSTONE_BYTE + BYTES_FOR_RECORD_FIELD_COUNT + nullFlagBytes + BYTES_FOR_POINTER_TO_RECORD_FIELD * numFields;  // field offsets go from record start
        long fieldsRemaining = numFields;
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
        // get current free space value and N value
        SizeType freeSpace, N;
        getFreeSpaceAndSlotCount(&freeSpace, &N, pageData);

        // determine the slot number for this new record
        SizeType assignedSlot = assignSlot(pageData);
        // calculate the offset that this new record will have into the page using arithmetic with free space
        SizeType offset = PAGE_SIZE - BYTES_FOR_PAGE_STATS - BYTES_FOR_SLOT_DIR_ENTRY * N - freeSpace;
        // create record directory entry for new record
        SizeType length = recordSpace - BYTES_FOR_SLOT_DIR_ENTRY;
        setSlotOffsetAndLen(&offset, &length, assignedSlot, pageData);

        // update free space and N, check if a new slot is made, or reusing a slot
        if (assignedSlot > N) {
            ++N;
            freeSpace -= recordSpace;
        } else
            freeSpace -= recordSpace - BYTES_FOR_SLOT_DIR_ENTRY;
        setFreeSpaceAndSlotCount(&freeSpace, &N, pageData);

        // record itself is placed into page, return slot number
        embedRecord(offset, recordDescriptor, data, pageData);
        return assignedSlot;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        char pageData[PAGE_SIZE];
        memset(pageData, 0, PAGE_SIZE);
        unsigned pageNum;  // page which record will be inserted to, starts with last page
        SizeType recordSpace = calcRecordSpace(recordDescriptor, data);
        if (recordSpace - BYTES_FOR_SLOT_DIR_ENTRY > MAX_RECORD_SIZE) return -1;  // record will not fit on a page
        SizeType slotNum;

        if (fileHandle.pageCount == 0) {
            pageNum = 0;  // no need to check pages
            slotNum = putRecordInEmptyPage(recordDescriptor, data, pageData, recordSpace);
        } else {
            pageNum = fileHandle.pageCount - 1;
            if (fileHandle.readPage(pageNum, pageData) == -1) return -1;

            // if not enough space, need to start iterating through other pages
            if (!fitsOnPage(recordSpace, pageData)) {
                for (pageNum = 0; pageNum < fileHandle.pageCount - 1; ++pageNum) {
                    if (fileHandle.readPage(pageNum, pageData) == -1) return -1;
                    if (fitsOnPage(recordSpace, pageData)) break;  // stop searching if enough space
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
        return writeStatus;
    }

    RC RecordBasedFileManager::deleteTombstone(FileHandle &fileHandle, char *pageData, unsigned pageNum, unsigned short slotNum, SizeType tombstoneOffset, SizeType tombstoneLen) {
        shiftRecordsLeft(tombstoneOffset + tombstoneLen, tombstoneLen, pageData);
        SizeType zero = 0;
        setSlotOffsetAndLen(&zero, &zero, slotNum, pageData);
        return fileHandle.writePage(pageNum, pageData);
    }

    RC RecordBasedFileManager::findRealRecord(FileHandle &fileHandle, char *pageData, unsigned & pageNum, unsigned short & slotNum, SizeType & recoOffset, SizeType & recoLen, bool removeTombstones) {
        unsigned short oldSlotNum;
        unsigned oldPageNum;
        const char * recoStart;
        unsigned char tombstoneCheck;

        // keeps looping through linked pages until the actual, non-tombstone record is found
        while (true) {
            if (fileHandle.readPage(pageNum, pageData) == -1) return -1;
            // pull offset and length of record from on-page directory
            getSlotOffsetAndLen(&recoOffset, &recoLen, slotNum, pageData);
            // if length is zero, then record doesn't exist
            if (recoLen == 0) return -1;
            recoStart = pageData + recoOffset;
            memmove(&tombstoneCheck, recoStart, TOMBSTONE_BYTE);

            if (tombstoneCheck == 0) break;

            oldSlotNum = slotNum;
            oldPageNum = pageNum;
            memmove(&pageNum, recoStart + TOMBSTONE_BYTE, BYTES_FOR_PAGE_NUM);
            memmove(&slotNum, recoStart + TOMBSTONE_BYTE + BYTES_FOR_PAGE_NUM, BYTES_FOR_SLOT_NUM);
            if (removeTombstones) {
                if (deleteTombstone(fileHandle, pageData, oldPageNum, oldSlotNum, recoOffset, recoLen) == -1) return -1;
            }
        }

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char pageData[PAGE_SIZE];
        unsigned short startingSlot = rid.slotNum;
        unsigned startingPage = rid.pageNum;
        SizeType recoOffset, recoLen;
        if (findRealRecord(fileHandle, pageData, startingPage, startingSlot, recoOffset, recoLen, false) == -1) return -1;
        SizeType bytesFromStart = TOMBSTONE_BYTE + BYTES_FOR_RECORD_FIELD_COUNT;  // start looking after the initial values

        // calculate number of null flag bytes, copy those bytes from the record
        SizeType nullFlagBytes = nullBytesNeeded(recordDescriptor.size());
        memmove(data, pageData + recoOffset + bytesFromStart, nullFlagBytes);
        data = static_cast<char *>(data) + nullFlagBytes;

        // now skip over the null bytes and all the directory bytes
        bytesFromStart += nullFlagBytes + BYTES_FOR_POINTER_TO_RECORD_FIELD * recordDescriptor.size();

        // copy rest of record into data variable
        memmove(data, pageData + recoOffset + bytesFromStart, recoLen - bytesFromStart);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        char pageData[PAGE_SIZE];
        SizeType recoOffset, recoLen;
        unsigned pageNum = rid.pageNum;
        unsigned short slotNum = rid.slotNum;
        if (findRealRecord(fileHandle, pageData, pageNum, slotNum, recoOffset, recoLen, true) == -1) return -1;

        shiftRecordsLeft(recoOffset + recoLen, recoLen, pageData);
        SizeType zero = 0;
        setSlotOffsetAndLen(&zero, &zero, slotNum, pageData);
        RC writeStatus = fileHandle.writePage(pageNum, pageData);
        return writeStatus;
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

                        char val[varcharLen + 1];
                        memmove(val, dataPos, varcharLen);
                        val[varcharLen] = '\0';
                        out << val;
                        dataPos += varcharLen;
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

    void RecordBasedFileManager::shiftRecordsLeft(SizeType shiftPoint, SizeType shiftDistance, void *pageData) {
        SizeType slotCount, freeSpace;
        getFreeSpaceAndSlotCount(&freeSpace, &slotCount, pageData);

        for (SizeType slot = 1, offset; slot <= slotCount; ++slot) {
            getSlotOffset(&offset, slot, pageData);
            if (offset >= shiftPoint) {
                offset -= shiftDistance;
                setSlotOffset(&offset, slot, pageData);
            }
        }

        SizeType bytesToMove = PAGE_SIZE - BYTES_FOR_PAGE_STATS - slotCount * BYTES_FOR_SLOT_DIR_ENTRY - freeSpace - shiftPoint;
        memmove(static_cast<char *>(pageData) + (shiftPoint - shiftDistance), static_cast<const char *>(pageData) + shiftPoint, bytesToMove);
        freeSpace += shiftDistance;
        setFreeSpace(&freeSpace, pageData);
    }

    void RecordBasedFileManager::shiftRecordsRight(SizeType shiftPoint, SizeType shiftDistance, void *pageData) {
        SizeType slotCount, freeSpace;
        getFreeSpaceAndSlotCount(&freeSpace, &slotCount, pageData);

        for (SizeType slot = 1, offset; slot <= slotCount; ++slot) {
            getSlotOffset(&offset, slot, pageData);
            if (offset >= shiftPoint) {
                offset += shiftDistance;
                setSlotOffset(&offset, slot, pageData);
            }
        }

        SizeType bytesToMove = PAGE_SIZE - BYTES_FOR_PAGE_STATS - slotCount * BYTES_FOR_SLOT_DIR_ENTRY - freeSpace - shiftPoint;
        memmove(static_cast<char *>(pageData) + (shiftPoint + shiftDistance), static_cast<const char *>(pageData) + shiftPoint, bytesToMove);
        freeSpace -= shiftDistance;
        setFreeSpace(&freeSpace, pageData);
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        // get length of what new record will be for comparison to current record
        SizeType newRecoLen = calcRecordSpace(recordDescriptor, data) - BYTES_FOR_SLOT_DIR_ENTRY;
        if (newRecoLen > MAX_RECORD_SIZE) return -1;  // updated record cannot fit on a page
        // initialize variables for page, record offset on page, current record's length, slot number it's using
        char pageData[PAGE_SIZE];
        unsigned pageNum = rid.pageNum;
        unsigned short slotNum = rid.slotNum;
        SizeType recoOffset, recoLen;
        // searches through any existing tombstones to find actual record, gets its offset, length, and slot on its page
        if (findRealRecord(fileHandle, pageData, pageNum, slotNum, recoOffset, recoLen, false) == -1) return -1;
        SizeType freeSpace;
        getFreeSpace(&freeSpace, pageData);

        if (newRecoLen < recoLen) {
            // updated record will be shorter, so shift page's records to the left
            shiftRecordsLeft(recoOffset + recoLen, recoLen - newRecoLen, pageData);
            embedRecord(recoOffset, recordDescriptor, data, pageData);
        } else if (newRecoLen > recoLen) {
            SizeType diff = newRecoLen - recoLen;
            if (diff <= freeSpace) {
                // if there is enough free space, shift records over and put in updated record
                shiftRecordsRight(recoOffset + recoLen, diff, pageData);
                embedRecord(recoOffset, recordDescriptor, data, pageData);
            } else {
                // if not enough space, make this record a tombstone then put updated record on new page
                RID newRid;
                if (insertRecord(fileHandle, recordDescriptor, data, newRid) == -1) return -1;
                memset(pageData + recoOffset, 1, TOMBSTONE_BYTE);
                memmove(pageData + (recoOffset + TOMBSTONE_BYTE), &newRid.pageNum, BYTES_FOR_PAGE_NUM);
                memmove(pageData + (recoOffset + TOMBSTONE_BYTE + BYTES_FOR_PAGE_NUM), &newRid.slotNum, BYTES_FOR_SLOT_NUM);
                newRecoLen = recoLen;
            }
        } else
            embedRecord(recoOffset, recordDescriptor, data, pageData);

        setSlotLen(&newRecoLen, slotNum, pageData);
        RC writeStatus = fileHandle.writePage(pageNum, pageData);
        return writeStatus;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        char pageData[PAGE_SIZE];
        unsigned short startingSlot = rid.slotNum;
        unsigned startingPage = rid.pageNum;
        SizeType recoOffset, recoLen;
        if (findRealRecord(fileHandle, pageData, startingPage, startingSlot, recoOffset, recoLen, false) == -1) return -1;

        int attrIndex;
        AttrType attrType;
        AttrLength attrLen;

        for (attrIndex = 0; attrIndex < recordDescriptor.size(); ++attrIndex) {
            if (attributeName == recordDescriptor[attrIndex].name) {
                attrType = recordDescriptor[attrIndex].type;
                attrLen = recordDescriptor[attrIndex].length;
                break;
            }
        }
        if (attrIndex == recordDescriptor.size()) return -1;

        SizeType nullFlagBytes = nullBytesNeeded(recordDescriptor.size());
        unsigned char nullByte;
        memmove(&nullByte, pageData + (recoOffset + TOMBSTONE_BYTE + BYTES_FOR_RECORD_FIELD_COUNT + nullFlagBytes / BITS_IN_BYTE), 1);

        if (nullBitOn(nullByte, nullFlagBytes % BITS_IN_BYTE)) {
            memset(data, 1, 1);
            return 0;
        }
        memset(data, 0, 1);
        data = static_cast<char *>(data) + 1;

        SizeType attrOffset;
        memmove(&attrOffset, pageData + (recoOffset + TOMBSTONE_BYTE + BYTES_FOR_RECORD_FIELD_COUNT + nullFlagBytes + BYTES_FOR_POINTER_TO_RECORD_FIELD * attrIndex), BYTES_FOR_POINTER_TO_RECORD_FIELD);
        char * attrLocation = pageData + (recoOffset + attrOffset);

        if (attrType == TypeVarChar) {
            int varcharLen = 0;
            memmove(&varcharLen, attrLocation, INT_BYTES);
            int fieldBytes = INT_BYTES + varcharLen;
            memmove(data, attrLocation, fieldBytes);
        } else
            memmove(data, attrLocation, attrLen);

        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.init(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
        return 0;
    }

    void RBFM_ScanIterator::init(FileHandle & fHandle, const std::vector<Attribute> &recordDescriptor,
                                 const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                 const std::vector<std::string> &attributeNames) {
        // initialize main variables
        fileHandle = fHandle;
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->attributeNames = attributeNames;
        firstScan = true;

        if (compOp == NO_OP) return;  // if no operation, comparison value is irrelevant
        // get the type of the comparison value
        for (auto attr : recordDescriptor) {
            if (attr.name == conditionAttribute) {
                valueType = attr.type;
                conditionAttrLen = attr.length;
                break;
            }
        }

        // if value is integer or real, simply copy over the bytes from value array
        if (valueType == TypeInt)
            memmove(&valueInt, value, INT_BYTES);
        else if (valueType == TypeReal)
            memmove(&valueReal, value, INT_BYTES);
        else {
            // if value is varchar, get length of the character section, append null character, then convert into string object
            int varcharLen;
            memmove(&varcharLen, value, INT_BYTES);
            char valString[varcharLen + 1];
            memmove(valString, static_cast<const char *>(value) + INT_BYTES, varcharLen);
            valString[varcharLen] = '\0';
            valueString = std::string{valString};
        }
    }

    RC RBFM_ScanIterator::close() {
        return RecordBasedFileManager::instance().closeFile(fileHandle);
    }

    bool RBFM_ScanIterator::compareInt(int conditionAttr) {
        switch (compOp) {
            case EQ_OP:
                return conditionAttr == valueInt;
            case NE_OP:
                return conditionAttr != valueInt;
            case LT_OP:
                return conditionAttr < valueInt;
            case GT_OP:
                return conditionAttr > valueInt;
            case LE_OP:
                return conditionAttr <= valueInt;
            case GE_OP:
                return conditionAttr >= valueInt;
            default:
                return false;
        }
    }

    bool RBFM_ScanIterator::compareReal(float conditionAttr) {
        switch (compOp) {
            case EQ_OP:
                return conditionAttr == valueReal;
            case NE_OP:
                return conditionAttr != valueReal;
            case LT_OP:
                return conditionAttr < valueReal;
            case GT_OP:
                return conditionAttr > valueReal;
            case LE_OP:
                return conditionAttr <= valueReal;
            case GE_OP:
                return conditionAttr >= valueReal;
            default:
                return false;
        }
    }

    bool RBFM_ScanIterator::compareVarchar(const std::string & conditionAttr) {
        switch (compOp) {
            case EQ_OP:
                return conditionAttr == valueString;
            case NE_OP:
                return conditionAttr != valueString;
            case LT_OP:
                return conditionAttr < valueString;
            case GT_OP:
                return conditionAttr > valueString;
            case LE_OP:
                return conditionAttr <= valueString;
            case GE_OP:
                return conditionAttr >= valueString;
            default:
                return false;
        }
    }

    bool RBFM_ScanIterator::acceptedRecord(const char * recordData, unsigned pageNum, unsigned short slotNum) {
        if (compOp == NO_OP) return true;
        char conditionAttrVal[conditionAttrLen + INT_BYTES + 1];
        RID recordRid{pageNum, slotNum};
        if (RecordBasedFileManager::instance().readAttribute(fileHandle, recordDescriptor, recordRid, conditionAttribute, conditionAttrVal) == -1) return false;

        unsigned char nullByte;
        memmove(&nullByte, conditionAttrVal, 1);
        if (nullByte == 1) return false;

        if (valueType == TypeInt) {
            int attrVal;
            memmove(&attrVal, conditionAttrVal + 1, INT_BYTES);
            return compareInt(attrVal);
        } else if (valueType == TypeReal) {
            float attrVal;
            memmove(&attrVal, conditionAttrVal + 1, INT_BYTES);
            return compareReal(attrVal);
        } else {
            // if value is varchar, get length of the character section, append null character, then convert into string object
            int varcharLen;
            memmove(&varcharLen, conditionAttrVal + 1, INT_BYTES);
            char valString[varcharLen + 1];
            memmove(valString, conditionAttrVal + INT_BYTES, varcharLen);
            valString[varcharLen] = '\0';
            std::string attrVal{valString};

            return compareVarchar(attrVal);
        }
    }

    void RBFM_ScanIterator::extractRecordData(const char * recordData, void * data) {
        const char *recordNullsPtr = recordData + TOMBSTONE_BYTE + BYTES_FOR_RECORD_FIELD_COUNT;
        const char *recordDirPtr = recordNullsPtr + RecordBasedFileManager::instance().nullBytesNeeded(recordDescriptor.size());
        int attrNamesIndex = 0;
        unsigned char nullByte;
        SizeType newNullByteCount = RecordBasedFileManager::instance().nullBytesNeeded(attributeNames.size());
        unsigned char newNullBytes[newNullByteCount] = {0};
        char *dataPtr = static_cast<char *>(data) + newNullByteCount;
        SizeType attrOffset;

        for (int recordDescIndex = 0; recordDescIndex < recordDescriptor.size() && attrNamesIndex < attributeNames.size(); ++recordDescIndex) {
            if (recordDescIndex % BITS_IN_BYTE == 0) {
                memmove(&nullByte, recordNullsPtr, 1);
                ++recordNullsPtr;
            }

            if (recordDescriptor[recordDescIndex].name == attributeNames[attrNamesIndex]) {
                if (RecordBasedFileManager::instance().nullBitOn(nullByte, recordDescIndex % BITS_IN_BYTE + 1))
                    newNullBytes[attrNamesIndex / BITS_IN_BYTE] |= 1 << (BITS_IN_BYTE - attrNamesIndex % BITS_IN_BYTE - 1);
                else {
                    memmove(&attrOffset, recordDirPtr + BYTES_FOR_POINTER_TO_RECORD_FIELD * recordDescIndex, BYTES_FOR_POINTER_TO_RECORD_FIELD);

                    if (recordDescriptor[recordDescIndex].type != TypeVarChar) {
                        memmove(dataPtr, recordData + attrOffset, recordDescriptor[recordDescIndex].length);
                        dataPtr += recordDescriptor[recordDescIndex].length;
                    } else {
                        // must get length value from varchar field to know needed space
                        int varcharLen;
                        memmove(&varcharLen, recordData + attrOffset, INT_BYTES);
                        memmove(dataPtr, recordData + attrOffset, varcharLen + INT_BYTES);
                        dataPtr += varcharLen + INT_BYTES;
                    }
                }
                ++attrNamesIndex;
            }
        }

        memmove(data, newNullBytes, newNullByteCount);
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        unsigned currPageNum;
        unsigned short currSlotNum;
        if (firstScan) {
            // start initial scan at zeroth page, very first slot
            currPageNum = 0;
            currSlotNum = 1;
            firstScan = false;
        } else {
            // will iterate from the most recently used rid
            currPageNum = rid.pageNum;
            currSlotNum = rid.slotNum;
        }

        char pageData[PAGE_SIZE];
        SizeType currSlotCount;
        SizeType recoOffset, recoLen;
        unsigned char tombstoneCheck;

        for (; currPageNum < fileHandle.pageCount; ++currPageNum, currSlotNum = 0) {
            if (fileHandle.readPage(currPageNum, pageData) == -1) return -1;
            RecordBasedFileManager::instance().getSlotCount(&currSlotCount, pageData);

            for (; currSlotNum <= currSlotCount; ++currSlotNum) {
                RecordBasedFileManager::instance().getSlotOffsetAndLen(&recoOffset, &recoLen, currSlotNum, pageData);
                if (recoLen == 0) continue;  // need to skip over unused empty slots
                memmove(&tombstoneCheck, pageData + recoOffset, TOMBSTONE_BYTE);
                if (tombstoneCheck == 1) continue;  // don't want to scan over tombstones, just real records

                if (acceptedRecord(pageData + recoOffset, currPageNum, currSlotNum)) {
                    extractRecordData(pageData + recoOffset, data);
                    rid.pageNum = currPageNum;
                    rid.slotNum = currSlotNum;
                    return 0;
                }
            }
        }

        return RBFM_EOF;
    }

} // namespace PeterDB

