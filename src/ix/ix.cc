#include "src/include/ix.h"
#include <cstring>
#include <iostream>

constexpr unsigned short SLOT_COUNT_BYTES = sizeof(PeterDB::SizeType);
constexpr unsigned short PAGE_POINTER_BYTES = sizeof(unsigned);
constexpr unsigned short PAGE_NUM_BYTES = sizeof(unsigned);
constexpr unsigned short SLOT_BYTES = sizeof(unsigned short);
constexpr unsigned short RID_BYTES = PAGE_NUM_BYTES + SLOT_BYTES;
constexpr unsigned short LEAF_CHECK_BYTE = 1;
constexpr unsigned short LEAF_BYTES_BEFORE_KEYS = LEAF_CHECK_BYTE + 2 * PAGE_POINTER_BYTES + SLOT_COUNT_BYTES;
constexpr unsigned short PARENT_BYTES_BEFORE_KEYS = LEAF_CHECK_BYTE + PAGE_POINTER_BYTES + SLOT_COUNT_BYTES + PAGE_POINTER_BYTES;
constexpr unsigned short ROOT_BYTES_BEFORE_KEYS = SLOT_COUNT_BYTES + PAGE_POINTER_BYTES;
constexpr unsigned short INT_BYTES = sizeof(int);


namespace PeterDB {
    RC IXFileHandle::initFileHandle(const std::string &fileName) {
        if (FileHandle::initFileHandle(fileName) == -1) return -1;
        indexMaxPageNodes = 0;
        return 0;
    }

    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager;
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().openFile(fileName, ixFileHandle);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return PagedFileManager::instance().closeFile(ixFileHandle);
    }

    SizeType IndexManager::nodeEntrySize(const Attribute & attr, bool isLeafPage) {
        SizeType entrySize = attr.length + RID_BYTES;
        if (attr.type == TypeVarChar) entrySize += INT_BYTES;
        if (!isLeafPage) entrySize += PAGE_POINTER_BYTES;
        return entrySize;
    }

    SizeType IndexManager::maxNodeSlots(const Attribute & attr) {
        // max index entries on a node determined by parent node layout
        return (PAGE_SIZE - PARENT_BYTES_BEFORE_KEYS) / nodeEntrySize(attr, false);
    }

    SizeType IndexManager::determineSlot(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, SizeType entrySize, SizeType slotCount, int typeOfSearch) {
        char *slotLoc;
        SizeType low = 1, high = slotCount, mid = 1;

        if (attr.type == TypeInt) {
            Key<int> iKey{*static_cast<const int *>(key), rid};

            while (low <= high) {
                mid = (low + high) / 2;
                slotLoc = pagePtr + (mid - 1) * entrySize;
                RID entryRID{*reinterpret_cast<unsigned *>(slotLoc + attr.length), *reinterpret_cast<unsigned short *>(slotLoc + attr.length + PAGE_NUM_BYTES)};
                Key<int> slotKey{*reinterpret_cast<int *>(slotLoc), entryRID};

                if (iKey < slotKey)
                    high = typeOfSearch == 3 ? --mid : mid - 1;
                else if (iKey == slotKey)
                    return mid;
                else
                    low = typeOfSearch == 2 ? ++mid : mid + 1;
            }
        } else if (attr.type == TypeReal) {
            Key<float> fKey{*static_cast<const float *>(key), rid};

            while (low <= high) {
                mid = (low + high) / 2;
                slotLoc = pagePtr + (mid - 1) * entrySize;
                RID entryRID{*reinterpret_cast<unsigned *>(slotLoc + attr.length), *reinterpret_cast<unsigned short *>(slotLoc + attr.length + PAGE_NUM_BYTES)};
                Key<float> slotKey{*reinterpret_cast<float *>(slotLoc), entryRID};

                if (fKey < slotKey)
                    high = typeOfSearch == 3 ? --mid : mid - 1;
                else if (fKey == slotKey)
                    return mid;
                else
                    low = typeOfSearch == 2 ? ++mid : mid + 1;
            }
        } else {
            int strLen = *static_cast<const int *>(key);
            char str[attr.length + 1];
            memmove(str, static_cast<const char *>(key) + INT_BYTES, strLen);
            str[strLen] = '\0';
            Key<std::string> strKey{str, rid};

            while (low <= high) {
                mid = (low + high) / 2;
                slotLoc = pagePtr + (mid - 1) * entrySize;
                strLen = *reinterpret_cast<int *>(slotLoc);
                memmove(str, slotLoc + INT_BYTES, strLen);
                str[strLen] = '\0';
                RID entryRID{*reinterpret_cast<unsigned *>(slotLoc + (INT_BYTES + strLen)), *reinterpret_cast<unsigned short *>(slotLoc + (INT_BYTES + strLen + PAGE_NUM_BYTES))};
                Key<std::string> slotKey{str, entryRID};

                if (strKey < slotKey)
                    high = typeOfSearch == 3 ? --mid : mid - 1;
                else if (strKey == slotKey)
                    return mid;
                else
                    low = typeOfSearch == 2 ? ++mid : mid + 1;
            }
        }

        return typeOfSearch == 1 ? 0 : mid;
    }

    void IndexManager::shiftEntriesRight(char *pagePtr, SizeType entriesToShift, SizeType entrySize) {
        memmove(pagePtr + entrySize, pagePtr, entriesToShift * entrySize);
    }

    void IndexManager::putEntryOnPage(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, unsigned childPage) {
        if (attr.type == TypeVarChar) {
            int varCharLen = *static_cast<const int *>(key);
            memmove(pagePtr, key, varCharLen + INT_BYTES);
            pagePtr += varCharLen + INT_BYTES;
        } else {
            memmove(pagePtr, key, attr.length);
            pagePtr += attr.length;
        }

        memmove(pagePtr, &rid.pageNum, PAGE_NUM_BYTES);
        pagePtr += PAGE_NUM_BYTES;
        memmove(pagePtr, &rid.slotNum, SLOT_BYTES);

        if (childPage > 0) {
            pagePtr += SLOT_BYTES;
            memmove(pagePtr, &childPage, PAGE_POINTER_BYTES);
        }
    }

    RC IndexManager::insertEntryIntoEmptyIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        memset(rootPage, 0, PAGE_SIZE);
        if (ixFileHandle.appendPage(rootPage) == -1) return -1;  // placing down page to act as "pointer" to root node
        SizeType slotCount = 1;
        memmove(rootPage, &slotCount, SLOT_COUNT_BYTES);

        putEntryOnPage(rootPage + SLOT_COUNT_BYTES, attribute, key, rid);
        return ixFileHandle.appendPage(rootPage);
    }

    RC IndexManager::splitRootLeaf(IXFileHandle &fh, char *rootPage, const Attribute &attr, const void *key, const RID &rid, SizeType entrySize, SizeType slot) {
        char leftPage[PAGE_SIZE];
        char rightPage[PAGE_SIZE];
        memset(leftPage, 0, PAGE_SIZE);
        memset(rightPage, 0, PAGE_SIZE);
        memset(leftPage, 1, LEAF_CHECK_BYTE);
        memset(rightPage, 1, LEAF_CHECK_BYTE);
        unsigned rightPageNum = 3;
        memmove(leftPage + (LEAF_CHECK_BYTE + PAGE_POINTER_BYTES), &rightPageNum, PAGE_POINTER_BYTES);
        char *leftPtr = leftPage + LEAF_BYTES_BEFORE_KEYS;
        char *rightPtr = rightPage + LEAF_BYTES_BEFORE_KEYS;

        SizeType leftSlotCount = (fh.indexMaxPageNodes + 1) / 2;
        SizeType rightSlotCount = fh.indexMaxPageNodes + 1 - leftSlotCount;
        memmove(leftPtr - SLOT_COUNT_BYTES, &leftSlotCount, SLOT_COUNT_BYTES);
        memmove(rightPtr - SLOT_COUNT_BYTES, &rightSlotCount, SLOT_COUNT_BYTES);
        char *rootPagePtr;

        for (SizeType entry = 1, entriesMoved = 0; entry <= fh.indexMaxPageNodes; ++entry) {
            rootPagePtr = rootPage + (SLOT_COUNT_BYTES + (entry - 1) * entrySize);
            if (slot == entry) {
                if (entriesMoved++ < leftSlotCount) {
                    putEntryOnPage(leftPtr, attr, key, rid);
                    leftPtr += entrySize;
                } else {
                    putEntryOnPage(rightPtr, attr, key, rid);
                    rightPtr += entrySize;
                }
            }

            if (entriesMoved++ < leftSlotCount) {
                memmove(leftPtr, rootPagePtr, entrySize);
                leftPtr += entrySize;
            } else {
                memmove(rightPtr, rootPagePtr, entrySize);
                rightPtr += entrySize;
            }
        }

        if (slot == fh.indexMaxPageNodes + 1)
            putEntryOnPage(rightPtr, attr, key, rid);
        reinterpret_cast<SizeType *>(rootPage)[0] = 1;
        reinterpret_cast<unsigned *>(rootPage + SLOT_COUNT_BYTES)[0] = 2;
        rightPtr = rightPage + LEAF_BYTES_BEFORE_KEYS;
        if (attr.type == TypeVarChar)
            rightPtr += INT_BYTES + *reinterpret_cast<int *>(rightPtr);
        else
            rightPtr += attr.length;
        RID entryRID{*reinterpret_cast<unsigned *>(rightPtr), *reinterpret_cast<unsigned short *>(rightPtr + PAGE_NUM_BYTES)};
        putEntryOnPage(rootPage + ROOT_BYTES_BEFORE_KEYS, attr, rightPage + LEAF_BYTES_BEFORE_KEYS, entryRID, 2);

        if (fh.writePage(1, rootPage) == -1) return -1;
        if (fh.appendPage(leftPage) == -1) return -1;
        return fh.appendPage(rightPage);
    }

    RC IndexManager::insertEntryIntoOnlyRootIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        if (ixFileHandle.readPage(0, rootPage) == -1) return -1;
        if (ixFileHandle.readPage(1, rootPage) == -1) return -1;
        SizeType slotCount;
        memmove(&slotCount, rootPage, SLOT_COUNT_BYTES);
        SizeType entrySize = nodeEntrySize(attribute, true);
        SizeType slot = determineSlot(rootPage + SLOT_COUNT_BYTES, attribute, key, rid, true, slotCount, 2);

        if (slotCount >= ixFileHandle.indexMaxPageNodes)
            return splitRootLeaf(ixFileHandle, rootPage, attribute, key, rid, entrySize, slot);

        char *location = rootPage + (SLOT_COUNT_BYTES + (slot - 1) * entrySize);
        if (slot <= slotCount) shiftEntriesRight(location, slotCount - slot + 1, entrySize);
        putEntryOnPage(location, attribute, key, rid);
        ++slotCount;
        memmove(rootPage, &slotCount, SLOT_COUNT_BYTES);
        return ixFileHandle.writePage(1, rootPage);
    }

    RC IndexManager::getLeafPage(IXFileHandle &fh, char *pageData, unsigned &pageNum, const Attribute &attr, const void *key, const RID &rid) {
        SizeType entrySize = nodeEntrySize(attr, false);
        char *keysStart;
        SizeType slotCount, entryToFollow;
        unsigned currPageNum = 1;
        if (fh.readPage(0, pageData) == -1) return -1;

        while (true) {
            if (fh.readPage(currPageNum, pageData) == -1) return -1;
            if (currPageNum == 1) {
                keysStart = pageData + ROOT_BYTES_BEFORE_KEYS;
                slotCount = *reinterpret_cast<SizeType *>(pageData);
            } else {
                if (*reinterpret_cast<unsigned char *>(pageData) == 1) break;
                keysStart = pageData + PARENT_BYTES_BEFORE_KEYS;
                memmove(&slotCount, pageData + (LEAF_CHECK_BYTE + PAGE_POINTER_BYTES), SLOT_COUNT_BYTES);
            }

            entryToFollow = determineSlot(keysStart, attr, key, rid, entrySize, slotCount, 3);
            if (entryToFollow == 0) {
                memmove(&currPageNum, keysStart - PAGE_POINTER_BYTES, PAGE_POINTER_BYTES);
            } else {
                char *entryStart = keysStart + (entryToFollow - 1) * entrySize;
                if (attr.type == TypeVarChar)
                    memmove(&currPageNum, entryStart + (INT_BYTES + *reinterpret_cast<int *>(entryStart) + RID_BYTES), PAGE_POINTER_BYTES);
                else
                    memmove(&currPageNum, entryStart + (attr.length + RID_BYTES), PAGE_POINTER_BYTES);
            }
        }

        pageNum = currPageNum;
        return 0;
    }

    RC IndexManager::insertEntryIntoIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char leafPage[PAGE_SIZE];
        unsigned leafPageNum;
        if (getLeafPage(ixFileHandle, leafPage, leafPageNum, attribute, key, rid) == -1) return -1;
        SizeType entrySize = nodeEntrySize(attribute, true);
        SizeType slotCount = *reinterpret_cast<SizeType *>(leafPage + (LEAF_BYTES_BEFORE_KEYS - SLOT_COUNT_BYTES));
        SizeType insertSlot = determineSlot(leafPage + LEAF_BYTES_BEFORE_KEYS, attribute, key, rid, entrySize, slotCount, 2);

        if (slotCount >= ixFileHandle.indexMaxPageNodes) return -1;  // will not handle splitting, for now

        char *slotLoc = leafPage + (LEAF_BYTES_BEFORE_KEYS + (insertSlot - 1) * entrySize);
        if (insertSlot <= slotCount) shiftEntriesRight(slotLoc, slotCount - insertSlot + 1, entrySize);
        putEntryOnPage(slotLoc, attribute, key, rid);
        ++slotCount;
        memmove(leafPage + (LEAF_BYTES_BEFORE_KEYS - SLOT_COUNT_BYTES), &slotCount, SLOT_COUNT_BYTES);
        return ixFileHandle.writePage(leafPageNum, leafPage);
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (ixFileHandle.indexMaxPageNodes == 0) ixFileHandle.indexMaxPageNodes = maxNodeSlots(attribute);

        switch (ixFileHandle.pageCount) {
            case 0: return insertEntryIntoEmptyIndex(ixFileHandle, attribute, key, rid);
            case 2: return insertEntryIntoOnlyRootIndex(ixFileHandle, attribute, key, rid);
            default: return insertEntryIntoIndex(ixFileHandle, attribute, key, rid);
        }
    }

    void IndexManager::shiftEntriesLeft(char *pagePtr, SizeType entriesToShift, SizeType entrySize) {
        memmove(pagePtr - entrySize, pagePtr, entriesToShift * entrySize);
    }

    RC IndexManager::deleteEntryFromOnlyRootIndex(IXFileHandle &fh, const Attribute &attr, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        if (fh.readPage(0, rootPage) == -1) return -1;
        if (fh.readPage(1, rootPage) == -1) return -1;
        SizeType slotCount = *reinterpret_cast<SizeType *>(rootPage);
        if (slotCount == 0) return -1;
        SizeType entrySize = nodeEntrySize(attr, true);

        SizeType slotToDelete = determineSlot(rootPage + SLOT_COUNT_BYTES, attr, key, rid, entrySize, slotCount, 1);
        if (slotToDelete == 0) return -1;
        char *shiftStart = rootPage + (SLOT_COUNT_BYTES + slotToDelete * entrySize);
        shiftEntriesLeft(shiftStart, slotCount - slotToDelete, entrySize);

        --slotCount;
        memmove(rootPage, &slotCount, SLOT_COUNT_BYTES);
        return fh.writePage(1, rootPage);
    }

    RC IndexManager::deleteEntryFromIndex(IXFileHandle &fh, const Attribute &attr, const void *key, const RID &rid) {
        char leafPage[PAGE_SIZE];
        unsigned leafPageNum;
        if (getLeafPage(fh, leafPage, leafPageNum, attr, key, rid) == -1) return -1;
        SizeType entrySize = nodeEntrySize(attr, true);
        SizeType slotCount = *reinterpret_cast<SizeType *>(leafPage + (LEAF_BYTES_BEFORE_KEYS - SLOT_COUNT_BYTES));

        SizeType slotToDelete = determineSlot(leafPage + LEAF_BYTES_BEFORE_KEYS, attr, key, rid, entrySize, slotCount, 1);
        if (slotToDelete == 0) return -1;
        char *shiftStart = leafPage + (LEAF_BYTES_BEFORE_KEYS + slotToDelete * entrySize);
        shiftEntriesLeft(shiftStart, slotCount - slotToDelete, entrySize);

        --slotCount;
        memmove(leafPage + (LEAF_BYTES_BEFORE_KEYS - SLOT_COUNT_BYTES), &slotCount, SLOT_COUNT_BYTES);
        return fh.writePage(leafPageNum, leafPage);
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (ixFileHandle.indexMaxPageNodes == 0) ixFileHandle.indexMaxPageNodes = maxNodeSlots(attribute);

        switch (ixFileHandle.pageCount) {
            case 0: return -1;
            case 2: return deleteEntryFromOnlyRootIndex(ixFileHandle, attribute, key, rid);
            default: return deleteEntryFromIndex(ixFileHandle, attribute, key, rid);
        }
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        return -1;
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() : FileHandle(), indexMaxPageNodes(0) {}

    IXFileHandle::~IXFileHandle() = default;

} // namespace PeterDB