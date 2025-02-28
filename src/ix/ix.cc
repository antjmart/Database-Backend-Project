#include "src/include/ix.h"
#include <cstring>
#include <iomanip>
#include <iostream>

constexpr unsigned short OFFSET_BYTES = sizeof(PeterDB::SizeType);
constexpr unsigned short PAGE_NUM_BYTES = sizeof(unsigned);
constexpr unsigned short SLOT_BYTES = sizeof(unsigned short);
constexpr unsigned short RID_BYTES = PAGE_NUM_BYTES + SLOT_BYTES;
constexpr unsigned short LEAF_CHECK_BYTE = 1;
constexpr unsigned short LEAF_BYTES_BEFORE_KEYS = LEAF_CHECK_BYTE + PAGE_NUM_BYTES + OFFSET_BYTES;
constexpr unsigned short NODE_BYTES_BEFORE_KEYS = LEAF_CHECK_BYTE + OFFSET_BYTES + PAGE_NUM_BYTES;
constexpr unsigned short INT_BYTES = sizeof(int);


namespace PeterDB {
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

    SizeType IndexManager::nodeEntrySize(const Attribute & attr, const void *key, bool isLeafPage) const {
        SizeType entrySize = RID_BYTES;
        if (attr.type == TypeVarChar)
            entrySize += INT_BYTES + *static_cast<const int *>(key);
        else
            entrySize += attr.length;
        if (!isLeafPage) entrySize += PAGE_NUM_BYTES;
        return entrySize;
    }

    char * IndexManager::determinePos(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, char *endPtr, bool isLeaf, int typeOfSearch) {
        char *prevKey = nullptr;
        switch (attr.type) {
            case TypeInt: {
                Key<int> iKey{*static_cast<const int *>(key), rid};
                SizeType entrySize = nodeEntrySize(attr, key, isLeaf);
                while (pagePtr < endPtr) {
                    RID entryRID{*reinterpret_cast<unsigned *>(pagePtr + attr.length), *reinterpret_cast<unsigned short *>(pagePtr + attr.length + PAGE_NUM_BYTES)};
                    Key<int> posKey{*reinterpret_cast<int *>(pagePtr), entryRID};

                    switch (typeOfSearch) {
                        case 1:
                            if (iKey == posKey) return pagePtr;
                        break;
                        case 2:
                            if (iKey <= posKey) return pagePtr;
                        break;
                        case 3:
                            if (iKey < posKey) return prevKey;
                        prevKey = pagePtr;
                    }
                    pagePtr += entrySize;
                }
                break;
            } case TypeReal: {
                Key<float> fKey{*static_cast<const float *>(key), rid};
                SizeType entrySize = nodeEntrySize(attr, key, isLeaf);
                while (pagePtr < endPtr) {
                    RID entryRID{*reinterpret_cast<unsigned *>(pagePtr + attr.length), *reinterpret_cast<unsigned short *>(pagePtr + attr.length + PAGE_NUM_BYTES)};
                    Key<float> posKey{*reinterpret_cast<float *>(pagePtr), entryRID};

                    switch (typeOfSearch) {
                        case 1:
                            if (fKey == posKey) return pagePtr;
                        break;
                        case 2:
                            if (fKey <= posKey) return pagePtr;
                        break;
                        case 3:
                            if (fKey < posKey) return prevKey;
                        prevKey = pagePtr;
                    }
                    pagePtr += entrySize;
                }
                break;
            } case TypeVarChar: {
                unsigned strLen = *static_cast<const unsigned *>(key);
                Key<std::string> strKey{{static_cast<const char *>(key) + INT_BYTES, strLen}, rid};

                while (pagePtr < endPtr) {
                    strLen = *reinterpret_cast<unsigned *>(pagePtr);
                    RID entryRID{*reinterpret_cast<unsigned *>(pagePtr + (INT_BYTES + strLen)), *reinterpret_cast<unsigned short *>(pagePtr + (INT_BYTES + strLen + PAGE_NUM_BYTES))};
                    Key<std::string> posKey{{pagePtr + INT_BYTES, strLen}, entryRID};

                    switch (typeOfSearch) {
                        case 1:
                            if (strKey == posKey) return pagePtr;
                        break;
                        case 2:
                            if (strKey <= posKey) return pagePtr;
                        break;
                        case 3:
                            if (strKey < posKey) return prevKey;
                        prevKey = pagePtr;
                    }
                    pagePtr += nodeEntrySize(attr, pagePtr, isLeaf);
                }
            }
        }

        return typeOfSearch == 3 ? prevKey : endPtr;
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
            memmove(pagePtr, &childPage, PAGE_NUM_BYTES);
        }
    }

    RC IndexManager::insertEntryIntoEmptyIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        *reinterpret_cast<unsigned *>(rootPage) = 1;
        if (ixFileHandle.appendPage(rootPage) == -1) return -1;  // placing down page to act as "pointer" to root node
        memset(rootPage, 0, LEAF_CHECK_BYTE + PAGE_NUM_BYTES);
        memset(rootPage, 1, LEAF_CHECK_BYTE);
        *reinterpret_cast<SizeType *>(rootPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES)) = 1;

        putEntryOnPage(rootPage + LEAF_BYTES_BEFORE_KEYS, attribute, key, rid);
        return ixFileHandle.appendPage(rootPage);
    }

    void IndexManager::splitLeaf(IXFileHandle &fh, char *leftPage, char *rightPage, const Attribute &attr, const void *key, const RID &rid, SizeType slot) {
        memset(rightPage, 1, LEAF_CHECK_BYTE);
        memmove(rightPage + LEAF_CHECK_BYTE, leftPage + LEAF_CHECK_BYTE, PAGE_NUM_BYTES);
        memmove(leftPage + LEAF_CHECK_BYTE, &fh.pageCount, PAGE_NUM_BYTES);
        SizeType leftSlotCount = (fh.indexMaxPageNodes + 1) / 2;
        SizeType rightSlotCount = fh.indexMaxPageNodes + 1 - leftSlotCount;
        char *leftPtr = leftPage + LEAF_BYTES_BEFORE_KEYS;
        char *rightPtr = rightPage + LEAF_BYTES_BEFORE_KEYS;
        memmove(leftPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES), &leftSlotCount, SLOT_COUNT_BYTES);
        memmove(rightPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES), &rightSlotCount, SLOT_COUNT_BYTES);
        SizeType entrySize = nodeEntrySize(attr, true);

        if (slot <= leftSlotCount) {
            memmove(rightPtr, leftPtr + (leftSlotCount - 1) * entrySize, rightSlotCount * entrySize);
            shiftEntriesRight(leftPtr + (slot - 1) * entrySize, leftSlotCount - slot, entrySize);
            putEntryOnPage(leftPtr + (slot - 1) * entrySize, attr, key, rid);
        } else {
            memmove(rightPtr, leftPtr + leftSlotCount * entrySize, (fh.indexMaxPageNodes - leftSlotCount) * entrySize);
            shiftEntriesRight(rightPtr + (slot - leftSlotCount - 1) * entrySize, rightSlotCount - (slot - leftSlotCount), entrySize);
            putEntryOnPage(rightPtr + (slot - leftSlotCount - 1) * entrySize, attr, key, rid);
        }
    }

    void IndexManager::splitNode(IXFileHandle &fh, char *leftPage, char *rightPage, const Attribute &attr, const void *key, const RID &rid, unsigned pageNum, SizeType slot, const void * & pushUpKey) {
        memset(rightPage, 0, LEAF_CHECK_BYTE);
        SizeType leftSlotCount = fh.indexMaxPageNodes / 2;
        SizeType rightSlotCount = fh.indexMaxPageNodes - leftSlotCount;
        char *leftPtr = leftPage + NODE_BYTES_BEFORE_KEYS;
        char *rightPtr = rightPage + NODE_BYTES_BEFORE_KEYS;
        memmove(leftPage + LEAF_CHECK_BYTE, &leftSlotCount, SLOT_COUNT_BYTES);
        memmove(rightPage + LEAF_CHECK_BYTE, &rightSlotCount, SLOT_COUNT_BYTES);
        SizeType entrySize = nodeEntrySize(attr, false);

        if (slot <= leftSlotCount) {
            memmove(rightPtr, leftPtr + leftSlotCount * entrySize, rightSlotCount * entrySize);
            pushUpKey = static_cast<const void *>(leftPtr + (leftSlotCount - 1) * entrySize);
            shiftEntriesRight(leftPtr + (slot - 1) * entrySize, leftSlotCount - slot, entrySize);
            putEntryOnPage(leftPtr + (slot - 1) * entrySize, attr, key, rid, pageNum);
        } else if (slot == leftSlotCount + 1) {
            memmove(rightPtr, leftPtr + leftSlotCount * entrySize, rightSlotCount * entrySize);
            pushUpKey = key;
        } else {
            memmove(rightPtr, leftPtr + (leftSlotCount + 1) * entrySize, (rightSlotCount - 1) * entrySize);
            pushUpKey = static_cast<const void *>(leftPtr + leftSlotCount * entrySize);
            shiftEntriesRight(rightPtr + (slot - leftSlotCount - 2) * entrySize, rightSlotCount - (slot - leftSlotCount - 1), entrySize);
            putEntryOnPage(rightPtr + (slot - leftSlotCount - 2) * entrySize, attr, key, rid, pageNum);
        }
    }

    RC IndexManager::getLeafPage(IXFileHandle &fh, char *pageData, unsigned &pageNum, const Attribute &attr, const void *key, const RID &rid) {
        SizeType entrySize = nodeEntrySize(attr, false);
        char *keysStart;
        SizeType slotCount, entryToFollow;
        if (fh.readPage(0, pageData) == -1) return -1;
        unsigned currPageNum = *reinterpret_cast<unsigned *>(pageData);

        while (true) {
            if (fh.readPage(currPageNum, pageData) == -1) return -1;
            if (*reinterpret_cast<unsigned char *>(pageData) == 1) break;
            keysStart = pageData + NODE_BYTES_BEFORE_KEYS;
            memmove(&slotCount, pageData + LEAF_CHECK_BYTE, SLOT_COUNT_BYTES);

            entryToFollow = key == nullptr ? 0 : determineSlot(keysStart, attr, key, rid, entrySize, slotCount, 3);
            if (entryToFollow == 0) {
                memmove(&currPageNum, keysStart - PAGE_NUM_BYTES, PAGE_NUM_BYTES);
            } else {
                char *entryStart = keysStart + (entryToFollow - 1) * entrySize;
                if (attr.type == TypeVarChar)
                    memmove(&currPageNum, entryStart + (INT_BYTES + *reinterpret_cast<int *>(entryStart) + RID_BYTES), PAGE_NUM_BYTES);
                else
                    memmove(&currPageNum, entryStart + (attr.length + RID_BYTES), PAGE_NUM_BYTES);
            }
        }

        pageNum = currPageNum;
        return 0;
    }

    RC IndexManager::visitInsertNode(IXFileHandle &fh, char *pageData, unsigned pageNum, const Attribute &attr, const void *key, const RID &rid, bool & needSplit, void *pushUpKey, RID &pushUpRID, unsigned &childPage) {
        bool isLeaf = *reinterpret_cast<unsigned char *>(pageData) == 1;
        SizeType entrySize = nodeEntrySize(attr, isLeaf);
        
        if (isLeaf) {
            char *keysStart = pageData + LEAF_BYTES_BEFORE_KEYS;
            SizeType slotCount = *reinterpret_cast<SizeType *>(pageData + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES));

            if (slotCount < fh.indexMaxPageNodes) {
                SizeType insertSlot = determineSlot(keysStart, attr, key, rid, entrySize, slotCount, 2);
                char *slotLoc = keysStart + (insertSlot - 1) * entrySize;
                if (insertSlot <= slotCount) shiftEntriesRight(slotLoc, slotCount - insertSlot + 1, entrySize);
                putEntryOnPage(slotLoc, attr, key, rid);
                *reinterpret_cast<SizeType *>(pageData + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES)) = slotCount + 1;
                needSplit = false;
                return fh.writePage(pageNum, pageData);
            } else {
                needSplit = true;
                if (attr.type == TypeVarChar)
                    memmove(pushUpKey, key, INT_BYTES + *static_cast<const int *>(key));
                else
                    memmove(pushUpKey, key, attr.length);
                pushUpRID.pageNum = rid.pageNum;
                pushUpRID.slotNum = rid.slotNum;
                return 0;
            }
        } else {
            char *keysStart = pageData + NODE_BYTES_BEFORE_KEYS;
            SizeType slotCount = *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE);
            SizeType entryToFollow = determineSlot(keysStart, attr, key, rid, entrySize, slotCount, 3);
            unsigned nextVisit;
            if (entryToFollow == 0) {
                memmove(&nextVisit, keysStart - PAGE_NUM_BYTES, PAGE_NUM_BYTES);
            } else {
                char *entryStart = keysStart + (entryToFollow - 1) * entrySize;
                if (attr.type == TypeVarChar)
                    memmove(&nextVisit, entryStart + (INT_BYTES + *reinterpret_cast<int *>(entryStart) + RID_BYTES), PAGE_NUM_BYTES);
                else
                    memmove(&nextVisit, entryStart + (attr.length + RID_BYTES), PAGE_NUM_BYTES);
            }

            char visitPage[PAGE_SIZE];
            if (fh.readPage(nextVisit, visitPage) == -1) return -1;
            if (visitInsertNode(fh, visitPage, nextVisit, attr, key, rid, needSplit, pushUpKey, pushUpRID, childPage) == -1) return -1;
            if (!needSplit) return 0;

            char newPage[PAGE_SIZE];
            bool leafVisited = *reinterpret_cast<unsigned char *>(visitPage) == 1;
            SizeType visitEntrySize = nodeEntrySize(attr, leafVisited);
            SizeType visitSlotCount = *reinterpret_cast<SizeType *>(visitPage + (leafVisited ? LEAF_CHECK_BYTE + PAGE_NUM_BYTES : LEAF_CHECK_BYTE));
            SizeType slot = determineSlot(visitPage + (leafVisited ? LEAF_BYTES_BEFORE_KEYS : NODE_BYTES_BEFORE_KEYS), attr, pushUpKey, pushUpRID, visitEntrySize, visitSlotCount, 2);

            if (leafVisited) {
                splitLeaf(fh, visitPage, newPage, attr, pushUpKey, pushUpRID, slot);
                char *splitKey = newPage + LEAF_BYTES_BEFORE_KEYS;
                char *ptr = splitKey;
                if (attr.type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<int *>(ptr);
                else
                    ptr += attr.length;
                RID r{*reinterpret_cast<unsigned *>(ptr), *reinterpret_cast<unsigned short *>(ptr + PAGE_NUM_BYTES)};

                if (slotCount < fh.indexMaxPageNodes) {
                    slot = determineSlot(pageData + NODE_BYTES_BEFORE_KEYS, attr, splitKey, r, entrySize, slotCount, 2);
                    char *slotLoc = pageData + NODE_BYTES_BEFORE_KEYS + (slot - 1) * entrySize;
                    if (slot <= slotCount) shiftEntriesRight(slotLoc, slotCount - slot + 1, entrySize);
                    putEntryOnPage(slotLoc, attr, splitKey, r, fh.pageCount);
                    *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE) = slotCount + 1;
                    needSplit = false;
                    if (fh.writePage(pageNum, pageData) == -1) return -1;
                } else {
                    needSplit = true;
                    memmove(pushUpKey, splitKey, ptr - splitKey);
                    pushUpRID.pageNum = r.pageNum;
                    pushUpRID.slotNum = r.slotNum;
                    childPage = fh.pageCount;
                }
            } else {
                const void *splitKey;
                splitNode(fh, visitPage, newPage, attr, pushUpKey, pushUpRID, childPage, slot, splitKey);
                RID r{};
                if (splitKey == pushUpKey) {
                    r.pageNum = pushUpRID.pageNum;
                    r.slotNum = pushUpRID.slotNum;
                } else {
                    const char *ptr = static_cast<const char *>(splitKey);
                    if (attr.type == TypeVarChar)
                        ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                    else
                        ptr += attr.length;
                    r.pageNum = *reinterpret_cast<const unsigned *>(ptr);
                    r.slotNum = *reinterpret_cast<const unsigned short *>(ptr + PAGE_NUM_BYTES);
                    memmove(&childPage, ptr + RID_BYTES, PAGE_NUM_BYTES);
                }
                memmove(newPage + (LEAF_CHECK_BYTE + SLOT_COUNT_BYTES), &childPage, PAGE_NUM_BYTES);

                if (slotCount < fh.indexMaxPageNodes) {
                    slot = determineSlot(pageData + NODE_BYTES_BEFORE_KEYS, attr, splitKey, r, entrySize, slotCount, 2);
                    char *slotLoc = pageData + NODE_BYTES_BEFORE_KEYS + (slot - 1) * entrySize;
                    if (slot <= slotCount) shiftEntriesRight(slotLoc, slotCount - slot + 1, entrySize);
                    putEntryOnPage(slotLoc, attr, splitKey, r, fh.pageCount);
                    *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE) = slotCount + 1;
                    needSplit = false;
                    if (fh.writePage(pageNum, pageData) == -1) return -1;
                } else {
                    needSplit = true;
                    if (attr.type == TypeVarChar)
                        memmove(pushUpKey, splitKey, INT_BYTES + *static_cast<const int *>(splitKey));
                    else
                        memmove(pushUpKey, splitKey, attr.length);
                    pushUpRID.pageNum = r.pageNum;
                    pushUpRID.slotNum = r.slotNum;
                    childPage = fh.pageCount;
                }
            }

            if (fh.writePage(nextVisit, visitPage) == -1) return -1;
            return fh.appendPage(newPage);
        }
    }

    RC IndexManager::createNewRoot(IXFileHandle &fh, char *rootPage, char *rootPtr, const Attribute &attr, const void *rootKey, const RID &rootRID, unsigned childPage) {
        bool rootIsLeaf = *reinterpret_cast<unsigned char *>(rootPage) == 1;
        unsigned rootPageNum = *reinterpret_cast<unsigned *>(rootPtr);
        SizeType entrySize = nodeEntrySize(attr, rootIsLeaf);
        SizeType slotCount = fh.indexMaxPageNodes;
        SizeType slot = determineSlot(rootPage + (rootIsLeaf ? LEAF_BYTES_BEFORE_KEYS : NODE_BYTES_BEFORE_KEYS), attr, rootKey, rootRID, entrySize, slotCount, 2);
        char newPage[PAGE_SIZE];
        char newRoot[PAGE_SIZE];
        memset(newRoot, 0, LEAF_CHECK_BYTE);
        *reinterpret_cast<SizeType *>(newRoot + LEAF_CHECK_BYTE) = 1;
        memmove(newRoot + (LEAF_CHECK_BYTE + SLOT_COUNT_BYTES), &rootPageNum, PAGE_NUM_BYTES);

        if (rootIsLeaf) {
            splitLeaf(fh, rootPage, newPage, attr, rootKey, rootRID, slot);
            char *ptr = newPage + LEAF_BYTES_BEFORE_KEYS;
            if (attr.type == TypeVarChar)
                ptr += INT_BYTES + *reinterpret_cast<int *>(ptr);
            else
                ptr += attr.length;
            RID r{*reinterpret_cast<unsigned *>(ptr), *reinterpret_cast<unsigned short *>(ptr + PAGE_NUM_BYTES)};
            putEntryOnPage(newRoot + NODE_BYTES_BEFORE_KEYS, attr, newPage + LEAF_BYTES_BEFORE_KEYS, r, fh.pageCount);
        } else {
            const void *splitKey;
            splitNode(fh, rootPage, newPage, attr, rootKey, rootRID, childPage, slot, splitKey);
            RID r{};
            if (splitKey == rootKey) {
                r.pageNum = rootRID.pageNum;
                r.slotNum = rootRID.slotNum;
            } else {
                const char *ptr = static_cast<const char *>(splitKey);
                if (attr.type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                else
                    ptr += attr.length;
                r.pageNum = *reinterpret_cast<const unsigned *>(ptr);
                r.slotNum = *reinterpret_cast<const unsigned short *>(ptr + PAGE_NUM_BYTES);
                memmove(&childPage, ptr + RID_BYTES, PAGE_NUM_BYTES);
            }
            memmove(newPage + (LEAF_CHECK_BYTE + SLOT_COUNT_BYTES), &childPage, PAGE_NUM_BYTES);
            putEntryOnPage(newRoot + NODE_BYTES_BEFORE_KEYS, attr, splitKey, r, fh.pageCount);
        }

        if (fh.writePage(rootPageNum, rootPage) == -1) return -1;
        if (fh.appendPage(newPage) == -1) return -1;
        memmove(rootPtr, &fh.pageCount, PAGE_NUM_BYTES);
        if (fh.writePage(0, rootPtr) == -1) return -1;
        return fh.appendPage(newRoot);
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (ixFileHandle.pageCount == 0)
            return insertEntryIntoEmptyIndex(ixFileHandle, attribute, key, rid);

        char rootPtr[PAGE_SIZE];
        if (ixFileHandle.readPage(0, rootPtr) == -1) return -1;
        char rootPage[PAGE_SIZE];
        unsigned rootPageNum = *reinterpret_cast<unsigned *>(rootPtr);
        if (ixFileHandle.readPage(rootPageNum, rootPage) == -1) return -1;

        RID pushUpRID{};
        unsigned childPage;
        bool needSplit;
        char pushUpKey[attribute.length + INT_BYTES + RID_BYTES + PAGE_NUM_BYTES];
        if (visitInsertNode(ixFileHandle, rootPage, rootPageNum, attribute, key, rid, needSplit, pushUpKey, pushUpRID, childPage) == -1) return -1;
        if (!needSplit) return 0;
        return createNewRoot(ixFileHandle, rootPage, rootPtr, attribute, pushUpKey, pushUpRID, childPage);
    }

    void IndexManager::shiftEntriesLeft(char *pagePtr, SizeType entriesToShift, SizeType entrySize) {
        memmove(pagePtr - entrySize, pagePtr, entriesToShift * entrySize);
    }

    RC IndexManager::deleteEntryFromOnlyRootIndex(IXFileHandle &fh, const Attribute &attr, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        if (fh.readPage(0, rootPage) == -1) return -1;
        if (fh.readPage(1, rootPage) == -1) return -1;
        SizeType slotCount = *reinterpret_cast<SizeType *>(rootPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES));
        if (slotCount == 0) return -1;
        SizeType entrySize = nodeEntrySize(attr, true);

        SizeType slotToDelete = determineSlot(rootPage + LEAF_BYTES_BEFORE_KEYS, attr, key, rid, entrySize, slotCount, 1);
        if (slotToDelete == 0) return -1;
        char *shiftStart = rootPage + (LEAF_BYTES_BEFORE_KEYS + slotToDelete * entrySize);
        shiftEntriesLeft(shiftStart, slotCount - slotToDelete, entrySize);

        --slotCount;
        memmove(rootPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES), &slotCount, SLOT_COUNT_BYTES);
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
        if (!ixFileHandle.file.is_open()) return -1;
        ix_ScanIterator.init(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
        return 0;
    }

    void IndexManager::printPageKeys(char *pagePtr, bool isLeafPage, SizeType slotCount, const Attribute &attr, std::ostream &out) const {
        SizeType entrySize = nodeEntrySize(attr, isLeafPage);
        char *entryLoc;
        out << "{\"keys\":[";

        if (isLeafPage) {
            int prevInt, currInt; float prevFloat, currFloat; std::string prevStr, currStr;
            for (SizeType slot = 1; slot <= slotCount; ++slot) {
                entryLoc = pagePtr + (slot - 1) * entrySize;

                if (slot == 1) {
                    switch (attr.type) {
                        case TypeInt:
                            prevInt = *reinterpret_cast<int *>(entryLoc);
                            out << "\"" << prevInt;
                            entryLoc += attr.length;
                            break;
                        case TypeReal:
                            prevFloat = *reinterpret_cast<float *>(entryLoc);
                            out << "\"" << prevFloat;
                            entryLoc += attr.length;
                            break;
                        case TypeVarChar:
                            unsigned varCharLen = *reinterpret_cast<unsigned *>(entryLoc);
                            prevStr = std::string{entryLoc + INT_BYTES, varCharLen};
                            out << "\"" << prevStr;
                            entryLoc += INT_BYTES + varCharLen;
                    }
                    out << ":[(" << *reinterpret_cast<unsigned *>(entryLoc) << ',' << *reinterpret_cast<unsigned short *>(entryLoc + PAGE_NUM_BYTES) << ')';
                    continue;
                }

                unsigned ridPage; unsigned short ridSlot;
                switch (attr.type) {
                    case TypeInt:
                        currInt = *reinterpret_cast<int *>(entryLoc);
                        memmove(&ridPage, entryLoc + attr.length, PAGE_NUM_BYTES);
                        memmove(&ridSlot, entryLoc + (attr.length + PAGE_NUM_BYTES), SLOT_BYTES);
                        if (currInt == prevInt) {
                            out << ",(" << ridPage << ',' << ridSlot << ')';
                        } else {
                            out << "]\",\"" << currInt << ":[(" << ridPage << ',' << ridSlot << ')';
                            prevInt = currInt;
                        }
                        break;
                    case TypeReal:
                        currFloat = *reinterpret_cast<float *>(entryLoc);
                        memmove(&ridPage, entryLoc + attr.length, PAGE_NUM_BYTES);
                        memmove(&ridSlot, entryLoc + (attr.length + PAGE_NUM_BYTES), SLOT_BYTES);
                        if (currFloat == prevFloat) {
                            out << ",(" << ridPage << ',' << ridSlot << ')';
                        } else {
                            out << "]\",\"" << currFloat << ":[(" << ridPage << ',' << ridSlot << ')';
                            prevFloat = currFloat;
                        }
                        break;
                    case TypeVarChar:
                        unsigned varCharLen = *reinterpret_cast<unsigned *>(entryLoc);
                        currStr = std::string{entryLoc + INT_BYTES, varCharLen};
                        memmove(&ridPage, entryLoc + (INT_BYTES + varCharLen), PAGE_NUM_BYTES);
                        memmove(&ridSlot, entryLoc + (INT_BYTES + varCharLen + PAGE_NUM_BYTES), SLOT_BYTES);
                        if (currStr == prevStr) {
                            out << ",(" << ridPage << ',' << ridSlot << ')';
                        } else {
                            out << "]\",\"" << currStr << ":[(" << ridPage << ',' << ridSlot << ')';
                            prevStr = currStr;
                        }
                }
            }
            out << (slotCount == 0 ? "]}" : "]\"]}");
        } else {
            for (SizeType slot = 1; slot <= slotCount; ++slot, out << "\"") {
                out << (slot == 1 ? "\"" : ",\"");
                entryLoc = pagePtr + (slot - 1) * entrySize;

                switch (attr.type) {
                    case TypeInt:
                        out << *reinterpret_cast<int *>(entryLoc);
                        break;
                    case TypeReal:
                        out << *reinterpret_cast<float *>(entryLoc);
                        break;
                    case TypeVarChar:
                        out << std::string{entryLoc + INT_BYTES, *reinterpret_cast<unsigned *>(entryLoc)};
                }
            }
            out << "],\n";
        }
    }

    RC IndexManager::printSubtree(unsigned pageNum, int indents, IXFileHandle &fh, const Attribute &attr, std::ostream &out) const {
        char *pageData = new char[PAGE_SIZE];
        if (fh.readPage(pageNum, pageData) == -1) {delete[] pageData; return -1;}
        char *keysStart;
        SizeType slotCount;
        out << std::setw(indents * 4) << "";

        if (*reinterpret_cast<unsigned char *>(pageData) == 1) {
            // leaf node
            keysStart = pageData + LEAF_BYTES_BEFORE_KEYS;
            memmove(&slotCount, keysStart - SLOT_COUNT_BYTES, SLOT_COUNT_BYTES);
            printPageKeys(keysStart, true, slotCount, attr, out);
            delete[] pageData;
            return 0;
        }

        // regular node
        keysStart = pageData + NODE_BYTES_BEFORE_KEYS;
        memmove(&slotCount, pageData + LEAF_CHECK_BYTE, SLOT_COUNT_BYTES);
        printPageKeys(keysStart, false, slotCount, attr, out);

        out << std::setw(indents * 4 + 1) << "" << "\"children\":[\n";
        if (slotCount > 0) {
            SizeType entrySize = nodeEntrySize(attr, false);
            char *slotLoc;
            unsigned childPage = *reinterpret_cast<unsigned *>(keysStart - PAGE_NUM_BYTES);
            std::vector<unsigned> childPages;
            childPages.reserve(slotCount + 1);
            childPages.push_back(childPage);

            for (SizeType slot = 1; slot <= slotCount; ++slot) {
                slotLoc = keysStart + (slot - 1) * entrySize;
                if (attr.type == TypeVarChar)
                    memmove(&childPage, slotLoc + (INT_BYTES + *reinterpret_cast<int *>(slotLoc) + RID_BYTES), PAGE_NUM_BYTES);
                else
                    memmove(&childPage, slotLoc + (attr.length + RID_BYTES), PAGE_NUM_BYTES);

                childPages.push_back(childPage);
            }

            delete[] pageData;
            for (SizeType i = 0; i < slotCount; ++i) {
                if (printSubtree(childPages[i], indents + 1, fh, attr, out) == -1) return -1;
                out << ",\n";
            }
            if (printSubtree(childPages[slotCount], indents + 1, fh, attr, out) == -1) return -1;
            out << '\n';
        } else
            delete[] pageData;
        out << std::setw(indents * 4) << "" << "]}";
        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        if (ixFileHandle.pageCount == 0) return 0;
        char rootPage[PAGE_SIZE];

        if (ixFileHandle.readPage(0, rootPage) == -1) return -1;
        if (printSubtree(*reinterpret_cast<unsigned *>(rootPage), 0, ixFileHandle, attribute, out) == -1) return -1;
        out << std::endl;
        return 0;
    }

    IX_ScanIterator::IX_ScanIterator()
        : fh(nullptr), lowKey(nullptr), highKey(nullptr), currPos(nullptr), endPos(nullptr) {}

    IX_ScanIterator::~IX_ScanIterator() {}

    void IX_ScanIterator::init(IXFileHandle &fh, const Attribute &attr, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        this->fh = &fh;
        this->attr = attr;
        this->lowKey = lowKey;
        this->highKey = highKey;
        this->lowKeyInclusive = lowKeyInclusive;
        this->highKeyInclusive = highKeyInclusive;
        firstScan = true;
    }

    int IX_ScanIterator::acceptKey(RID &rid, void *key) {
        // 0 for success, 1 for rejection, 2 for IX_EOF
        switch (attr.type) {
            case TypeInt: {
                int entryInt = *reinterpret_cast<int *>(currPos);
                currPos += attr.length + RID_BYTES;
                if (lowKey) {
                    int lowKeyInt = *static_cast<const int *>(lowKey);
                    if (entryInt < lowKeyInt || (entryInt == lowKeyInt && !lowKeyInclusive)) return 1;
                }
                if (highKey) {
                    int highKeyInt = *static_cast<const int *>(highKey);
                    if (entryInt > highKeyInt || (entryInt == highKeyInt && !highKeyInclusive)) return 2;
                }
                memmove(key, &entryInt, attr.length);
                break;
            } case TypeReal: {
                float entryFloat = *reinterpret_cast<float *>(keyLoc);
                currPos += attr.length + RID_BYTES;
                if (lowKey) {
                    float lowKeyFloat = *static_cast<const float *>(lowKey);
                    if (entryFloat < lowKeyFloat || (entryFloat == lowKeyFloat && !lowKeyInclusive)) return 1;
                }
                if (highKey) {
                    float highKeyFloat = *static_cast<const float *>(highKey);
                    if (entryFloat > highKeyFloat || (entryFloat == highKeyFloat && !highKeyInclusive)) return 2;
                }
                memmove(key, &entryFloat, attr.length);
                break;
            } case TypeVarChar: {
                unsigned len = *reinterpret_cast<unsigned *>(currPos);
                std::string entryStr{currPos + INT_BYTES, len};
                currPos += len + INT_BYTES + RID_BYTES;
                if (lowKey) {
                    std::string lowKeyStr{static_cast<const char *>(lowKey) + INT_BYTES, *static_cast<const unsigned *>(lowKey)};
                    if (entryStr < lowKeyStr || (entryStr == lowKeyStr && !lowKeyInclusive)) return 1;
                }
                if (highKey) {
                    std::string highKeyStr{static_cast<const char *>(highKey) + INT_BYTES, *static_cast<const unsigned *>(highKey)};
                    if (entryStr > highKeyStr || (entryStr == highKeyStr && !highKeyInclusive)) return 2;
                }
                memmove(key, &len, INT_BYTES);
                memmove(static_cast<char *>(key) + INT_BYTES, entryStr.c_str(), len);
            }
        }

        memmove(&rid.pageNum, currPos - RID_BYTES, PAGE_NUM_BYTES);
        memmove(&rid.slotNum, currPos - SLOT_BYTES, SLOT_BYTES);
        return 0;
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if (firstScan) {
            if (fh->pageCount == 0) return IX_EOF;
            unsigned pgNum;
            if (IndexManager::instance().getLeafPage(*fh, currPage, pgNum, attr, lowKey, RID{0, 1}) == -1) return -1;
            currPos = currPage + LEAF_BYTES_BEFORE_KEYS;
            endPos = currPage + *reinterpret_cast<SizeType *>(currPos - OFFSET_BYTES);
            memmove(&nextPageNum, currPage + LEAF_CHECK_BYTE, PAGE_NUM_BYTES);
            firstScan = false;
        }

        while (true) {
            while (currPos < endPos) {
                int status = acceptKey(rid, key);
                if (status == 0) return 0;
                if (status == 2) return IX_EOF;
            }

            if (nextPageNum == 0) return IX_EOF;
            if (fh->readPage(nextPageNum, currPage) == -1) return -1;
            memmove(&nextPageNum, currPage + LEAF_CHECK_BYTE, PAGE_NUM_BYTES);
            currPos = currPage + LEAF_BYTES_BEFORE_KEYS;
            endPos = currPage + *reinterpret_cast<SizeType *>(currPos - OFFSET_BYTES);
        }
    }

    RC IX_ScanIterator::close() {
        fh = nullptr;
        return 0;
    }

    IXFileHandle::IXFileHandle() : FileHandle() {}

    IXFileHandle::~IXFileHandle() = default;

} // namespace PeterDB