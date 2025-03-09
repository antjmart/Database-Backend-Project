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

    void IndexManager::shiftEntriesRight(char *oldLoc, char *newLoc, SizeType bytesToShift) {
        memmove(newLoc, oldLoc, bytesToShift);
    }

    char * IndexManager::putEntryOnPage(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, unsigned childPage) {
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
        pagePtr += SLOT_BYTES;

        if (childPage > 0) {
            memmove(pagePtr, &childPage, PAGE_NUM_BYTES);
            pagePtr += PAGE_NUM_BYTES;
        }
        return pagePtr;
    }

    RC IndexManager::insertEntryIntoEmptyIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        *reinterpret_cast<unsigned *>(rootPage) = 1;
        if (ixFileHandle.appendPage(rootPage) == -1) return -1;  // placing down page to act as "pointer" to root node
        memset(rootPage, 0, LEAF_CHECK_BYTE + PAGE_NUM_BYTES);
        memset(rootPage, 1, LEAF_CHECK_BYTE);
        char *endPos = putEntryOnPage(rootPage + LEAF_BYTES_BEFORE_KEYS, attribute, key, rid);
        *reinterpret_cast<SizeType *>(rootPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES)) = endPos - rootPage;
        return ixFileHandle.appendPage(rootPage);
    }

    void IndexManager::splitLeaf(IXFileHandle &fh, char *leftPage, char *rightPage, const Attribute &attr, const void *key, const RID &rid, char *insertPos) {
        char newLeft[PAGE_SIZE];
        memset(newLeft, 1, LEAF_CHECK_BYTE);
        memset(rightPage, 1, LEAF_CHECK_BYTE);
        memmove(rightPage + LEAF_CHECK_BYTE, leftPage + LEAF_CHECK_BYTE, PAGE_NUM_BYTES);
        memmove(newLeft + LEAF_CHECK_BYTE, &fh.pageCount, PAGE_NUM_BYTES);
        char *leftPtr = leftPage + LEAF_BYTES_BEFORE_KEYS;
        char * ptrs[2] = {newLeft + LEAF_BYTES_BEFORE_KEYS, rightPage + LEAF_BYTES_BEFORE_KEYS};
        char *leftEnd = leftPage + *reinterpret_cast<SizeType *>(leftPtr - OFFSET_BYTES);

        SizeType entrySize;
        int i;
        while (leftPtr < leftEnd) {
            i = (ptrs[0] - newLeft < PAGE_SIZE / 2) ? 0 : 1;
            if (leftPtr == insertPos) {
                ptrs[i] = putEntryOnPage(ptrs[i], attr, key, rid);
                insertPos = nullptr;
            } else {
                entrySize = nodeEntrySize(attr, leftPtr, true);
                memmove(ptrs[i], leftPtr, entrySize);
                ptrs[i] += entrySize;
                leftPtr += entrySize;
            }
        }

        if (insertPos != nullptr) ptrs[1] = putEntryOnPage(ptrs[1], attr, key, rid);
        *reinterpret_cast<SizeType *>(newLeft + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES)) = ptrs[0] - newLeft;
        *reinterpret_cast<SizeType *>(rightPage + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES)) = ptrs[1] - rightPage;
        memmove(leftPage, newLeft, PAGE_SIZE);
    }

    void IndexManager::splitNode(IXFileHandle &fh, char *leftPage, char *rightPage, const Attribute &attr, const void *key, const RID &rid, unsigned pageNum, char *insertPos, void *pushUpKey) {
        char newLeft[PAGE_SIZE];
        memset(newLeft, 0, LEAF_CHECK_BYTE);
        memmove(newLeft + (LEAF_CHECK_BYTE + OFFSET_BYTES), leftPage + (LEAF_CHECK_BYTE + OFFSET_BYTES), PAGE_NUM_BYTES);
        memset(rightPage, 0, LEAF_CHECK_BYTE);
        char *leftPtr = leftPage + NODE_BYTES_BEFORE_KEYS;
        char * ptrs[2] = {newLeft + NODE_BYTES_BEFORE_KEYS, rightPage + NODE_BYTES_BEFORE_KEYS};
        char *leftEnd = leftPage + *reinterpret_cast<SizeType *>(leftPage + LEAF_CHECK_BYTE);

        SizeType entrySize;
        bool valuePushed = false;
        int i;
        while (leftPtr < leftEnd) {
            i = (ptrs[0] - newLeft < PAGE_SIZE / 2) ? 0 : 1;
            if (ptrs[0] - newLeft >= PAGE_SIZE / 2 && !valuePushed) {
                if (leftPtr == insertPos) {
                    putEntryOnPage(static_cast<char *>(pushUpKey), attr, key, rid, pageNum);
                    insertPos = nullptr;
                } else {
                    entrySize = nodeEntrySize(attr, leftPtr, false);
                    memmove(pushUpKey, leftPtr, entrySize);
                    leftPtr += entrySize;
                }
                valuePushed = true;
                continue;
            }

            if (leftPtr == insertPos) {
                ptrs[i] = putEntryOnPage(ptrs[i], attr, key, rid, pageNum);
                insertPos = nullptr;
            } else {
                entrySize = nodeEntrySize(attr, leftPtr, false);
                memmove(ptrs[i], leftPtr, entrySize);
                ptrs[i] += entrySize;
                leftPtr += entrySize;
            }
        }

        if (insertPos != nullptr) ptrs[1] = putEntryOnPage(ptrs[1], attr, key, rid, pageNum);
        *reinterpret_cast<SizeType *>(newLeft + LEAF_CHECK_BYTE) = ptrs[0] - newLeft;
        *reinterpret_cast<SizeType *>(rightPage + LEAF_CHECK_BYTE) = ptrs[1] - rightPage;
        memmove(leftPage, newLeft, PAGE_SIZE);
    }

    RC IndexManager::getLeafPage(IXFileHandle &fh, char *pageData, unsigned &pageNum, const Attribute &attr, const void *key, const RID &rid) {
        char *keysStart = pageData + NODE_BYTES_BEFORE_KEYS;
        char *pos, *end;
        if (fh.readPage(0, pageData) == -1) return -1;
        unsigned currPageNum = *reinterpret_cast<unsigned *>(pageData);

        while (true) {
            if (fh.readPage(currPageNum, pageData) == -1) return -1;
            if (*reinterpret_cast<unsigned char *>(pageData) == 1) break;
            end = pageData + *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE);

            pos = key == nullptr ? nullptr : determinePos(keysStart, attr, key, rid, end, false, 3);
            if (pos == nullptr) {
                memmove(&currPageNum, keysStart - PAGE_NUM_BYTES, PAGE_NUM_BYTES);
            } else {
                if (attr.type == TypeVarChar)
                    memmove(&currPageNum, pos + (INT_BYTES + *reinterpret_cast<int *>(pos) + RID_BYTES), PAGE_NUM_BYTES);
                else
                    memmove(&currPageNum, pos + (attr.length + RID_BYTES), PAGE_NUM_BYTES);
            }
        }

        pageNum = currPageNum;
        return 0;
    }

    RC IndexManager::visitInsertNode(IXFileHandle &fh, char *pageData, unsigned pageNum, const Attribute &attr, const void *key, const RID &rid, bool & needSplit, void *pushUpKey, RID &pushUpRID, unsigned &childPage) {
        bool isLeaf = *reinterpret_cast<unsigned char *>(pageData) == 1;
        
        if (isLeaf) {
            char *keysStart = pageData + LEAF_BYTES_BEFORE_KEYS;
            char *endPos = pageData + *reinterpret_cast<SizeType *>(keysStart - OFFSET_BYTES);
            SizeType entrySize = nodeEntrySize(attr, key, true);
            if (entrySize + (endPos - pageData) <= PAGE_SIZE) {
                char *insertPos = determinePos(keysStart, attr, key, rid, endPos, true, 2);
                if (insertPos < endPos) shiftEntriesRight(insertPos, insertPos + entrySize, endPos - insertPos);
                putEntryOnPage(insertPos, attr, key, rid);
                *reinterpret_cast<SizeType *>(pageData + (LEAF_CHECK_BYTE + PAGE_NUM_BYTES)) = (endPos + entrySize) - pageData;
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
            char *endPos = pageData + *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE);
            char *pos = determinePos(keysStart, attr, key, rid, endPos, false, 3);
            unsigned nextVisit;
            if (pos == nullptr) {
                memmove(&nextVisit, keysStart - PAGE_NUM_BYTES, PAGE_NUM_BYTES);
            } else {
                if (attr.type == TypeVarChar)
                    memmove(&nextVisit, pos + (INT_BYTES + *reinterpret_cast<int *>(pos) + RID_BYTES), PAGE_NUM_BYTES);
                else
                    memmove(&nextVisit, pos + (attr.length + RID_BYTES), PAGE_NUM_BYTES);
            }

            char *visitPage = new char[PAGE_SIZE];
            if (fh.readPage(nextVisit, visitPage) == -1) {delete[] visitPage; return -1;}
            if (visitInsertNode(fh, visitPage, nextVisit, attr, key, rid, needSplit, pushUpKey, pushUpRID, childPage) == -1) {delete[] visitPage; return -1;}
            if (!needSplit) {delete[] visitPage; return 0;}

            char *newPage = new char[PAGE_SIZE];
            bool leafVisited = *reinterpret_cast<unsigned char *>(visitPage) == 1;
            char *visitEnd = visitPage + *reinterpret_cast<SizeType *>(visitPage + (leafVisited ? (LEAF_CHECK_BYTE + PAGE_NUM_BYTES) : LEAF_CHECK_BYTE));
            char *visitInsert = determinePos(visitPage + (leafVisited ? LEAF_BYTES_BEFORE_KEYS : NODE_BYTES_BEFORE_KEYS), attr, pushUpKey, pushUpRID, visitEnd, leafVisited, 2);

            if (leafVisited) {
                splitLeaf(fh, visitPage, newPage, attr, pushUpKey, pushUpRID, visitInsert);
                char *splitKey = newPage + LEAF_BYTES_BEFORE_KEYS;
                char *ptr = splitKey;
                if (attr.type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<int *>(ptr);
                else
                    ptr += attr.length;
                RID r{*reinterpret_cast<unsigned *>(ptr), *reinterpret_cast<unsigned short *>(ptr + PAGE_NUM_BYTES)};

                SizeType entrySize = nodeEntrySize(attr, splitKey, false);
                if (entrySize + (endPos - pageData) <= PAGE_SIZE) {
                    char *insertPos = determinePos(keysStart, attr, splitKey, r, endPos, false, 2);
                    if (insertPos < endPos) shiftEntriesRight(insertPos, insertPos + entrySize, endPos - insertPos);
                    putEntryOnPage(insertPos, attr, splitKey, r, fh.pageCount);
                    *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE) = (endPos + entrySize) - pageData;
                    needSplit = false;
                    if (fh.writePage(pageNum, pageData) == -1) {delete[] visitPage; delete[] newPage; return -1;}
                } else {
                    needSplit = true;
                    memmove(pushUpKey, splitKey, ptr - splitKey);
                    pushUpRID.pageNum = r.pageNum;
                    pushUpRID.slotNum = r.slotNum;
                    childPage = fh.pageCount;
                }
            } else {
                char splitKey[attr.length + INT_BYTES + RID_BYTES + PAGE_NUM_BYTES];
                splitNode(fh, visitPage, newPage, attr, pushUpKey, pushUpRID, childPage, visitInsert, splitKey);
                RID r{};
                const char *ptr = splitKey;
                if (attr.type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                else
                    ptr += attr.length;
                r.pageNum = *reinterpret_cast<const unsigned *>(ptr);
                r.slotNum = *reinterpret_cast<const unsigned short *>(ptr + PAGE_NUM_BYTES);
                memmove(&childPage, ptr + RID_BYTES, PAGE_NUM_BYTES);
                memmove(newPage + (LEAF_CHECK_BYTE + OFFSET_BYTES), &childPage, PAGE_NUM_BYTES);

                SizeType entrySize = nodeEntrySize(attr, splitKey, false);
                if (entrySize + (endPos - pageData) <= PAGE_SIZE) {
                    char *insertPos = determinePos(keysStart, attr, splitKey, r, endPos, false, 2);
                    if (insertPos < endPos) shiftEntriesRight(insertPos, insertPos + entrySize, endPos - insertPos);
                    putEntryOnPage(insertPos, attr, splitKey, r, fh.pageCount);
                    *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE) = (endPos + entrySize) - pageData;
                    needSplit = false;
                    if (fh.writePage(pageNum, pageData) == -1) {delete[] visitPage; delete[] newPage; return -1;}
                } else {
                    needSplit = true;
                    memmove(pushUpKey, splitKey, ptr - splitKey);
                    pushUpRID.pageNum = r.pageNum;
                    pushUpRID.slotNum = r.slotNum;
                    childPage = fh.pageCount;
                }
            }

            if (fh.writePage(nextVisit, visitPage) == -1) {delete[] visitPage; delete[] newPage; return -1;}
            RC status = fh.appendPage(newPage);
            delete[] visitPage;
            delete[] newPage;
            return status;
        }
    }

    RC IndexManager::createNewRoot(IXFileHandle &fh, char *rootPage, char *rootPtr, const Attribute &attr, const void *rootKey, const RID &rootRID, unsigned childPage) {
        bool rootIsLeaf = *reinterpret_cast<unsigned char *>(rootPage) == 1;
        unsigned rootPageNum = *reinterpret_cast<unsigned *>(rootPtr);
        char *keysStart = rootPage + (rootIsLeaf ? LEAF_BYTES_BEFORE_KEYS : NODE_BYTES_BEFORE_KEYS);
        char *endPos = rootPage + *reinterpret_cast<SizeType *>(rootPage + (LEAF_CHECK_BYTE + (rootIsLeaf ? PAGE_NUM_BYTES : 0)));
        char *insertPos = determinePos(keysStart, attr, rootKey, rootRID, endPos, rootIsLeaf, 2);
        char newPage[PAGE_SIZE];
        char newRoot[PAGE_SIZE];
        memset(newRoot, 0, LEAF_CHECK_BYTE);
        memmove(newRoot + (LEAF_CHECK_BYTE + OFFSET_BYTES), &rootPageNum, PAGE_NUM_BYTES);

        if (rootIsLeaf) {
            splitLeaf(fh, rootPage, newPage, attr, rootKey, rootRID, insertPos);
            char *ptr = newPage + LEAF_BYTES_BEFORE_KEYS;
            if (attr.type == TypeVarChar)
                ptr += INT_BYTES + *reinterpret_cast<int *>(ptr);
            else
                ptr += attr.length;
            RID r{*reinterpret_cast<unsigned *>(ptr), *reinterpret_cast<unsigned short *>(ptr + PAGE_NUM_BYTES)};
            char *newRootEnd = putEntryOnPage(newRoot + NODE_BYTES_BEFORE_KEYS, attr, newPage + LEAF_BYTES_BEFORE_KEYS, r, fh.pageCount);
            *reinterpret_cast<SizeType *>(newRoot + LEAF_CHECK_BYTE) = newRootEnd - newRoot;
        } else {
            char splitKey[attr.length + INT_BYTES + RID_BYTES + PAGE_NUM_BYTES];
            splitNode(fh, rootPage, newPage, attr, rootKey, rootRID, childPage, insertPos, splitKey);
            RID r{};
            const char *ptr = splitKey;
            if (attr.type == TypeVarChar)
                ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
            else
                ptr += attr.length;
            r.pageNum = *reinterpret_cast<const unsigned *>(ptr);
            r.slotNum = *reinterpret_cast<const unsigned short *>(ptr + PAGE_NUM_BYTES);
            memmove(&childPage, ptr + RID_BYTES, PAGE_NUM_BYTES);

            memmove(newPage + (LEAF_CHECK_BYTE + OFFSET_BYTES), &childPage, PAGE_NUM_BYTES);
            char *newRootEnd = putEntryOnPage(newRoot + NODE_BYTES_BEFORE_KEYS, attr, splitKey, r, fh.pageCount);
            *reinterpret_cast<SizeType *>(newRoot + LEAF_CHECK_BYTE) = newRootEnd - newRoot;
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

        char *rootPtr = new char[PAGE_SIZE];
        if (ixFileHandle.readPage(0, rootPtr) == -1) {delete[] rootPtr; return -1;}
        char *rootPage = new char[PAGE_SIZE];
        unsigned rootPageNum = *reinterpret_cast<unsigned *>(rootPtr);
        if (ixFileHandle.readPage(rootPageNum, rootPage) == -1) {delete[] rootPtr; delete[] rootPage; return -1;}

        RID pushUpRID{};
        unsigned childPage;
        bool needSplit;
        char pushUpKey[attribute.length + INT_BYTES + RID_BYTES + PAGE_NUM_BYTES];
        if (visitInsertNode(ixFileHandle, rootPage, rootPageNum, attribute, key, rid, needSplit, pushUpKey, pushUpRID, childPage) == -1) {delete[] rootPtr; delete[] rootPage; return -1;}
        if (!needSplit) {delete[] rootPtr; delete[] rootPage; return 0;}

        RC status = createNewRoot(ixFileHandle, rootPage, rootPtr, attribute, pushUpKey, pushUpRID, childPage);
        delete[] rootPtr;
        delete[] rootPage;
        return status;
    }

    void IndexManager::shiftEntriesLeft(char *oldLoc, char *newLoc, SizeType bytesToShift) {
        memmove(newLoc, oldLoc, bytesToShift);
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (ixFileHandle.pageCount == 0) return -1;

        char leafPage[PAGE_SIZE];
        unsigned leafPageNum;
        if (getLeafPage(ixFileHandle, leafPage, leafPageNum, attribute, key, rid) == -1) return -1;
        char *keysStart = leafPage + LEAF_BYTES_BEFORE_KEYS;
        char *end = leafPage + *reinterpret_cast<SizeType *>(keysStart - OFFSET_BYTES);
        if (end == keysStart) return -1;

        char *deletePos = determinePos(keysStart, attribute, key, rid, end, true, 1);
        if (deletePos == end) return -1;
        char *nextPos = deletePos + nodeEntrySize(attribute, deletePos, true);
        shiftEntriesLeft(nextPos, deletePos, end - nextPos);

        *reinterpret_cast<SizeType *>(keysStart - OFFSET_BYTES) = end - (nextPos - deletePos) - leafPage;
        return ixFileHandle.writePage(leafPageNum, leafPage);
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

    void IndexManager::printPageKeys(char * const pagePtr, bool isLeafPage, char * const endPos, const Attribute &attr, std::ostream &out) const {
        char *pos = pagePtr;
        out << "{\"keys\":[";

        if (isLeafPage) {
            int prevInt, currInt; float prevFloat, currFloat; std::string prevStr, currStr;
            while (pos < endPos) {
                if (pos == pagePtr) {
                    switch (attr.type) {
                        case TypeInt:
                            prevInt = *reinterpret_cast<int *>(pos);
                            out << "\"" << prevInt;
                            pos += attr.length;
                            break;
                        case TypeReal:
                            prevFloat = *reinterpret_cast<float *>(pos);
                            out << "\"" << prevFloat;
                            pos += attr.length;
                            break;
                        case TypeVarChar:
                            unsigned varCharLen = *reinterpret_cast<unsigned *>(pos);
                            prevStr = std::string{pos + INT_BYTES, varCharLen};
                            out << "\"" << prevStr;
                            pos += INT_BYTES + varCharLen;
                    }
                    out << ":[(" << *reinterpret_cast<unsigned *>(pos) << ',' << *reinterpret_cast<unsigned short *>(pos + PAGE_NUM_BYTES) << ')';
                    pos += RID_BYTES;
                    continue;
                }

                unsigned ridPage; unsigned short ridSlot;
                switch (attr.type) {
                    case TypeInt:
                        currInt = *reinterpret_cast<int *>(pos);
                        memmove(&ridPage, pos + attr.length, PAGE_NUM_BYTES);
                        memmove(&ridSlot, pos + (attr.length + PAGE_NUM_BYTES), SLOT_BYTES);
                        if (currInt == prevInt) {
                            out << ",(" << ridPage << ',' << ridSlot << ')';
                        } else {
                            out << "]\",\"" << currInt << ":[(" << ridPage << ',' << ridSlot << ')';
                            prevInt = currInt;
                        }
                        break;
                    case TypeReal:
                        currFloat = *reinterpret_cast<float *>(pos);
                        memmove(&ridPage, pos + attr.length, PAGE_NUM_BYTES);
                        memmove(&ridSlot, pos + (attr.length + PAGE_NUM_BYTES), SLOT_BYTES);
                        if (currFloat == prevFloat) {
                            out << ",(" << ridPage << ',' << ridSlot << ')';
                        } else {
                            out << "]\",\"" << currFloat << ":[(" << ridPage << ',' << ridSlot << ')';
                            prevFloat = currFloat;
                        }
                        break;
                    case TypeVarChar:
                        unsigned varCharLen = *reinterpret_cast<unsigned *>(pos);
                        currStr = std::string{pos + INT_BYTES, varCharLen};
                        memmove(&ridPage, pos + (INT_BYTES + varCharLen), PAGE_NUM_BYTES);
                        memmove(&ridSlot, pos + (INT_BYTES + varCharLen + PAGE_NUM_BYTES), SLOT_BYTES);
                        if (currStr == prevStr) {
                            out << ",(" << ridPage << ',' << ridSlot << ')';
                        } else {
                            out << "]\",\"" << currStr << ":[(" << ridPage << ',' << ridSlot << ')';
                            prevStr = currStr;
                        }
                }
                pos += nodeEntrySize(attr, pos, true);
            }
            out << (pagePtr == endPos ? "]}" : "]\"]}");
        } else {
            for (; pos < endPos; out << "\"", pos += nodeEntrySize(attr, pos, false)) {
                out << (pos == pagePtr ? "\"" : ",\"");

                switch (attr.type) {
                    case TypeInt:
                        out << *reinterpret_cast<int *>(pos);
                        break;
                    case TypeReal:
                        out << *reinterpret_cast<float *>(pos);
                        break;
                    case TypeVarChar:
                        out << std::string{pos + INT_BYTES, *reinterpret_cast<unsigned *>(pos)};
                }
            }
            out << "],\n";
        }
    }

    RC IndexManager::printSubtree(unsigned pageNum, int indents, IXFileHandle &fh, const Attribute &attr, std::ostream &out) const {
        char *pageData = new char[PAGE_SIZE];
        if (fh.readPage(pageNum, pageData) == -1) {delete[] pageData; return -1;}
        char *keysStart, *end;
        out << std::setw(indents * 4) << "";

        if (*reinterpret_cast<unsigned char *>(pageData) == 1) {
            // leaf node
            keysStart = pageData + LEAF_BYTES_BEFORE_KEYS;
            end = pageData + *reinterpret_cast<SizeType *>(keysStart - OFFSET_BYTES);
            printPageKeys(keysStart, true, end, attr, out);
            delete[] pageData;
            return 0;
        }

        // regular node
        keysStart = pageData + NODE_BYTES_BEFORE_KEYS;
        end = pageData + *reinterpret_cast<SizeType *>(pageData + LEAF_CHECK_BYTE);
        printPageKeys(keysStart, false, end, attr, out);

        out << std::setw(indents * 4 + 1) << "" << "\"children\":[\n";
        if (end > keysStart) {
            char *pos = keysStart;
            unsigned childPage = *reinterpret_cast<unsigned *>(keysStart - PAGE_NUM_BYTES);
            std::vector<unsigned> childPages;
            childPages.push_back(childPage);

            while (pos < end) {
                if (attr.type == TypeVarChar)
                    memmove(&childPage, pos + (INT_BYTES + *reinterpret_cast<int *>(pos) + RID_BYTES), PAGE_NUM_BYTES);
                else
                    memmove(&childPage, pos + (attr.length + RID_BYTES), PAGE_NUM_BYTES);

                childPages.push_back(childPage);
                pos += nodeEntrySize(attr, pos, false);
            }

            delete[] pageData;
            unsigned last = childPages.size() - 1;
            for (unsigned i = 0; i < last; ++i) {
                if (printSubtree(childPages[i], indents + 1, fh, attr, out) == -1) return -1;
                out << ",\n";
            }
            if (printSubtree(childPages[last], indents + 1, fh, attr, out) == -1) return -1;
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
                float entryFloat = *reinterpret_cast<float *>(currPos);
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