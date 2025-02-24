#include "src/include/ix.h"
#include <cstring>

constexpr int SLOT_COUNT_BYTES = sizeof(unsigned short);
constexpr int PAGE_POINTER_BYTES = sizeof(unsigned);
constexpr int PAGE_NUM_BYTES = sizeof(unsigned);
constexpr int SLOT_BYTES = sizeof(unsigned short);
constexpr int RID_BYTES = PAGE_NUM_BYTES + SLOT_BYTES;
constexpr int LEAF_CHECK_BYTE = 1;
constexpr int LEAF_BYTES_BEFORE_KEYS = LEAF_CHECK_BYTE + 2 * PAGE_POINTER_BYTES + SLOT_COUNT_BYTES;
constexpr int PARENT_BYTES_BEFORE_KEYS = LEAF_CHECK_BYTE + PAGE_POINTER_BYTES + SLOT_COUNT_BYTES + PAGE_POINTER_BYTES;
constexpr int ROOT_BYTES_BEFORE_KEYS = SLOT_COUNT_BYTES + PAGE_POINTER_BYTES;
constexpr int INT_BYTES = sizeof(int);


namespace PeterDB {
    bool IntKey::operator < (const IntKey & other) const {
        return key < other.key || (key == other.key && rid < other.rid);
    }

    bool FloatKey::operator < (const FloatKey & other) const {
        return key < other.key || (key == other.key && rid < other.rid);
    }

    bool StringKey::operator < (const StringKey & other) const {
        return key < other.key || (key == other.key && rid < other.rid);
    }

    bool IntKey::operator == (const IntKey & other) const {
        return key == other.key && rid == other.rid;
    }

    bool FloatKey::operator == (const FloatKey & other) const {
        return key == other.key && rid == other.rid;
    }

    bool StringKey::operator == (const StringKey & other) const {
        return key == other.key && rid == other.rid;
    }

    bool IntKey::operator <= (const IntKey & other) const {
        return key < other.key || (key == other.key && rid <= other.rid);
    }

    bool FloatKey::operator <= (const FloatKey & other) const {
        return key < other.key || (key == other.key && rid <= other.rid);
    }

    bool StringKey::operator <= (const StringKey & other) const {
        return key < other.key || (key == other.key && rid <= other.rid);
    }

    RC IXFileHandle::initFileHandle(const std::string &fileName) {
        if (FileHandle::initFileHandle(fileName) == -1) return -1;
        indexMaxPageNodes = -1;
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

    int IndexManager::nodeEntrySize(const Attribute & attr, bool isLeafPage) {
        int entrySize = attr.length + RID_BYTES;
        if (attr.type == TypeVarChar) entrySize += INT_BYTES;
        if (!isLeafPage) entrySize += PAGE_POINTER_BYTES;
        return entrySize;
    }

    int IndexManager::maxNodeSlots(const Attribute & attr) {
        // max index entries on a node determined by parent node layout
        return (PAGE_SIZE - PARENT_BYTES_BEFORE_KEYS) / nodeEntrySize(attr, false);
    }

    unsigned short IndexManager::determineSlot(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, bool isLeafPage, unsigned short slotCount) {
        int entrySize = attr.length + RID_BYTES;
        if (!isLeafPage) entrySize += PAGE_POINTER_BYTES;
        char *slotLoc;
        unsigned short slot = 1;

        if (attr.type == TypeInt) {
            IntKey iKey{*static_cast<const int *>(key), rid};

            for (; slot <= slotCount; ++slot) {
                slotLoc = pagePtr + (slot - 1) * entrySize;
                RID entryRID{*reinterpret_cast<unsigned *>(slotLoc + attr.length), *reinterpret_cast<unsigned short *>(slotLoc + attr.length + PAGE_NUM_BYTES)};
                IntKey slotKey{*reinterpret_cast<int *>(slotLoc), entryRID};
                if (iKey < slotKey) break;
            }
        } else if (attr.type == TypeReal) {
            FloatKey fKey{*static_cast<const float *>(key), rid};

            for (; slot <= slotCount; ++slot) {
                slotLoc = pagePtr + (slot - 1) * entrySize;
                RID entryRID{*reinterpret_cast<unsigned *>(slotLoc + attr.length), *reinterpret_cast<unsigned short *>(slotLoc + attr.length + PAGE_NUM_BYTES)};
                FloatKey slotKey{*reinterpret_cast<float *>(slotLoc), entryRID};
                if (fKey < slotKey) break;
            }
        } else {
            entrySize += INT_BYTES;
            int strLen = *static_cast<const int *>(key);
            char str[attr.length + 1];
            memmove(str, static_cast<const char *>(key) + INT_BYTES, strLen);
            str[strLen] = '\0';
            StringKey strKey{str, rid};

            for (; slot <= slotCount; ++slot) {
                slotLoc = pagePtr + (slot - 1) * entrySize;
                strLen = *reinterpret_cast<int *>(slotLoc);
                memmove(str, slotLoc + INT_BYTES, strLen);
                str[strLen] = '\0';
                RID entryRID{*reinterpret_cast<unsigned *>(slotLoc + (INT_BYTES + strLen)), *reinterpret_cast<unsigned short *>(slotLoc + (INT_BYTES + strLen + PAGE_NUM_BYTES))};
                StringKey slotKey{str, entryRID};

                if (strKey < slotKey) break;
            }
        }

        return slot;
    }

    void IndexManager::shiftEntriesRight(char *pagePtr, unsigned short entriesToShift, int entrySize) {
        memmove(pagePtr + entrySize, pagePtr, entriesToShift * entrySize);
    }

    void IndexManager::putEntryOnPage(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, unsigned childPage) {
        if (attr.type == TypeVarChar) {
            int varCharLen;
            memmove(&varCharLen, key, INT_BYTES);
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
        unsigned short slotCount = 1;
        memmove(rootPage, &slotCount, SLOT_COUNT_BYTES);

        putEntryOnPage(rootPage + SLOT_COUNT_BYTES, attribute, key, rid);
        return ixFileHandle.appendPage(rootPage);
    }

    RC IndexManager::insertEntryIntoOnlyRootIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        char rootPage[PAGE_SIZE];
        if (ixFileHandle.readPage(0, rootPage) == -1) return -1;
        unsigned short slotCount;
        memmove(&slotCount, rootPage, SLOT_COUNT_BYTES);
        int entrySize = nodeEntrySize(attribute, true);

        if (slotCount < ixFileHandle.indexMaxPageNodes) {
            unsigned short slot = determineSlot(rootPage + SLOT_COUNT_BYTES, attribute, key, rid, true, slotCount);
            char *location = rootPage + (SLOT_COUNT_BYTES + (slot - 1) * entrySize);
            if (slot <= slotCount) shiftEntriesRight(location, slotCount - slot + 1, entrySize);
            putEntryOnPage(location, attribute, key, rid);
            ++slotCount;
            memmove(rootPage, &slotCount, SLOT_COUNT_BYTES);
            return ixFileHandle.writePage(0, rootPage);
        } else {
            return -1;
        }
    }

    RC IndexManager::insertEntryIntoIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (ixFileHandle.indexMaxPageNodes == -1) ixFileHandle.indexMaxPageNodes = maxNodeSlots(attribute);

        if (ixFileHandle.pageCount == 0)
            return insertEntryIntoEmptyIndex(ixFileHandle, attribute, key, rid);
        else if (ixFileHandle.pageCount == 1)
            return insertEntryIntoOnlyRootIndex(ixFileHandle, attribute, key, rid);
        else
            return insertEntryIntoIndex(ixFileHandle, attribute, key, rid);
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        if (ixFileHandle.indexMaxPageNodes == -1) ixFileHandle.indexMaxPageNodes = maxNodeSlots(attribute);
        return -1;
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

    IXFileHandle::IXFileHandle() : FileHandle(), indexMaxPageNodes(-1) {}

    IXFileHandle::~IXFileHandle() = default;

} // namespace PeterDB