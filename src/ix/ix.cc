#include "src/include/ix.h"

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

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
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