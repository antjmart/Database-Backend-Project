#include "src/include/ix.h"

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

    IXFileHandle::IXFileHandle() : FileHandle() {}

    IXFileHandle::~IXFileHandle() = default;

} // namespace PeterDB