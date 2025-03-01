#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    template <typename T>
    class Key {
        T val;
        RID rid;

    public:
        Key(const T &value, const RID &recoID)
            : val(value), rid(recoID) {}

        bool operator < (const Key<T> & other) const {
            return val < other.val || (val == other.val && rid < other.rid);
        }

        bool operator == (const Key<T> & other) const {
            return val == other.val && rid == other.rid;
        }

        bool operator <= (const Key<T> & other) const {
            return val < other.val || (val == other.val && rid <= other.rid);
        }
    };

    class IndexManager {
        friend class IX_ScanIterator;

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

        SizeType nodeEntrySize(const Attribute & attr, const void *key, bool isLeafPage) const;
        RC insertEntryIntoEmptyIndex(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);
        char * putEntryOnPage(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, unsigned childPage = 0);
        char * determinePos(char *pagePtr, const Attribute &attr, const void *key, const RID &rid, char *endPtr, bool isLeaf, int typeOfSearch);
        void shiftEntriesRight(char *oldLoc, char *newLoc, SizeType bytesToShift);
        void shiftEntriesLeft(char *oldLoc, char *newLoc, SizeType bytesToShift);
        void splitLeaf(IXFileHandle &fh, char *leftPage, char *rightPage, const Attribute &attr, const void *key, const RID &rid, char *insertPos);
        void splitNode(IXFileHandle &fh, char *leftPage, char *rightPage, const Attribute &attr, const void *key, const RID &rid, unsigned pageNum, SizeType slot, const void * & pushUpKey);
        RC getLeafPage(IXFileHandle &fh, char *pageData, unsigned &pageNum, const Attribute &attr, const void *key, const RID &rid);
        RC printSubtree(unsigned pageNum, int indents, IXFileHandle &fh, const Attribute &attr, std::ostream &out) const;
        void printPageKeys(char * const pagePtr, bool isLeafPage, char * const endPos, const Attribute &attr, std::ostream &out) const;
        RC visitInsertNode(IXFileHandle &fh, char *pageData, unsigned pageNum, const Attribute &attr, const void *key, const RID &rid, bool & needSplit, void *pushUpKey, RID &pushUpRID, unsigned &childPage);
        RC createNewRoot(IXFileHandle &fh, char *rootPage, char *rootPtr, const Attribute &attr, const void *rootKey, const RID &rootRID, unsigned childPage);
    };

    class IX_ScanIterator {
        IXFileHandle *fh;
        Attribute attr;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        char currPage[PAGE_SIZE];
        char *currPos;
        char *endPos;
        unsigned nextPageNum;
        bool firstScan;

        // 0 for accepted key, 1 for rejected key, 2 for no more possible acceptable keys (IX_EOF)
        int acceptKey(RID &rid, void *key);

    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // initialize all data members after IndexManager::scan() is called on this iterator
        void init(IXFileHandle &fh, const Attribute &attr, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive);

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle : public FileHandle {
    public:
        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();
    };

}// namespace PeterDB
#endif // _ix_h_
