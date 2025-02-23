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

    struct IntKey {
        int key;
        RID rid;

        bool operator < (const IntKey & other) const;
        bool operator == (const IntKey & other) const;
        bool operator <= (const IntKey & other) const;
    };

    struct FloatKey {
        float key;
        RID rid;

        bool operator < (const FloatKey & other) const;
        bool operator == (const FloatKey & other) const;
        bool operator <= (const FloatKey & other) const;
    };

    struct StringKey {
        std::string key;
        RID rid;

        bool operator < (const StringKey & other) const;
        bool operator == (const StringKey & other) const;
        bool operator <= (const StringKey & other) const;
    };

    class IndexManager {

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

        int maxNodeSlots(const Attribute & attr);
        int nodeEntrySize(const Attribute & attr, bool isLeafPage);
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle : public FileHandle {
    public:
        int indexMaxPageNodes;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        RC initFileHandle(const std::string &fileName);
    };

}// namespace PeterDB
#endif // _ix_h_
