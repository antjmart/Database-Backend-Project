#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_set>
#include "src/include/rbfm.h"

struct tupleVal {
    int intVal;
    float floatVal;
    std::string stringVal;
    std::string type;

    explicit tupleVal(int i) : intVal(i), type("int") {}
    explicit tupleVal(float f) : floatVal(f), type("float") {}
    explicit tupleVal(const char *s) : stringVal(s), type("str") {}
};

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
        RBFM_ScanIterator recordScanner;

    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        void init(FileHandle & fHandle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute,
                  const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames);

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
        std::vector<Attribute> tablesDescriptor;
        std::vector<Attribute> columnsDescriptor;
        std::vector<Attribute> schemasDescriptor;
        std::vector<std::string> columnsColumns;
        int nextTableID;

    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, int *isSystemTable = nullptr, int *version = nullptr, std::unordered_map<std::string, int> *attrPositions = nullptr);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment
        void craftTupleData(char *data, const std::vector<tupleVal> & values);
        RC initTablesTable(FileHandle &fh);
        RC initColumnsTable(FileHandle &fh);
        RC addTablesEntry(FileHandle &fh, int table_id, int tableNameLen, const char *tableName, int fileNameLen, const char *fileName, int isSystem, char *data);
        RC addColumnsEntry(FileHandle &fh, int table_id, int nameLen, const char *name, AttrType columnType, int columnLen, int pos, char *data);
        RC addSchemasEntry(FileHandle &fh, int table_id, int version, int fieldCount, const char *fields, char *data);
        RC getTableID(const std::string &tableName, int &tableID, bool deleteEntry, int *isSystemTable);
        void formatString(const std::string &str, char *value);
        RC getSchemaVersionInfo(const std::string &tableName, int &tableID, int &version, int &pos, std::unordered_map<std::string, int> &names, std::unordered_set<int> &positions);
        void RelationManager::convertDataToCurrSchema(void *data, const std::vector<Attribute> &currDescriptor, const std::vector<Attribute> &recordDescriptor,
                                                      std::unordered_map<std::string, int> &currAttrPos, std::unordered_map<std::string, int> &recoVersionAttrPos);
    };

} // namespace PeterDB

#endif // _rm_h_