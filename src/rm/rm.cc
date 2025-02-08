#include "src/include/rm.h"
#include <cstring>

constexpr int INT_BYTES = 4;
constexpr int FLOAT_BYTES = 4;

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() {
        tablesDescriptor.push_back(Attribute{"table-id", TypeInt, 4});
        tablesDescriptor.push_back(Attribute{"table-name", TypeVarChar, 50});
        tablesDescriptor.push_back(Attribute{"file-name", TypeVarChar, 50});
        tablesDescriptor.push_back(Attribute{"is-system-table", TypeInt, 4});
        columnsDescriptor.push_back(Attribute{"table-id", TypeInt, 4});
        columnsDescriptor.push_back(Attribute{"column-name", TypeVarChar, 50});
        columnsDescriptor.push_back(Attribute{"column-type", TypeInt, 4});
        columnsDescriptor.push_back(Attribute{"column-length", TypeInt, 4});
        columnsDescriptor.push_back(Attribute{"column-position", TypeInt, 4});
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    void RelationManager::craftTupleData(char *data, const std::vector<tupleVal> & values) {
        int numVals = values.size();
        std::string valType;

        for (int i = 0; i < numVals; ++i) {
            valType = values[i].type;
            if (valType == "int") {
                memmove(data, &values[i].intVal, INT_BYTES);
                data += INT_BYTES;
            } else if (valType == "float") {
                memmove(data, &values[i].floatVal, FLOAT_BYTES);
                data += FLOAT_BYTES;
            } else if (valType == "str") {
                memmove(data, values[i].stringVal, values[i - 1].intVal);
                data += values[i - 1].intVal;
            }
        }
    }

    RC RelationManager::addTablesEntry(FileHandle &fh, int table_id, int tableNameLen, const char *tableName, int fileNameLen, const char *fileName, int isSystem, char *data) {
        std::vector<tupleVal> values{tupleVal{table_id}, tupleVal{tableNameLen}, tupleVal{tableName}, tupleVal{fileNameLen}, tupleVal{fileName}, tupleVal{isSystem}};
        craftTupleData(data + 1, values);
        RID rid;
        return RecordBasedFileManager::instance().insertRecord(fh, tablesDescriptor, data, rid);
    }

    RC RelationManager::initTablesTable(FileHandle &fh) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        char data[50];
        if (rbfm.openFile("Tables", fh) == -1) return -1;
        memset(data, 0, 1);

        if (addTablesEntry(fh, 1, 6, "Tables", 6, "Tables", 1, data) == -1) return -1;
        if (addTablesEntry(fh, 2, 7, "Columns", 7, "Columns", 1, data) == -1) return -1;

        return rbfm.closeFile(fh);
    }

    RC RelationManager::addColumnsEntry(FileHandle &fh, int table_id, int nameLen, const char *name, AttrType columnType, int columnLen, int pos, char *data) {
        std::vector<tupleVal> values{tupleVal{table_id}, tupleVal{nameLen}, tupleVal{name}, tupleVal{columnType}, tupleVal{columnLen}, tupleVal{pos}};
        craftTupleData(data + 1, values);
        RID rid;
        return RecordBasedFileManager::instance().insertRecord(fh, columnsDescriptor, data, rid);
    }

    RC RelationManager::initColumnsTable(FileHandle &fh) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        char data[50];
        if (rbfm.openFile("Columns", fh) == -1) return -1;
        memset(data, 0, 1);

        if (addColumnsEntry(fh, 1, 8, "table-id", TypeInt, 4, 1, data) == -1) return -1;
        if (addColumnsEntry(fh, 1, 10, "table-name", TypeVarChar, 50, 2, data) == -1) return -1;
        if (addColumnsEntry(fh, 1, 9, "file-name", TypeVarChar, 50, 3, data) == -1) return -1;
        if (addColumnsEntry(fh, 1, 15, "is-system-table", TypeInt, 4, 4, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 8, "table-id", TypeInt, 4, 1, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 11, "column-name", TypeVarChar, 50, 2, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 11, "column-type", TypeInt, 4, 3, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 13, "column-length", TypeInt, 4, 4, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 15, "column-position", TypeInt, 4, 5, data) == -1) return -1;

        return rbfm.closeFile(fh);
    }

    RC RelationManager::createCatalog() {
        // create files to store records for Tables and Columns, error if either already exists
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.createFile("Tables") == -1) return -1;
        if (rbfm.createFile("Columns") == -1) return -1;
        FileHandle fh;
        nextTableID = 3;  // reset table ID tracker every time catalog is made

        if (initTablesTable(fh) == -1) return -1;
        return initColumnsTable(fh);
    }

    RC RelationManager::deleteCatalog() {
        return -1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.createFile(tableName) == -1) return -1;  // error if table already exists
        FileHandle fh;
        char data[1024];
        memset(data, 0, 1);

        if (rbfm.openFile("Tables", fh) == -1) return -1;
        if (addTablesEntry(fh, nextTableID, tableName.size(), tableName.c_str(), tableName.size(), tableName.c_str(), 0, data) == -1) return -1;
        if (rbfm.closeFile(fh) == -1) return -1;

        if (rbfm.openFile("Columns", fh) == -1) return -1;
        int pos = 1;
        for (Attribute attr : attrs) {
            if (addColumnsEntry(fh, nextTableID, attr.name.size(), attr.name.c_str(), attr.type,attr.length, pos, data) == -1) return -1;
            ++pos;
        }
        ++nextTableID;
        return rbfm.closeFile(fh);
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB