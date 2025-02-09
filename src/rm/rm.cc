#include "src/include/rm.h"
#include <cstring>
#include <map>
#include <fstream>
#include <iostream>

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
        columnsColumns.emplace_back("table-id");
        columnsColumns.emplace_back("column-name");
        columnsColumns.emplace_back("column-type");
        columnsColumns.emplace_back("column-length");
        columnsColumns.emplace_back("column-position");
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
                memmove(data, values[i].stringVal.c_str(), values[i - 1].intVal);
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

    void RelationManager::formatString(const std::string &str, char *value) {
        int len = str.size();
        memmove(value, &len, INT_BYTES);
        strcpy(value + INT_BYTES, str.c_str());
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
        RC status = 0;
        if (RecordBasedFileManager::instance().destroyFile("Tables") == -1) status = -1;
        if (RecordBasedFileManager::instance().destroyFile("Columns") == -1) status = -1;
        return status;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        // verify Tables and Columns are present to be modified
        std::ifstream ifs{"Tables"};
        if (!ifs.is_open()) return -1;
        ifs.close(); ifs.open("Columns");
        if (!ifs.is_open()) return -1;
        ifs.close();

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

    RC RelationManager::getTableID(const std::string &tableName, int &tableID, bool deleteEntry, int *isSystemTable) {
        RM_ScanIterator scanner;
        std::string tables{"Tables"};
        std::string table_name_field{"table-name"};

        // first, scan through Tables table to get id of tableName
        std::vector<std::string> neededAttributes;
        neededAttributes.emplace_back("table-id");
        neededAttributes.emplace_back("is-system-table");
        char value[tableName.size() + 1 + INT_BYTES];
        formatString(tableName, value);
        if (scan(tables, table_name_field, EQ_OP, value, neededAttributes, scanner) == -1) {scanner.close(); return -1;}
        RID rid;
        char data[15];
        if (scanner.getNextTuple(rid, data) == RM_EOF) {scanner.close(); return -1;}
        memmove(&tableID, data + 1, INT_BYTES);
        if (isSystemTable != nullptr) memmove(isSystemTable, data + (1 + INT_BYTES), INT_BYTES);

        if (scanner.close() == -1) return -1;
        if (deleteEntry) {
            FileHandle fh;
            if (RecordBasedFileManager::instance().openFile(tables, fh) == -1) return -1;
            if (RecordBasedFileManager::instance().deleteRecord(fh, tablesDescriptor, rid) == -1) return -1;
            return RecordBasedFileManager::instance().closeFile(fh);
        }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RM_ScanIterator scanner;
        std::string columns{"Columns"};
        std::string columns_table_id{"table-id"};
        int tableID, isSystemTable;
        if (getTableID(tableName, tableID, true, &isSystemTable) == -1) return -1;
        if (isSystemTable == 1) return -1;
        RID rid;
        char data[10];

        // scan through the Columns table and delete all records associated to the table ID
        std::vector<std::string> requestedAttributes;
        requestedAttributes.emplace_back("table-id");
        if (scan(columns, columns_table_id, EQ_OP, &tableID, requestedAttributes, scanner) == -1) {scanner.close(); return -1;}

        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        std::vector<RID> recordsToDelete;
        while (scanner.getNextTuple(rid, data) != RM_EOF) {
            recordsToDelete.push_back(rid);
        }
        if (scanner.close() == -1) return -1;

        FileHandle fh;
        if (rbfm.openFile(columns, fh) == -1) return -1;
        for (auto recoid : recordsToDelete) {
            if (rbfm.deleteRecord(fh, columnsDescriptor, recoid) == -1) return -1;
        }
        if (rbfm.closeFile(fh) == -1) return -1;

        return RecordBasedFileManager::instance().destroyFile(tableName);
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, int *isSystemTable) {
        attrs = std::vector<Attribute>{};
        RM_ScanIterator scanner;
        std::string columns{"Columns"};
        std::string columns_table_id{"table-id"};
        int tableID;
        if (getTableID(tableName, tableID, false, isSystemTable) == -1) return -1;
        RID rid;
        char data[100];

        // now, scan through columns table searching for the table ID we now have
        if (scan(columns, columns_table_id, EQ_OP, &tableID, columnsColumns, scanner) == -1) {scanner.close(); return -1;}
        std::map<int, Attribute> attrOrdering;
        while (scanner.getNextTuple(rid, data) != RM_EOF) {
            char *ptr = data + (1 + INT_BYTES);
            Attribute attr;
            int nameLen;
            memmove(&nameLen, ptr, INT_BYTES);
            ptr += INT_BYTES;
            char name[nameLen + 1];
            memmove(name, ptr, nameLen);
            name[nameLen] = '\0';
            attr.name = name;
            ptr += nameLen;
            memmove(&attr.type, ptr, INT_BYTES);
            ptr += INT_BYTES;
            memmove(&attr.length, ptr, INT_BYTES);
            ptr += INT_BYTES;
            int pos;
            memmove(&pos, ptr, INT_BYTES);
            attrOrdering[pos] = attr;
        }
        for (auto const & pair : attrOrdering)
            attrs.push_back(pair.second);
        return scanner.close();
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        std::vector<Attribute> recordDescriptor;
        int isSystemTable = 0;
        if (getAttributes(tableName, recordDescriptor, &isSystemTable) == -1) return -1;
        if (isSystemTable == 1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.insertRecord(fh, recordDescriptor, data, rid) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        std::vector<Attribute> recordDescriptor;
        int isSystemTable = 0;
        if (getAttributes(tableName, recordDescriptor, &isSystemTable) == -1) return -1;
        if (isSystemTable == 1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.deleteRecord(fh, recordDescriptor, rid) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        std::vector<Attribute> recordDescriptor;
        int isSystemTable = 0;
        if (getAttributes(tableName, recordDescriptor, &isSystemTable) == -1) return -1;
        if (isSystemTable == 1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.updateRecord(fh, recordDescriptor, data, rid) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.readRecord(fh, recordDescriptor, rid, data) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return RecordBasedFileManager::instance().printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) == -1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.readAttribute(fh, recordDescriptor, rid, attributeName, data) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        FileHandle *fh = new FileHandle{};
        if (RecordBasedFileManager::instance().openFile(tableName, *fh) == -1) {
            delete fh;
            return -1;
        }

        std::vector<Attribute> attrs;
        if (tableName == "Tables") attrs = tablesDescriptor;
        else if (tableName == "Columns") attrs = columnsDescriptor;
        else {if (getAttributes(tableName, attrs) == -1) return -1;}
        rm_ScanIterator.init(*fh, attrs, conditionAttribute, compOp, value, attributeNames);
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    void RM_ScanIterator::init(FileHandle & fHandle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute,
                  const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames) {
        recordScanner.init(fHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return recordScanner.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        return recordScanner.close();
    }

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