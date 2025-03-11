#include "src/include/rm.h"
#include <cstring>
#include <map>
#include <fstream>
#include <iostream>

constexpr int INT_BYTES = 4;
constexpr int FLOAT_BYTES = 4;
constexpr int BITS_PER_BYTE = 8;

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

        schemasDescriptor.push_back(Attribute{"table-id", TypeInt, 4});
        schemasDescriptor.push_back(Attribute{"version", TypeInt, 4});
        schemasDescriptor.push_back(Attribute{"fields", TypeVarChar, 100});

        indicesDescriptor.push_back(Attribute{"table-id", TypeInt, 4});
        indicesDescriptor.push_back(Attribute{"attribute-name", TypeVarChar, 50});
        indicesDescriptor.push_back(Attribute{"file-name", TypeVarChar, 50});

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
        if (addTablesEntry(fh, 3, 7, "Schemas", 7, "Schemas", 1, data) == -1) return -1;
        if (addTablesEntry(fh, 4, 7, "Indices", 7, "Indices", 1, data) == -1) return -1;

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
        if (addColumnsEntry(fh, 2, 8, "table-id", TypeInt, 4, 1, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 11, "column-name", TypeVarChar, 50, 2, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 11, "column-type", TypeInt, 4, 3, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 13, "column-length", TypeInt, 4, 4, data) == -1) return -1;
        if (addColumnsEntry(fh, 2, 15, "column-position", TypeInt, 4, 5, data) == -1) return -1;
        if (addColumnsEntry(fh, 1, 15, "is-system-table", TypeInt, 4, 4, data) == -1) return -1;
        if (addColumnsEntry(fh, 3, 8, "table-id", TypeInt, 4, 1, data) == -1) return -1;
        if (addColumnsEntry(fh, 3, 7, "version", TypeInt, 4, 2, data) == -1) return -1;
        if (addColumnsEntry(fh, 3, 6, "fields", TypeVarChar, 100, 3, data) == -1) return -1;
        if (addColumnsEntry(fh, 4, 8, "table-id", TypeInt, 4, 1, data) == -1) return -1;
        if (addColumnsEntry(fh, 4, 14, "attribute-name", TypeVarChar, 50, 2, data) == -1) return -1;
        if (addColumnsEntry(fh, 4, 9, "file-name", TypeVarChar, 50, 3, data) == -1) return -1;
        return rbfm.closeFile(fh);
    }

    RC RelationManager::addSchemasEntry(FileHandle &fh, int table_id, int version, int fieldCount, const char *fields, char *data) {
        std::vector<tupleVal> values{tupleVal{table_id}, tupleVal{version}, tupleVal{fieldCount}, tupleVal{fields}};
        craftTupleData(data + 1, values);
        RID rid;
        return RecordBasedFileManager::instance().insertRecord(fh, schemasDescriptor, data, rid);
    }

    void RelationManager::formatString(const std::string &str, char *value) {
        int len = str.size();
        memmove(value, &len, INT_BYTES);
        memmove(value + INT_BYTES, str.c_str(), len);
    }

    RC RelationManager::createCatalog() {
        // create files to store records for Tables and Columns, error if either already exists
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.createFile("Tables") == -1) return -1;
        if (rbfm.createFile("Columns") == -1) return -1;
        if (rbfm.createFile("Schemas") == -1) return -1;
        if (rbfm.createFile("Indices") == -1) return -1;
        FileHandle fh;
        nextTableID = 5;  // reset table ID tracker every time catalog is made

        if (initTablesTable(fh) == -1) return -1;
        return initColumnsTable(fh);
    }

    RC RelationManager::deleteCatalog() {
        RC status = 0;
        if (RecordBasedFileManager::instance().destroyFile("Tables") == -1) status = -1;
        if (RecordBasedFileManager::instance().destroyFile("Columns") == -1) status = -1;
        if (RecordBasedFileManager::instance().destroyFile("Schemas") == -1) status = -1;
        if (RecordBasedFileManager::instance().destroyFile("Indices") == -1) status = -1;
        return status;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        // verify Tables and Columns are present to be modified
        std::ifstream ifs{"Tables"};
        if (!ifs.is_open()) return -1;
        ifs.close(); ifs.open("Columns");
        if (!ifs.is_open()) return -1;
        ifs.close(); ifs.open("Schemas");
        if (!ifs.is_open()) return -1;
        ifs.close();

        if (rbfm.createFile(tableName) == -1) return -1;  // error if table already exists
        FileHandle fh;
        char data[150];
        memset(data, 0, 1);

        if (rbfm.openFile("Tables", fh) == -1) return -1;
        if (addTablesEntry(fh, nextTableID, tableName.size(), tableName.c_str(), tableName.size(), tableName.c_str(), 0, data) == -1) return -1;
        if (rbfm.closeFile(fh) == -1) return -1;

        if (rbfm.openFile("Columns", fh) == -1) return -1;
        char positions[attrs.size() + 1];
        positions[attrs.size()] = '\0';
        int pos = 1;
        for (Attribute attr : attrs) {
            if (addColumnsEntry(fh, nextTableID, attr.name.size(), attr.name.c_str(), attr.type,attr.length, pos, data) == -1) return -1;
            positions[pos - 1] = static_cast<char>(pos);
            ++pos;
        }
        if (rbfm.closeFile(fh) == -1) return -1;

        if (rbfm.openFile("Schemas", fh) == -1) return -1;
        if (addSchemasEntry(fh, nextTableID, 1, attrs.size(), positions, data) == -1) return -1;
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
        char value[tableName.size() + INT_BYTES];
        formatString(tableName, value);
        if (scan(tables, table_name_field, EQ_OP, value, neededAttributes, scanner) == -1) {scanner.close(); return -1;}
        RID rid;
        char data[15];
        if (scanner.getNextTuple(rid, data) == RM_EOF) {scanner.close(); return -1;}
        memmove(&tableID, data + 1, INT_BYTES);
        if (isSystemTable != nullptr) memmove(isSystemTable, data + (1 + INT_BYTES), INT_BYTES);

        if (scanner.close() == -1) return -1;
        if (deleteEntry && isSystemTable && *isSystemTable == 0) {
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
        std::string schemas{"Schemas"};
        std::string table_id_str{"table-id"};
        int tableID, isSystemTable;
        // retrieve the table id and remove corresponding record from Tables table
        if (getTableID(tableName, tableID, true, &isSystemTable) == -1) return -1;
        if (isSystemTable == 1) return -1;
        RID rid;
        char data[60];

        // scan through the Columns table and delete all records associated to the table ID
        std::vector<std::string> requestedAttributes;
        requestedAttributes.emplace_back("table-id");
        if (scan(columns, table_id_str, EQ_OP, &tableID, requestedAttributes, scanner) == -1) {scanner.close(); return -1;}

        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        std::vector<RID> recordsToDelete;
        while (scanner.getNextTuple(rid, data) != RM_EOF)
            recordsToDelete.push_back(rid);
        if (scanner.close() == -1) return -1;

        FileHandle fh;
        if (rbfm.openFile(columns, fh) == -1) return -1;
        for (auto recoid : recordsToDelete) {
            if (rbfm.deleteRecord(fh, columnsDescriptor, recoid) == -1) return -1;
        }
        if (rbfm.closeFile(fh) == -1) return -1;

        // scan through the Schemas table to find all schema version records related to this table
        if (scan(schemas, table_id_str, EQ_OP, &tableID, requestedAttributes, scanner) == -1) {scanner.close(); return -1;}
        recordsToDelete.clear();
        while (scanner.getNextTuple(rid, data) != RM_EOF)
            recordsToDelete.push_back(rid);
        if (scanner.close() == -1) return -1;

        // delete those records from Schemas
        if (rbfm.openFile(schemas, fh) == -1) return -1;
        for (auto recoid : recordsToDelete) {
            if (rbfm.deleteRecord(fh, schemasDescriptor, recoid) == -1) return -1;
        }
        if (rbfm.closeFile(fh) == -1) return -1;

        // scan through Indices to find all index file records related to the table to be deleted, destroy the index files as we go
        requestedAttributes.emplace_back("file-name");
        if (scan("Indices", table_id_str, EQ_OP, &tableID, requestedAttributes, scanner) == -1) {scanner.close(); return -1;}
        recordsToDelete.clear();
        while (scanner.getNextTuple(rid, data) != RM_EOF) {
            recordsToDelete.push_back(rid);
            if (IndexManager::instance().destroyFile(std::string{data + (1 + INT_BYTES + INT_BYTES),
                                                     *reinterpret_cast<unsigned *>(data + (1 + INT_BYTES))}) == -1) return -1;
        }
        if (scanner.close() == -1) return -1;

        // delete those records from Indices
        if (rbfm.openFile("Indices", fh) == -1) return -1;
        for (auto recoid : recordsToDelete) {
            if (rbfm.deleteRecord(fh, schemasDescriptor, recoid) == -1) return -1;
        }
        if (rbfm.closeFile(fh) == -1) return -1;

        return RecordBasedFileManager::instance().destroyFile(tableName);
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, int *isSystemTable, int *version, std::unordered_map<std::string, int> *attrPositions) {
        attrs = std::vector<Attribute>{};
        if (tableName == "Tables" || tableName == "Columns" || tableName == "Schemas" || tableName == "Indices") {
            if (tableName == "Tables") attrs = tablesDescriptor;
            else if (tableName == "Columns") attrs = columnsDescriptor;
            else if (tableName == "Schemas") attrs = schemasDescriptor;
            else if (tableName == "Indices") attrs = indicesDescriptor;

            if (attrPositions) {
                attrPositions->clear();
                for (int i = 0; i < attrs.size(); ++i) {
                    (*attrPositions)[attrs[i].name] = i + 1;
                }
            }
            if (version) *version = 1;
            if (isSystemTable) *isSystemTable = 1;
            return 0;
        }

        std::unordered_set<int> positions;
        std::unordered_map<std::string, int> placeHolder;
        int tableID, lastPos;
        int zero = 0;
        if (version == nullptr) version = &zero;
        if (attrPositions == nullptr) {
            if (getSchemaVersionInfo(tableName, tableID, *version, lastPos, placeHolder, positions) == -1) return -1;
        } else {
            if (getSchemaVersionInfo(tableName, tableID, *version, lastPos, *attrPositions, positions) == -1) return -1;
        }

        RM_ScanIterator scanner;
        std::string columns{"Columns"};
        std::string columns_table_id{"table-id"};
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
            if (positions.find(pos) != positions.end())
                attrOrdering[pos] = attr;
        }
        for (auto const & pair : attrOrdering)
            attrs.push_back(pair.second);
        return scanner.close();
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        std::vector<Attribute> recordDescriptor;
        int isSystemTable = 0, version = 0;
        if (getAttributes(tableName, recordDescriptor, &isSystemTable, &version) == -1) return -1;
        if (isSystemTable == 1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.insertRecord(fh, recordDescriptor, data, rid, version) == -1) {rbfm.closeFile(fh); return -1;}
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
        int isSystemTable = 0, version = 0;
        if (getAttributes(tableName, recordDescriptor, &isSystemTable, &version) == -1) return -1;
        if (isSystemTable == 1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;

        if (rbfm.updateRecord(fh, recordDescriptor, data, rid, version) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    void RelationManager::convertDataToCurrSchema(void *data, const std::vector<Attribute> &currDescriptor, const std::vector<Attribute> &recordDescriptor,
                                                  std::unordered_map<std::string, int> &currAttrPos, std::unordered_map<std::string, int> &recoVersionAttrPos) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        char convertedData[PAGE_SIZE];
        SizeType nullByteCount = rbfm.nullBytesNeeded(currDescriptor.size());
        memset(convertedData, 0, nullByteCount);
        char *convertedPtr = convertedData + nullByteCount;
        const char *dataPtr = static_cast<const char *>(data) + rbfm.nullBytesNeeded(recordDescriptor.size());
        int nullByteNum = 0;
        int bitNum = 1;
        int recoDescIndex = 0;
        unsigned char dataNullByte;

        for (const Attribute &neededAttr : currDescriptor) {
            int neededPos = currAttrPos[neededAttr.name];
            bool loopBroken = false;

            for (; recoDescIndex < recordDescriptor.size(); ++recoDescIndex) {
                if (recoDescIndex % BITS_PER_BYTE == 0)
                    memmove(&dataNullByte, static_cast<char *>(data) + recoDescIndex / BITS_PER_BYTE, 1);
                Attribute recoAttr = recordDescriptor[recoDescIndex];
                int recoAttrPos = recoVersionAttrPos[recoAttr.name];
                if (recoAttrPos > neededPos) {
                    convertedData[nullByteNum] |= 1 << (BITS_PER_BYTE - bitNum);
                    loopBroken = true;
                    break;
                }

                if (rbfm.nullBitOn(dataNullByte, recoDescIndex % BITS_PER_BYTE + 1)) {
                    if (recoAttrPos == neededPos)
                        convertedData[nullByteNum] |= 1 << (BITS_PER_BYTE - bitNum);
                } else {
                    if (recoAttr.type == TypeVarChar) {
                        int varcharLen;
                        memmove(&varcharLen, dataPtr, INT_BYTES);
                        if (recoAttrPos == neededPos) {
                            memmove(convertedPtr, dataPtr, INT_BYTES + varcharLen);
                            convertedPtr += INT_BYTES + varcharLen;
                        }
                        dataPtr += INT_BYTES + varcharLen;
                    } else {
                        if (recoAttrPos == neededPos) {
                            memmove(convertedPtr, dataPtr, recoAttr.length);
                            convertedPtr += recoAttr.length;
                        }
                        dataPtr += recoAttr.length;
                    }
                }

                if (recoAttrPos == neededPos) {
                    ++recoDescIndex;
                    loopBroken = true;
                    break;
                }
            }

            if (recoDescIndex == recordDescriptor.size() && !loopBroken)
                convertedData[nullByteNum] |= 1 << (BITS_PER_BYTE - bitNum);
            bitNum = bitNum % BITS_PER_BYTE + 1;
            if (bitNum == 1) ++nullByteNum;
        }
        memmove(data, convertedData, convertedPtr - convertedData);
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> currentDescriptor;
        std::unordered_map<std::string, int> attrToPos;
        int currVersion = 0;
        if (getAttributes(tableName, currentDescriptor, nullptr, &currVersion, &attrToPos) == -1) return -1;
        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;
        SizeType version;
        if (rbfm.readRecord(fh, currentDescriptor, rid, data, &version) == -1) {rbfm.closeFile(fh); return -1;}

        if (version == currVersion) {
            if (rbfm.readRecord(fh, currentDescriptor, rid, data) == -1) {rbfm.closeFile(fh); return -1;}
        } else {
            std::vector<Attribute> recordDescriptor;
            std::unordered_map<std::string, int> recordVersionAttrPos;
            int versionInt = static_cast<int>(version);
            if (getAttributes(tableName, recordDescriptor, nullptr, &versionInt, &recordVersionAttrPos) == -1) {rbfm.closeFile(fh); return -1;}
            if (rbfm.readRecord(fh, recordDescriptor, rid, data) == -1) {rbfm.closeFile(fh); return -1;}
            convertDataToCurrSchema(data, currentDescriptor, recordDescriptor, attrToPos, recordVersionAttrPos);
        }

        return rbfm.closeFile(fh);
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return RecordBasedFileManager::instance().printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute> currentDescriptor;
        std::unordered_map<std::string, int> attrToPos;
        int currVersion = 0;
        if (getAttributes(tableName, currentDescriptor, nullptr, &currVersion, &attrToPos) == -1) return -1;

        FileHandle fh;
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        if (rbfm.openFile(tableName, fh) == -1) return -1;
        SizeType version;
        if (rbfm.readAttribute(fh, currentDescriptor, rid, attributeName, data, &version) == -1) {rbfm.closeFile(fh); return -1;}

        if (version == currVersion) {
            if (rbfm.readAttribute(fh, currentDescriptor, rid, attributeName, data) == -1) {rbfm.closeFile(fh); return -1;}
        } else {
            std::vector<Attribute> recordDescriptor;
            std::unordered_map<std::string, int> recordVersionAttrPos;
            int versionInt = static_cast<int>(version);
            if (getAttributes(tableName, recordDescriptor, nullptr, &versionInt, &recordVersionAttrPos) == -1) {rbfm.closeFile(fh); return -1;}

            if (recordVersionAttrPos.find(attributeName) != recordVersionAttrPos.end() && attrToPos[attributeName] == recordVersionAttrPos[attributeName]) {
                if (rbfm.readAttribute(fh, recordDescriptor, rid, attributeName, data) == -1) {rbfm.closeFile(fh); return -1;}
            } else
                memset(data, 128, 1);
        }

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
        std::unordered_map<std::string, int> attrToPos;
        int version = 0;
        if (getAttributes(tableName, attrs, nullptr, &version, &attrToPos) == -1) return -1;
        rm_ScanIterator.init(tableName, *fh, attrs, conditionAttribute, compOp, value, attributeNames, version, attrToPos);
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    void RM_ScanIterator::init(const std::string &tableName, FileHandle & fHandle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute,
                  const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames, int version, const std::unordered_map<std::string, int> &attrToPos) {
        this->tableName = tableName;
        comparator = compOp;
        schemaVersion = version;
        attrDescriptor = recordDescriptor;
        attrPositions = attrToPos;
        conditionAttrName = conditionAttribute;
        recordScanner.init(fHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        SizeType nextRecoVersion;
        bool recoAccepted, verifyRecord;

        while (true) {
            if (recordScanner.getNextRecord(rid, data, &nextRecoVersion) == -1) return -1;

            if (nextRecoVersion == schemaVersion) {
                recordScanner.recordDescriptor = attrDescriptor;
                recordScanner.attrNameIndexes.clear();
                for (int i = 0; i < attrDescriptor.size(); ++i)
                    recordScanner.attrNameIndexes[attrDescriptor[i].name] = i;
                verifyRecord = true;
            } else {
                std::vector<Attribute> recoDescriptor;
                std::unordered_map<std::string, int> recoAttrToPos;
                int nextRecoVersionInt = static_cast<int>(nextRecoVersion);
                if (RelationManager::instance().getAttributes(tableName, recoDescriptor, nullptr, &nextRecoVersionInt, &recoAttrToPos) == -1) return -1;
                recordScanner.attrNameIndexes.clear();
                for (int i = 0; i < recoDescriptor.size(); ++i)
                    recordScanner.attrNameIndexes[recoDescriptor[i].name] = i;
                verifyRecord = (comparator == NO_OP) || (recoAttrToPos.find(conditionAttrName) != recoAttrToPos.end() && recoAttrToPos[conditionAttrName] == attrPositions[conditionAttrName]);
                for (std::string &aname : recordScanner.attributeNames) {
                    if (recoAttrToPos.find(aname) == recoAttrToPos.end() || recoAttrToPos[aname] != attrPositions[aname])
                        aname.clear();
                }
            }

            if (recordScanner.getNextRecord(rid, data, nullptr, &recoAccepted, &verifyRecord) == -1) return -1;
            if (recoAccepted) return 0;
        }
    }

    RC RM_ScanIterator::close() {
        return recordScanner.close();
    }

    // Extra credit work
    RC RelationManager::getSchemaVersionInfo(const std::string &tableName, int &tableID, int &version, int &pos, std::unordered_map<std::string, int> &names, std::unordered_set<int> &positions) {
        bool specificVersion = version != 0;
        names.clear();
        positions.clear();
        int isSystemTable = 0;
        if (getTableID(tableName, tableID, false, &isSystemTable) == -1) return -1;
        if (isSystemTable == 1) return -1;
        RM_ScanIterator scanner;
        std::vector<std::string> neededValues{"version", "fields"};
        if (scan("Schemas", "table-id", EQ_OP, &tableID, neededValues, scanner) == -1) {scanner.close(); return -1;}

        char data[120];
        int numFields = -1;
        int newestVersion = specificVersion ? version : 0;
        unsigned char fields[110];
        RID rid;

        while (scanner.getNextTuple(rid, data) != RM_EOF) {
            memmove(&version, data + 1, INT_BYTES);
            if (specificVersion) {
                if (version == newestVersion) {
                    memmove(&numFields, data + (1 + INT_BYTES), INT_BYTES);
                    memmove(fields, data + (1 + INT_BYTES + INT_BYTES), numFields);
                    break;
                }
            } else {
                if (version > newestVersion) {
                    newestVersion = version;
                    memmove(&numFields, data + (1 + INT_BYTES), INT_BYTES);
                    memmove(fields, data + (1 + INT_BYTES + INT_BYTES), numFields);
                }
            }
        }
        if (scanner.close() == -1 || numFields == -1) return -1;
        version = newestVersion;
        neededValues = {"column-name", "column-position"};
        if (scan("Columns", "table-id", EQ_OP, &tableID, neededValues, scanner) == -1) {scanner.close(); return -1;}

        for (int i = 0; i < numFields; ++i)
            positions.insert(fields[i]);
        int max_pos = 0;
        int nameLen;

        while (scanner.getNextTuple(rid, data) != RM_EOF) {
            memmove(&nameLen, data + 1, INT_BYTES);
            memmove(&pos, data + (1 + INT_BYTES + nameLen), INT_BYTES);
            if (pos > max_pos)
                max_pos = pos;
            if (positions.find(pos) != positions.end()) {
                data[1 + INT_BYTES + nameLen] = '\0';
                names[data + (1 + INT_BYTES)] = pos;
            }
        }
        pos = max_pos;
        return scanner.close();
    }

    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        std::unordered_map<std::string, int> currVersionAttrs;
        std::unordered_set<int> currVersionPositions;
        int currPos, currVersion = 0, tableID;
        if (getSchemaVersionInfo(tableName, tableID, currVersion, currPos, currVersionAttrs, currVersionPositions) == -1) return -1;
        if (currVersionAttrs.find(attributeName) == currVersionAttrs.end()) return -1;  // cannot drop attribute that is not in schema

        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        FileHandle fh;

        if (rbfm.openFile("Schemas", fh) == -1) return -1;
        currVersionPositions.erase(currVersionPositions.find(currVersionAttrs[attributeName]));
        char newPositions[currVersionPositions.size() + 1];
        int i = 0;
        for (int newPos : currVersionPositions)
            newPositions[i++] = static_cast<char>(newPos);
        newPositions[i] = '\0';
        char data[150];
        memset(data, 0, 1);
        if (addSchemasEntry(fh, tableID, currVersion + 1, currVersionPositions.size(), newPositions, data) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        std::unordered_map<std::string, int> currVersionAttrs;
        std::unordered_set<int> currVersionPositions;
        int currPos, currVersion = 0, tableID;
        if (getSchemaVersionInfo(tableName, tableID, currVersion, currPos, currVersionAttrs, currVersionPositions) == -1) return -1;
        if (currVersionAttrs.find(attr.name) != currVersionAttrs.end()) return -1;  // attribute already in current schema

        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        currVersionPositions.insert(currPos + 1);
        FileHandle fh;
        if (rbfm.openFile("Columns", fh) == -1) return -1;
        char data[150];
        memset(data, 0, 1);
        if (addColumnsEntry(fh, tableID, attr.name.size(), attr.name.c_str(), attr.type, attr.length, currPos + 1, data) == -1) {rbfm.closeFile(fh); return -1;}
        if (rbfm.closeFile(fh) == -1) return -1;

        if (rbfm.openFile("Schemas", fh) == -1) return -1;
        char newPositions[currVersionPositions.size() + 1];
        int i = 0;
        for (int newPos : currVersionPositions)
            newPositions[i++] = static_cast<char>(newPos);
        newPositions[i] = '\0';
        if (addSchemasEntry(fh, tableID, currVersion + 1, currVersionPositions.size(), newPositions, data) == -1) {rbfm.closeFile(fh); return -1;}
        return rbfm.closeFile(fh);
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