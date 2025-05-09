#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <unordered_map>
#include "pfm.h"

namespace PeterDB {
    using SizeType = unsigned short;

    // Record ID
    struct RID {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page

        bool operator < (const RID & other) const {
            return pageNum < other.pageNum || (pageNum == other.pageNum && slotNum < other.slotNum);
        }

        bool operator == (const RID & other) const {
            return pageNum == other.pageNum && slotNum == other.slotNum;
        }

        bool operator <= (const RID & other) const {
            return pageNum < other.pageNum || (pageNum == other.pageNum && slotNum <= other.slotNum);
        }
    };

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
        friend class RM_ScanIterator;
        FileHandle *fileHandle;
        std::vector<Attribute> recordDescriptor;
        std::unordered_map<std::string, int> attrNameIndexes;
        std::string conditionAttribute;
        AttrLength conditionAttrLen;
        CompOp compOp;
        AttrType valueType;
        int valueInt;
        float valueReal;
        std::string valueString;
        std::vector<std::string> attributeNames;
        bool firstScan;
        unsigned lastPageNum;
        unsigned short lastSlotNum;

        bool acceptedRecord(unsigned pageNum, unsigned short slotNum);
        void extractRecordData(const char * recordData, void * data);
        bool compareInt(int conditionAttr);
        bool compareReal(float conditionAttr);
        bool compareVarchar(const std::string & conditionAttr);

    public:
        RBFM_ScanIterator() : fileHandle(nullptr) {}

        ~RBFM_ScanIterator() = default;

        void init(FileHandle & fHandle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute,
                  const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames);
        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data, SizeType *version = nullptr, bool *recoAccepted = nullptr, bool *verifyRecord = nullptr);

        RC close();
    };

    class RecordBasedFileManager {
        friend class RBFM_ScanIterator;
        friend class RelationManager;
        friend class Filter;
        friend class Project;
        friend class Aggregate;
        friend class BNLJoin;
        friend class INLJoin;
        friend class GHJoin;

    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid, SizeType version = 1);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data, SizeType *version = nullptr);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid, SizeType version = 1);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data, SizeType *version = nullptr);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

        // helper functions for insertRecord method
        SizeType calcRecordSpace(const std::vector<Attribute> &recordDescriptor, const void * data);
        SizeType putRecordInEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, SizeType recordSpace, SizeType version);
        SizeType putRecordInNonEmptyPage(const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, SizeType recordSpace, SizeType version);
        void embedRecord(SizeType offset, const std::vector<Attribute> &recordDescriptor, const void * data, void * pageData, SizeType version);
        void getFreeSpace(SizeType * freeSpace, const void * pageData);
        void setFreeSpace(SizeType * freeSpace, void * pageData);
        void setSlotCount(SizeType * slotCount, void * pageData);
        void getFreeSpaceAndSlotCount(SizeType * freeSpace, SizeType * slotCount, const void * pageData);
        void setFreeSpaceAndSlotCount(SizeType * freeSpace, SizeType * slotCount, void * pageData);
        void setSlotOffset(SizeType * offset, SizeType slotNum, void * pageData);
        void getSlotLen(SizeType * len, SizeType slotNum, const void * pageData);
        void setSlotLen(SizeType * len, SizeType slotNum, void * pageData);
        void setSlotOffsetAndLen(SizeType * offset, SizeType * len, SizeType slotNum, void * pageData);
        SizeType assignSlot(const void * pageData);
        bool fitsOnPage(SizeType recordSpace, const void * pageData);
        void shiftRecordsLeft(SizeType shiftPoint, SizeType shiftDistance, void * pageData);
        void shiftRecordsRight(SizeType shiftPoint, SizeType shiftDistance, void * pageData);
        RC deleteTombstone(FileHandle &fileHandle, char *pageData, unsigned pageNum, unsigned short slotNum, SizeType tombstoneOffset, SizeType tombstoneLen);
        RC findRealRecord(FileHandle &fileHandle, char *pageData, unsigned & pageNum, unsigned short & slotNum, SizeType & recoOffset, SizeType & recoLen, bool removeTombstones);
        void getSlotCount(SizeType * slotCount, const void * pageData);
        void getSlotOffset(SizeType * offset, SizeType slotNum, const void * pageData);
        SizeType nullBytesNeeded(SizeType numFields);
        bool nullBitOn(unsigned char nullByte, int bitNum);
        void getSlotOffsetAndLen(SizeType * offset, SizeType * len, SizeType slotNum, const void * pageData);
    };

} // namespace PeterDB

#endif // _rbfm_h_
