#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    };

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    struct Value {
        AttrType type;          // type of value
        void *data;             // value
    };

    struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    };

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = nullptr)
            : rm(rm), rid() {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, nullptr, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        }

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, nullptr, attrNames, iter);
        }

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        }

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        }

        ~TableScan() override {
            iter.close();
        }
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = nullptr) : rm(rm), rid() {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, nullptr, nullptr, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        }

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        }

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        }

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        }

        ~IndexScan() override {
            iter.close();
        }
    };

    class Filter : public Iterator {
        // Filter operator
        Iterator & iter;
        const Condition & cond;
        std::vector<Attribute> attrs;

        bool validTuple(void *data, SizeType nullBytes);
        bool compareValues(void *left, void *right, AttrType type);

    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Project : public Iterator {
        // Projection operator
        Iterator & iter;
        std::vector<Attribute> attrs;
        std::unordered_set<std::string> projectAttrNames;
        std::vector<Attribute> projectAttrs;

    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
        Iterator & left;
        TableScan & right;
        std::string lhsAttr;
        std::string rhsAttr;
        const unsigned byteLimit;
        unsigned bytesUsed = 0;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;
        std::unordered_map<int, std::vector<unsigned char *>> intKeys;
        std::unordered_map<float, std::vector<unsigned char *>> realKeys;
        std::unordered_map<std::string, std::vector<unsigned char *>> strKeys;
        std::vector<unsigned char *> *tuplePtr = nullptr;
        unsigned tupleIndex = 0;
        unsigned char rightTuple[PAGE_SIZE];
        SizeType rightTupleSize = 0;

        void clearMemory();
        RC scanLeftIter();
        void getLeftMatches();
        void joinTuples(void *data);

    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
        Iterator & left;
        IndexScan & right;
        std::string lhsAttr;
        std::vector<Attribute> leftAttrs;
        std::vector<Attribute> rightAttrs;
        unsigned char leftTuple[PAGE_SIZE];
        SizeType leftTupleSize = 0;
        unsigned char rightTuple[PAGE_SIZE];
        SizeType rightTupleSize = 0;
        bool scanStarted = false;
        unsigned char *leftKey = nullptr;

        void joinTuples(void *data);
        RC setLeftKey();
        void setRightTupleSize();

    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
        RecordBasedFileManager & rbfm;
        RBFM_ScanIterator scanner;
        Iterator & left;
        Iterator & right;
        // keeps track of attribute information for each Iterator
        std::string lhsAttr;
        std::string rhsAttr;
        std::vector<Attribute> leftAttrs;
        std::vector<std::string> leftAttrNames;
        std::vector<Attribute> rightAttrs;
        std::vector<std::string> rightAttrNames;

        // maps used to line up a key to different left tuples for right tuples to be matched with
        std::unordered_map<int, std::vector<unsigned char *>> intKeys;
        std::unordered_map<float, std::vector<unsigned char *>> realKeys;
        std::unordered_map<std::string, std::vector<unsigned char *>> strKeys;
        std::vector<unsigned char *> *tuplePtr;
        unsigned tupleIndex;

        // byte arrays for storing current tuple data
        unsigned char rightTuple[PAGE_SIZE];
        SizeType rightTupleSize;
        bool leftIsOuter;

        // hash value will be an index into vector, value is file name of the partition
        const unsigned numPartitions;
        unsigned partitionNum;
        bool scanningPartition;
        std::vector<std::string> leftPartitions;
        std::vector<std::string> rightPartitions;
        FileHandle *fh;

        void clearMemory();
        void joinTuples(void *data);
        void createPartitions(bool forOuter);
        RC getMatchingPartition(unsigned &partition, const std::vector<Attribute> &attrs, const std::string &keyAttr);
        RC processSmallerPartition();
        void getMatches();

    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
        Iterator & iter;
        std::vector<Attribute> attrs;
        int aggIndex;
        int groupIndex;
        std::vector<const unsigned char *> groupAggs;
        int groupAggsIndex = 0;
        AggregateOp op;

        void minMaxAggregation(bool isMin);
        void countAggregation();
        void sumAvgAggregation(bool isAvg);
        RC nextVal(float *realVal, int *intVal, int *groupInt, float *groupReal, std::string *groupStr, AttrType attrType);

    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };
} // namespace PeterDB

#endif // _qe_h_
