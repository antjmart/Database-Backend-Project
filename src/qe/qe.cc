#include "src/include/qe.h"
#include <cstring>
#include <iostream>

constexpr PeterDB::SizeType BITS_PER_BYTE = 8;
constexpr PeterDB::SizeType INT_BYTES = 4;
constexpr PeterDB::SizeType OFFSET_BYTES = sizeof(PeterDB::SizeType);

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition)
        : iter(*input), cond(condition) {
        input->getAttributes(attrs);
    }

    Filter::~Filter() = default;

    bool Filter::compareValues(void *left, void *right, AttrType type) {
        if (type == TypeInt) {
            int l = *static_cast<int *>(left);
            int r = *static_cast<int *>(right);

            switch (cond.op) {
                case EQ_OP:
                    return l == r;
                case NE_OP:
                    return l != r;
                case LT_OP:
                    return l < r;
                case GT_OP:
                    return l > r;
                case LE_OP:
                    return l <= r;
                case GE_OP:
                    return l >= r;
                default:
                    return false;
            }
        } else if (type == TypeReal) {
            float l = *static_cast<float *>(left);
            float r = *static_cast<float *>(right);

            switch (cond.op) {
                case EQ_OP:
                    return l == r;
                case NE_OP:
                    return l != r;
                case LT_OP:
                    return l < r;
                case GT_OP:
                    return l > r;
                case LE_OP:
                    return l <= r;
                case GE_OP:
                    return l >= r;
                default:
                    return false;
            }
        } else {
            std::string l{static_cast<char *>(left) + INT_BYTES, *static_cast<unsigned *>(left)};
            std::string r{static_cast<char *>(right) + INT_BYTES, *static_cast<unsigned *>(right)};

            switch (cond.op) {
                case EQ_OP:
                    return l == r;
                case NE_OP:
                    return l != r;
                case LT_OP:
                    return l < r;
                case GT_OP:
                    return l > r;
                case LE_OP:
                    return l <= r;
                case GE_OP:
                    return l >= r;
                default:
                    return false;
            }
        }
    }

    bool Filter::validTuple(void *data, SizeType nullBytes) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        char *dataPtr = static_cast<char *>(data) + nullBytes;
        SizeType numAttrs = attrs.size();
        Attribute *attr = nullptr;

        if (cond.bRhsIsAttr) {
            AttrType leftType, rightType;
            void *leftValue = nullptr, *rightValue = nullptr;

            for (SizeType i = 0; i < numAttrs; ++i) {
                if (i % BITS_PER_BYTE == 0)
                    memmove(&nullByte, static_cast<const char *>(data) + i / BITS_PER_BYTE, 1);
                bitNum = i % BITS_PER_BYTE + 1;
                attr = &attrs[i];
                bool nullAttr = rbfm.nullBitOn(nullByte, bitNum);

                if (attr->name == cond.lhsAttr) {
                    if (nullAttr) return false;
                    leftType = attr->type;
                    leftValue = static_cast<void *>(dataPtr);
                }

                if (attr->name == cond.rhsAttr) {
                    if (nullAttr) return false;
                    rightType = attr->type;
                    rightValue = static_cast<void *>(dataPtr);
                }

                if (!nullAttr) {
                    if (attr->type == TypeVarChar)
                        dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                    else
                        dataPtr += attr->length;
                }
            }

            if (leftValue == nullptr || rightValue == nullptr || leftType != rightType)
                return false;
            return compareValues(leftValue, rightValue, leftType);
        } else {
            for (SizeType i = 0; i < numAttrs; ++i) {
                if (i % BITS_PER_BYTE == 0)
                    memmove(&nullByte, static_cast<const char *>(data) + i / BITS_PER_BYTE, 1);
                bitNum = i % BITS_PER_BYTE + 1;
                attr = &attrs[i];

                if (attr->name == cond.lhsAttr) {
                    if (rbfm.nullBitOn(nullByte, bitNum) || attr->type != cond.rhsValue.type)
                        return false;
                    return compareValues(dataPtr, cond.rhsValue.data, attr->type);
                }

                if (!rbfm.nullBitOn(nullByte, bitNum)) {
                    if (attr->type == TypeVarChar)
                        dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                    else
                        dataPtr += attr->length;
                }
            }
            return false;
        }
    }

    RC Filter::getNextTuple(void *data) {
        SizeType nullBytes = RecordBasedFileManager::instance().nullBytesNeeded(attrs.size());
        while (iter.getNextTuple(data) != QE_EOF) {
            if (validTuple(data, nullBytes)) return 0;
        }
        return QE_EOF;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        return 0;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames)
        : iter(*input), projectAttrNames(attrNames.begin(), attrNames.end()) {
        input->getAttributes(attrs);
        std::unordered_map<std::string, unsigned> attrToIndex;
        attrToIndex.reserve(attrNames.size());
        for (unsigned i = 0; i < attrs.size(); ++i) {
            std::string & name = attrs[i].name;
            if (projectAttrNames.find(name) != projectAttrNames.end())
                attrToIndex[name] = i;
        }

        projectAttrs.reserve(attrNames.size());
        for (const std::string & attrName : attrNames) {
            projectAttrs.push_back(attrs[attrToIndex[attrName]]);
        }
    }

    Project::~Project() = default;

    RC Project::getNextTuple(void *data) {
        if (iter.getNextTuple(data) == -1) return QE_EOF;

        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        SizeType numAttrs = attrs.size();
        unsigned char *dataPtr = static_cast<unsigned char *>(data) + rbfm.nullBytesNeeded(numAttrs);
        Attribute *attr = nullptr;
        bool nullAttr;
        std::unordered_map<std::string, unsigned char *> projectLocations;

        for (SizeType i = 0; i < numAttrs; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&nullByte, static_cast<const char *>(data) + i / BITS_PER_BYTE, 1);
            bitNum = i % BITS_PER_BYTE + 1;
            attr = &attrs[i];
            nullAttr = rbfm.nullBitOn(nullByte, bitNum);

            if (projectAttrNames.find(attr->name) != projectAttrNames.end())
                projectLocations[attr->name] = nullAttr ? nullptr : dataPtr;

            if (!nullAttr) {
                if (attr->type == TypeVarChar)
                    dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                else
                    dataPtr += attr->length;
            }
        }

        unsigned char projectedData[PAGE_SIZE];
        unsigned char *projectPtr = projectedData + rbfm.nullBytesNeeded(projectAttrs.size());
        memset(projectedData, 0, projectPtr - projectedData);
        unsigned char *projectNullByte = projectedData;
        int projectBitNum = 1;
        SizeType bytesToCopy;

        for (const Attribute & attribute : projectAttrs) {
            dataPtr = projectLocations[attribute.name];
            if (dataPtr == nullptr) {
                *projectNullByte |= 1 << (BITS_PER_BYTE - projectBitNum);
            } else {
                bytesToCopy = attribute.type == TypeVarChar ? INT_BYTES + *reinterpret_cast<const int *>(dataPtr) : attribute.length;
                memmove(projectPtr, dataPtr, bytesToCopy);
                projectPtr += bytesToCopy;
            }
            projectBitNum = projectBitNum % BITS_PER_BYTE + 1;
            if (projectBitNum == 1) ++projectNullByte;
        }

        memmove(data, projectedData, projectPtr - projectedData);
        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = projectAttrs;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages)
        : left(*leftIn), right(*rightIn), lhsAttr(condition.lhsAttr), rhsAttr(condition.rhsAttr), byteLimit(numPages * PAGE_SIZE) {
        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);
    }

    void BNLJoin::clearMemory() {
        for (auto & tuples : intKeys) {
            for (unsigned char * tuple : tuples.second)
                delete[] tuple;
        }
        for (auto & tuples : realKeys) {
            for (unsigned char * tuple : tuples.second)
                delete[] tuple;
        }
        for (auto & tuples : strKeys) {
            for (unsigned char * tuple : tuples.second)
                delete[] tuple;
        }
        intKeys.clear();
        realKeys.clear();
        strKeys.clear();
    }

    BNLJoin::~BNLJoin() {
        clearMemory();
    }

    RC BNLJoin::scanLeftIter() {
        unsigned char leftTuple[PAGE_SIZE];
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        SizeType numAttrs = leftAttrs.size();
        unsigned char *start = leftTuple + rbfm.nullBytesNeeded(numAttrs);
        unsigned char *ptr, *lhsAttrPos;
        Attribute *attr = nullptr;
        bool nullAttr;
        AttrType lhsType;

        while (bytesUsed < byteLimit && left.getNextTuple(leftTuple) != QE_EOF) {
            lhsAttrPos = nullptr;
            ptr = start;

            for (SizeType i = 0; i < numAttrs; ++i) {
                if (i % BITS_PER_BYTE == 0)
                    memmove(&nullByte, leftTuple + i / BITS_PER_BYTE, 1);
                bitNum = i % BITS_PER_BYTE + 1;
                attr = &leftAttrs[i];
                nullAttr = rbfm.nullBitOn(nullByte, bitNum);

                if (attr->name == lhsAttr) {
                    if (nullAttr) break;
                    lhsAttrPos = ptr;
                    lhsType = attr->type;
                }

                if (!nullAttr) {
                    if (attr->type == TypeVarChar)
                        ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                    else
                        ptr += attr->length;
                }
            }

            if (lhsAttrPos) {
                SizeType neededBytes = OFFSET_BYTES + (ptr - leftTuple);
                unsigned char *tuple = new unsigned char[neededBytes];
                *reinterpret_cast<SizeType *>(tuple) = ptr - leftTuple;
                memmove(tuple + OFFSET_BYTES, leftTuple, ptr - leftTuple);
                bytesUsed += neededBytes;

                switch (lhsType) {
                    case TypeInt:
                        intKeys[*reinterpret_cast<int *>(lhsAttrPos)].push_back(tuple);
                        break;
                    case TypeReal:
                        realKeys[*reinterpret_cast<float *>(lhsAttrPos)].push_back(tuple);
                        break;
                    case TypeVarChar:
                        strKeys[{reinterpret_cast<char *>(lhsAttrPos + INT_BYTES), *reinterpret_cast<unsigned *>(lhsAttrPos)}].push_back(tuple);
                }
            }
        }
        return bytesUsed > 0 ? 0 : QE_EOF;
    }

    void BNLJoin::getLeftMatches() {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        SizeType numAttrs = rightAttrs.size();
        unsigned char *ptr = rightTuple + rbfm.nullBytesNeeded(numAttrs);
        unsigned char *rhsAttrPos = nullptr;
        Attribute *attr = nullptr;
        bool nullAttr;
        AttrType rhsType;

        for (SizeType i = 0; i < numAttrs; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&nullByte, rightTuple + i / BITS_PER_BYTE, 1);
            bitNum = i % BITS_PER_BYTE + 1;
            attr = &rightAttrs[i];
            nullAttr = rbfm.nullBitOn(nullByte, bitNum);

            if (attr->name == rhsAttr) {
                if (nullAttr) return;
                rhsAttrPos = ptr;
                rhsType = attr->type;
            }

            if (!nullAttr) {
                if (attr->type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                else
                    ptr += attr->length;
            }
        }

        rightTupleSize = ptr - rightTuple;
        switch (rhsType) {
            case TypeInt: {
                int key = *reinterpret_cast<int *>(rhsAttrPos);
                if (intKeys.find(key) != intKeys.end())
                    tuplePtr = &intKeys[key];
                break;
            } case TypeReal: {
                float key = *reinterpret_cast<float *>(rhsAttrPos);
                if (realKeys.find(key) != realKeys.end())
                    tuplePtr = &realKeys[key];
                break;
            } case TypeVarChar: {
                const std::string & key = std::string{reinterpret_cast<char *>(rhsAttrPos + INT_BYTES), *reinterpret_cast<unsigned *>(rhsAttrPos)};
                if (strKeys.find(key) != strKeys.end())
                    tuplePtr = &strKeys[key];
            }
        }
    }

    void BNLJoin::joinTuples(void *data) {
        unsigned char *leftTuple = (*tuplePtr)[tupleIndex] + OFFSET_BYTES;
        SizeType leftTupleSize = *reinterpret_cast<SizeType *>(leftTuple - OFFSET_BYTES);
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        SizeType leftAttrCount = leftAttrs.size(), rightAttrCount = rightAttrs.size(), totalAttrs = leftAttrs.size() + rightAttrs.size();
        SizeType leftNullBytes = rbfm.nullBytesNeeded(leftAttrCount);
        SizeType rightNullBytes = rbfm.nullBytesNeeded(rightAttrCount);
        SizeType totalNullBytes = rbfm.nullBytesNeeded(totalAttrs);
        memset(data, 0, totalNullBytes);

        unsigned char *dataPtr = static_cast<unsigned char *>(data) + totalNullBytes;
        memmove(dataPtr, leftTuple + leftNullBytes, leftTupleSize - leftNullBytes);
        dataPtr += leftTupleSize - leftNullBytes;
        memmove(dataPtr, rightTuple + rightNullBytes, rightTupleSize - rightNullBytes);

        unsigned char *dataNullByte = static_cast<unsigned char *>(data);
        unsigned char leftNullByte, rightNullByte;
        int leftBit, rightBit, dataBit = 1;

        for (SizeType i = 0; i < leftAttrCount; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&leftNullByte, leftTuple + i / BITS_PER_BYTE, 1);
            leftBit = i % BITS_PER_BYTE + 1;
            if (rbfm.nullBitOn(leftNullByte, leftBit))
                *dataNullByte |= 1 << (BITS_PER_BYTE - dataBit);
            if (dataBit == BITS_PER_BYTE) ++dataNullByte;
            dataBit = dataBit % BITS_PER_BYTE + 1;
        }
        for (SizeType i = 0; i < rightAttrCount; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&rightNullByte, rightTuple + i / BITS_PER_BYTE, 1);
            rightBit = i % BITS_PER_BYTE + 1;
            if (rbfm.nullBitOn(rightNullByte, rightBit))
                *dataNullByte |= 1 << (BITS_PER_BYTE - dataBit);
            if (dataBit == BITS_PER_BYTE) ++dataNullByte;
            dataBit = dataBit % BITS_PER_BYTE + 1;
        }

        if (++tupleIndex >= tuplePtr->size()) {
            tuplePtr = nullptr;
            tupleIndex = 0;
        }
    }

    RC BNLJoin::getNextTuple(void *data) {
        if (tuplePtr != nullptr) {
            joinTuples(data);
            return 0;
        }

        while (true) {
            if (bytesUsed == 0) {
                clearMemory();
                if (scanLeftIter() == QE_EOF) return QE_EOF;
                right.setIterator();
            }
            while (right.getNextTuple(rightTuple) != QE_EOF) {
                getLeftMatches();
                if (tuplePtr != nullptr) {
                    joinTuples(data);
                    return 0;
                }
            }
            bytesUsed = 0;
        }
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = leftAttrs;
        for (const Attribute & attr : rightAttrs)
            attrs.push_back(attr);
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition)
        : left(*leftIn), right(*rightIn), lhsAttr(condition.lhsAttr) {
        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);
    }

    INLJoin::~INLJoin() = default;

    void INLJoin::joinTuples(void *data) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        SizeType leftAttrCount = leftAttrs.size(), rightAttrCount = rightAttrs.size(), totalAttrs = leftAttrs.size() + rightAttrs.size();
        SizeType leftNullBytes = rbfm.nullBytesNeeded(leftAttrCount);
        SizeType rightNullBytes = rbfm.nullBytesNeeded(rightAttrCount);
        SizeType totalNullBytes = rbfm.nullBytesNeeded(totalAttrs);
        memset(data, 0, totalNullBytes);

        unsigned char *dataPtr = static_cast<unsigned char *>(data) + totalNullBytes;
        memmove(dataPtr, leftTuple + leftNullBytes, leftTupleSize - leftNullBytes);
        dataPtr += leftTupleSize - leftNullBytes;
        memmove(dataPtr, rightTuple + rightNullBytes, rightTupleSize - rightNullBytes);

        unsigned char *dataNullByte = static_cast<unsigned char *>(data);
        unsigned char leftNullByte, rightNullByte;
        int leftBit, rightBit, dataBit = 1;

        for (SizeType i = 0; i < leftAttrCount; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&leftNullByte, leftTuple + i / BITS_PER_BYTE, 1);
            leftBit = i % BITS_PER_BYTE + 1;
            if (rbfm.nullBitOn(leftNullByte, leftBit))
                *dataNullByte |= 1 << (BITS_PER_BYTE - dataBit);
            if (dataBit == BITS_PER_BYTE) ++dataNullByte;
            dataBit = dataBit % BITS_PER_BYTE + 1;
        }
        for (SizeType i = 0; i < rightAttrCount; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&rightNullByte, rightTuple + i / BITS_PER_BYTE, 1);
            rightBit = i % BITS_PER_BYTE + 1;
            if (rbfm.nullBitOn(rightNullByte, rightBit))
                *dataNullByte |= 1 << (BITS_PER_BYTE - dataBit);
            if (dataBit == BITS_PER_BYTE) ++dataNullByte;
            dataBit = dataBit % BITS_PER_BYTE + 1;
        }
    }

    RC INLJoin::setLeftKey() {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        SizeType numAttrs = leftAttrs.size();
        unsigned char *ptr = leftTuple + rbfm.nullBytesNeeded(numAttrs);
        Attribute *attr = nullptr;
        bool nullAttr;

        for (SizeType i = 0; i < numAttrs; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&nullByte, leftTuple + i / BITS_PER_BYTE, 1);
            bitNum = i % BITS_PER_BYTE + 1;
            attr = &leftAttrs[i];
            nullAttr = rbfm.nullBitOn(nullByte, bitNum);

            if (attr->name == lhsAttr) {
                if (nullAttr) return -1;
                leftKey = ptr;
            }

            if (!nullAttr) {
                if (attr->type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                else
                    ptr += attr->length;
            }
        }
        leftTupleSize = ptr - leftTuple;
        return 0;
    }

    void INLJoin::setRightTupleSize() {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        SizeType numAttrs = rightAttrs.size();
        unsigned char *ptr = rightTuple + rbfm.nullBytesNeeded(numAttrs);
        Attribute *attr = nullptr;

        for (SizeType i = 0; i < numAttrs; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&nullByte, rightTuple + i / BITS_PER_BYTE, 1);
            bitNum = i % BITS_PER_BYTE + 1;
            attr = &rightAttrs[i];

            if (!rbfm.nullBitOn(nullByte, bitNum)) {
                if (attr->type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                else
                    ptr += attr->length;
            }
        }
        rightTupleSize = ptr - rightTuple;
    }

    RC INLJoin::getNextTuple(void *data) {
        while (true) {
            if (scanStarted) {
                if (right.getNextTuple(rightTuple) != QE_EOF) {
                    setRightTupleSize();
                    joinTuples(data);
                    return 0;
                }
            } else scanStarted = true;

            while (true) {
                if (left.getNextTuple(leftTuple) == QE_EOF) return QE_EOF;
                if (setLeftKey() == 0) break;
            }
            right.setIterator(leftKey, leftKey, true, true);
        }
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = leftAttrs;
        for (const Attribute & attr : rightAttrs)
            attrs.push_back(attr);
        return 0;
    }

    RC GHJoin::getMatchingPartition(unsigned &partition, const unsigned &numPartitions, const std::vector<Attribute> &attrs, const std::string &keyAttr) {
        unsigned char nullByte;
        int bitNum;
        SizeType numAttrs = attrs.size();
        unsigned char *ptr = leftTuple + rbfm.nullBytesNeeded(numAttrs);
        const Attribute *attr = nullptr;
        bool nullAttr;

        for (SizeType i = 0; i < numAttrs; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&nullByte, leftTuple + i / BITS_PER_BYTE, 1);
            bitNum = i % BITS_PER_BYTE + 1;
            attr = &attrs[i];
            nullAttr = rbfm.nullBitOn(nullByte, bitNum);

            if (attr->name == keyAttr) {
                if (nullAttr) break;
                switch (attr->type) {
                    case TypeInt:
                        partition = std::hash<int>{}(*reinterpret_cast<int *>(ptr)) % numPartitions;
                        break;
                    case TypeReal:
                        partition = std::hash<float>{}(*reinterpret_cast<float *>(ptr)) % numPartitions;
                        break;
                    case TypeVarChar:
                        const std::string & key = std::string{reinterpret_cast<char *>(ptr + INT_BYTES), *reinterpret_cast<unsigned *>(ptr)};
                        partition = std::hash<std::string>{}(key) % numPartitions;
                }
                return 0;
            }

            if (!nullAttr) {
                if (attr->type == TypeVarChar)
                    ptr += INT_BYTES + *reinterpret_cast<const int *>(ptr);
                else
                    ptr += attr->length;
            }
        }
        return -1;
    }

    void GHJoin::createPartitions(const unsigned &numPartitions, bool forOuter) {
        std::vector<std::string> & partitions = forOuter ? leftPartitions : rightPartitions;
        std::vector<FileHandle> fhandles{numPartitions, FileHandle{}};
        std::string baseFileName = forOuter ? "left" : "right";
        std::string fileName;

        for (unsigned i = 0; i < numPartitions; ++i) {
            unsigned underscores = 1;
            fileName = baseFileName + "_" + std::to_string(i);
            while (rbfm.createFile(fileName) == -1) {
                ++underscores;
                fileName = baseFileName;
                for (unsigned u = 0; u < underscores; ++u) fileName += "_";
                fileName += std::to_string(i);
            }
            partitions[i] = fileName;
            rbfm.openFile(fileName, fhandles[i]);
        }

        Iterator & iter = forOuter ? left : right;
        std::vector<Attribute> & attrs = forOuter ? leftAttrs : rightAttrs;
        std::string & keyAttr = forOuter ? lhsAttr : rhsAttr;
        unsigned partition;
        RID rid{};
        while (iter.getNextTuple(leftTuple) != QE_EOF) {
            if (getMatchingPartition(partition, numPartitions, attrs, keyAttr) == 0)
                rbfm.insertRecord(fhandles[partition], attrs, leftTuple, rid);
        }

        for (FileHandle & fh : fhandles)
            rbfm.closeFile(fh);
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions)
        : rbfm(RecordBasedFileManager::instance()), left(*leftIn), right(*rightIn), lhsAttr(condition.lhsAttr),
          rhsAttr(condition.rhsAttr), tuplePtr(nullptr), tupleIndex(0), leftPartitions(numPartitions, ""),
          rightPartitions(numPartitions, ""), fh(nullptr) {
        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);
        createPartitions(numPartitions, true);
        createPartitions(numPartitions, false);
    }

    GHJoin::~GHJoin() {
        clearMemory();
        for (const std::string &partition : leftPartitions)
            rbfm.destroyFile(partition);
        for (const std::string &partition : rightPartitions)
            rbfm.destroyFile(partition);
    }

    void GHJoin::clearMemory() {
        for (auto & tuples : intKeys) {
            for (unsigned char * tuple : tuples.second)
                delete[] tuple;
        }
        for (auto & tuples : realKeys) {
            for (unsigned char * tuple : tuples.second)
                delete[] tuple;
        }
        for (auto & tuples : strKeys) {
            for (unsigned char * tuple : tuples.second)
                delete[] tuple;
        }
        intKeys.clear();
        realKeys.clear();
        strKeys.clear();
    }

    void GHJoin::joinTuples(void *data) {
        SizeType firstAttrCount, secondAttrCount, firstSize, secondSize;
        unsigned char *first, *second;
        if (leftIsOuter) {
            firstAttrCount = leftAttrs.size();
            secondAttrCount = rightAttrs.size();
            first = leftTuple;
            second = rightTuple;
            firstSize = leftTupleSize;
            secondSize = rightTupleSize;
        } else {
            firstAttrCount = rightAttrs.size();
            secondAttrCount = leftAttrs.size();
            first = rightTuple;
            second = leftTuple;
            firstSize = rightTupleSize;
            secondSize = leftTupleSize;
        }
        SizeType totalAttrs = leftAttrs.size() + rightAttrs.size();
        SizeType firstNullBytes = rbfm.nullBytesNeeded(firstAttrCount);
        SizeType secondNullBytes = rbfm.nullBytesNeeded(secondAttrCount);
        SizeType totalNullBytes = rbfm.nullBytesNeeded(totalAttrs);
        memset(data, 0, totalNullBytes);

        unsigned char *dataPtr = static_cast<unsigned char *>(data) + totalNullBytes;
        memmove(dataPtr, first + firstNullBytes, firstSize - firstNullBytes);
        dataPtr += firstSize - firstNullBytes;
        memmove(dataPtr, second + secondNullBytes, secondSize - secondNullBytes);

        unsigned char *dataNullByte = static_cast<unsigned char *>(data);
        unsigned char firstNullByte, secondNullByte;
        int firstBit, secondBit, dataBit = 1;

        for (SizeType i = 0; i < firstAttrCount; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&firstNullByte, first + i / BITS_PER_BYTE, 1);
            firstBit = i % BITS_PER_BYTE + 1;
            if (rbfm.nullBitOn(firstNullByte, firstBit))
                *dataNullByte |= 1 << (BITS_PER_BYTE - dataBit);
            if (dataBit == BITS_PER_BYTE) ++dataNullByte;
            dataBit = dataBit % BITS_PER_BYTE + 1;
        }
        for (SizeType i = 0; i < secondAttrCount; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&secondNullByte, second + i / BITS_PER_BYTE, 1);
            secondBit = i % BITS_PER_BYTE + 1;
            if (rbfm.nullBitOn(secondNullByte, secondBit))
                *dataNullByte |= 1 << (BITS_PER_BYTE - dataBit);
            if (dataBit == BITS_PER_BYTE) ++dataNullByte;
            dataBit = dataBit % BITS_PER_BYTE + 1;
        }
    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = leftAttrs;
        for (const Attribute & attr : rightAttrs)
            attrs.push_back(attr);
        return 0;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op)
        : iter(*input), groupIndex(-1), op(op) {
        input->getAttributes(attrs);
        aggIndex = 0;
        for (Attribute & attr : attrs) {
            if (attr.name == aggAttr.name) break;
            ++aggIndex;
        }
        switch (op) {
            case MIN:
                minMaxAggregation(true);
                break;
            case MAX:
                minMaxAggregation(false);
                break;
            case COUNT:
                countAggregation();
                break;
            case SUM:
                sumAvgAggregation(false);
                break;
            case AVG:
                sumAvgAggregation(true);
        }
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op)
        : iter(*input), op(op) {
        input->getAttributes(attrs);
        for (int i = 0; i < attrs.size(); ++i) {
            if (attrs[i].name == aggAttr.name) aggIndex = i;
            if (attrs[i].name == groupAttr.name) groupIndex = i;
        }
        switch (op) {
            case MIN:
                minMaxAggregation(true);
                break;
            case MAX:
                minMaxAggregation(false);
                break;
            case COUNT:
                countAggregation();
                break;
            case SUM:
                sumAvgAggregation(false);
                break;
            case AVG:
                sumAvgAggregation(true);
        }
    }

    Aggregate::~Aggregate() {
        for (const unsigned char *ptr : groupAggs)
            delete[] ptr;
    }

    RC Aggregate::nextVal(float *realVal, int *intVal, int *groupInt, float *groupReal, std::string *groupStr, AttrType attrType) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        unsigned char data[PAGE_SIZE];
        unsigned numAttrs = attrs.size();
        unsigned char *dataStart = static_cast<unsigned char *>(data) + rbfm.nullBytesNeeded(numAttrs);
        unsigned char *dataPtr;
        bool nullAttr, includeTuple;

        while (iter.getNextTuple(data) != QE_EOF) {
            dataPtr = dataStart;
            includeTuple = true;

            for (unsigned i = 0; i < numAttrs; ++i) {
                if (i % BITS_PER_BYTE == 0)
                    memmove(&nullByte, data + i / BITS_PER_BYTE, 1);
                bitNum = i % BITS_PER_BYTE + 1;
                nullAttr = rbfm.nullBitOn(nullByte, bitNum);

                if (i == aggIndex) {
                    if (nullAttr) {includeTuple = false; break;}
                    if (intVal != nullptr)
                        memmove(intVal, dataPtr, INT_BYTES);
                    else if (realVal != nullptr)
                        memmove(realVal, dataPtr, INT_BYTES);
                }
                if (i == groupIndex) {
                    if (nullAttr) {includeTuple = false; break;}
                    switch (attrType) {
                        case TypeInt:
                            memmove(groupInt, dataPtr, INT_BYTES);
                            break;
                        case TypeReal:
                            memmove(groupReal, dataPtr, INT_BYTES);
                            break;
                        case TypeVarChar:
                            *groupStr = std::string{reinterpret_cast<char *>(dataPtr) + INT_BYTES, *reinterpret_cast<unsigned *>(dataPtr)};
                    }
                }

                if (!nullAttr) {
                    if (attrs[i].type == TypeVarChar)
                        dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                    else
                        dataPtr += INT_BYTES;
                }
            }
            if (includeTuple) return 0;
        }
        return QE_EOF;
    }

    void Aggregate::minMaxAggregation(bool isMin) {
        if (groupIndex == -1) {
            unsigned char *start = new unsigned char[INT_BYTES + 1 + OFFSET_BYTES];
            *reinterpret_cast<SizeType *>(start) = INT_BYTES + 1;
            unsigned char *data = start + OFFSET_BYTES;
            memset(data, 0, 1);
            if (attrs[aggIndex].type == TypeInt) {
                int limit = isMin ? std::numeric_limits<int>::max() : std::numeric_limits<int>::min();
                int val;
                while (nextVal(nullptr, &val, nullptr, nullptr, nullptr, TypeInt) != QE_EOF)
                    limit = isMin ? std::min(limit, val) : std::max(limit, val);
                memmove(data + 1, &limit, INT_BYTES);
            } else {
                float limit = isMin ? std::numeric_limits<float>::max() : std::numeric_limits<float>::min();
                float val;
                while (nextVal(&val, nullptr, nullptr, nullptr, nullptr, TypeInt) != QE_EOF)
                    limit = isMin ? std::min(limit, val) : std::max(limit, val);
                memmove(data + 1, &limit, INT_BYTES);
            }
            groupAggs.push_back(start);
            return;
        }

        switch (attrs[groupIndex].type) {
            case TypeInt:
                if (attrs[aggIndex].type == TypeInt) {
                    int val, group;
                    std::unordered_map<int, int> groupings;

                    while (nextVal(nullptr, &val, &group, nullptr, nullptr, TypeInt) != QE_EOF) {
                        if (groupings.find(group) != groupings.end())
                            groupings[group] = isMin ? std::min(groupings[group], val) : std::max(groupings[group], val);
                        else
                            groupings[group] = val;
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                } else {
                    float val;
                    int group;
                    std::unordered_map<int, float> groupings;

                    while (nextVal(&val, nullptr, &group, nullptr, nullptr, TypeInt) != QE_EOF) {
                        if (groupings.find(group) != groupings.end())
                            groupings[group] = isMin ? std::min(groupings[group], val) : std::max(groupings[group], val);
                        else
                            groupings[group] = val;
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                }
                break;
            case TypeReal:
                if (attrs[aggIndex].type == TypeInt) {
                    int val;
                    float group;
                    std::unordered_map<float, int> groupings;

                    while (nextVal(nullptr, &val, nullptr, &group, nullptr, TypeReal) != QE_EOF) {
                        if (groupings.find(group) != groupings.end())
                            groupings[group] = isMin ? std::min(groupings[group], val) : std::max(groupings[group], val);
                        else
                            groupings[group] = val;
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                } else {
                    float val, group;
                    std::unordered_map<float, float> groupings;

                    while (nextVal(&val, nullptr, nullptr, &group, nullptr, TypeReal) != QE_EOF) {
                        if (groupings.find(group) != groupings.end())
                            groupings[group] = isMin ? std::min(groupings[group], val) : std::max(groupings[group], val);
                        else
                            groupings[group] = val;
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                }
                break;
            case TypeVarChar:
                if (attrs[aggIndex].type == TypeInt) {
                    int val;
                    std::string group;
                    std::unordered_map<std::string, int> groupings;

                    while (nextVal(nullptr, &val, nullptr, nullptr, &group, TypeVarChar) != QE_EOF) {
                        if (groupings.find(group) != groupings.end())
                            groupings[group] = isMin ? std::min(groupings[group], val) : std::max(groupings[group], val);
                        else
                            groupings[group] = val;
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[1 + INT_BYTES + g.first.size() + INT_BYTES + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES + g.first.size() + INT_BYTES;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        *reinterpret_cast<unsigned *>(data + 1) = g.first.size();
                        memmove(data + (1 + INT_BYTES), g.first.c_str(), g.first.size());
                        memmove(data + (1 + INT_BYTES + g.first.size()), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                } else {
                    float val;
                    std::string group;
                    std::unordered_map<std::string, float> groupings;

                    while (nextVal(&val, nullptr, nullptr, nullptr, &group, TypeVarChar) != QE_EOF) {
                        if (groupings.find(group) != groupings.end())
                            groupings[group] = isMin ? std::min(groupings[group], val) : std::max(groupings[group], val);
                        else
                            groupings[group] = val;
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[1 + INT_BYTES + g.first.size() + INT_BYTES + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES + g.first.size() + INT_BYTES;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        *reinterpret_cast<unsigned *>(data + 1) = g.first.size();
                        memmove(data + (1 + INT_BYTES), g.first.c_str(), g.first.size());
                        memmove(data + (1 + INT_BYTES + g.first.size()), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                }
        }
    }

    void Aggregate::countAggregation() {
        if (groupIndex == -1) {
            unsigned char *start = new unsigned char[INT_BYTES + 1 + OFFSET_BYTES];
            *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES;
            unsigned char *data = start + OFFSET_BYTES;
            int count = 0;
            memset(data, 0, 1);
            while (nextVal(nullptr, nullptr, nullptr, nullptr, nullptr, TypeInt) != QE_EOF)
                ++count;
            memmove(data + 1, &count, INT_BYTES);
            groupAggs.push_back(start);
            return;
        }

        switch (attrs[groupIndex].type) {
            case TypeInt: {
                int group;
                std::unordered_map<int, int> groupings;

                while (nextVal(nullptr, nullptr, &group, nullptr, nullptr, TypeInt) != QE_EOF) ++groupings[group];
                for (auto g : groupings) {
                    unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                    *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                    unsigned char *data = start + OFFSET_BYTES;
                    memset(data, 0, 1);
                    memmove(data + 1, &g.first, INT_BYTES);
                    memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                    groupAggs.push_back(start);
                }
                break;
            } case TypeReal: {
                float group;
                std::unordered_map<float, int> groupings;

                while (nextVal(nullptr, nullptr, nullptr, &group, nullptr, TypeReal) != QE_EOF) ++groupings[group];
                for (auto g : groupings) {
                    unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                    *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                    unsigned char *data = start + OFFSET_BYTES;
                    memset(data, 0, 1);
                    memmove(data + 1, &g.first, INT_BYTES);
                    memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                    groupAggs.push_back(start);
                }
                break;
            } case TypeVarChar: {
                std::string group;
                std::unordered_map<std::string, int> groupings;

                while (nextVal(nullptr, nullptr, nullptr, nullptr, &group, TypeVarChar) != QE_EOF) ++groupings[group];
                for (auto g : groupings) {
                    unsigned char *start = new unsigned char[1 + INT_BYTES + g.first.size() + INT_BYTES + OFFSET_BYTES];
                    *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES + g.first.size() + INT_BYTES;
                    unsigned char *data = start + OFFSET_BYTES;
                    memset(data, 0, 1);
                    *reinterpret_cast<unsigned *>(data + 1) = g.first.size();
                    memmove(data + (1 + INT_BYTES), g.first.c_str(), g.first.size());
                    memmove(data + (1 + INT_BYTES + g.first.size()), &g.second, INT_BYTES);
                    groupAggs.push_back(start);
                }
            }
        }
    }

    void Aggregate::sumAvgAggregation(bool isAvg) {
        if (groupIndex == -1) {
            unsigned char *start = new unsigned char[INT_BYTES + 1 + OFFSET_BYTES];
            *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES;
            unsigned char *data = start + OFFSET_BYTES;
            float count = 0.0;
            memset(data, 0, 1);

            if (attrs[aggIndex].type == TypeInt) {
                int sum = 0;
                int val;
                while (nextVal(nullptr, &val, nullptr, nullptr, nullptr, TypeInt) != QE_EOF) {
                    sum += val;
                    ++count;
                }

                if (isAvg) {
                    float avg = sum / count;
                    memmove(data + 1, &avg, INT_BYTES);
                } else
                    memmove(data + 1, &sum, INT_BYTES);
            } else {
                float sum = 0.0;
                float val;
                while (nextVal(&val, nullptr, nullptr, nullptr, nullptr, TypeInt) != QE_EOF) {
                    sum += val;
                    ++count;
                }
                *reinterpret_cast<float *>(data + 1) = isAvg ? sum / count : sum;
            }
            groupAggs.push_back(start);
            return;
        }

        switch (attrs[groupIndex].type) {
            case TypeInt: {
                std::unordered_map<int, float> groupCounts;
                if (attrs[aggIndex].type == TypeInt) {
                    int val, group;
                    std::unordered_map<int, int> groupings;

                    while (nextVal(nullptr, &val, &group, nullptr, nullptr, TypeInt) != QE_EOF) {
                        groupings[group] += val;
                        ++groupCounts[group];
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        if (isAvg) {
                            float avg = g.second / groupCounts[g.first];
                            memmove(data + (1 + INT_BYTES), &avg, INT_BYTES);
                        } else
                            memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                } else {
                    float val;
                    int group;
                    std::unordered_map<int, float> groupings;

                    while (nextVal(&val, nullptr, &group, nullptr, nullptr, TypeInt) != QE_EOF) {
                        groupings[group] += val;
                        ++groupCounts[group];
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        *reinterpret_cast<float *>(data + (1 + INT_BYTES)) = isAvg ? g.second / groupCounts[g.first] : g.second;
                        groupAggs.push_back(start);
                    }
                }
                break;
            } case TypeReal: {
                std::unordered_map<float, float> groupCounts;
                if (attrs[aggIndex].type == TypeInt) {
                    int val;
                    float group;
                    std::unordered_map<float, int> groupings;

                    while (nextVal(nullptr, &val, nullptr, &group, nullptr, TypeReal) != QE_EOF) {
                        groupings[group] += val;
                        ++groupCounts[group];
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        if (isAvg) {
                            float avg = g.second / groupCounts[g.first];
                            memmove(data + (1 + INT_BYTES), &avg, INT_BYTES);
                        } else
                            memmove(data + (1 + INT_BYTES), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                } else {
                    float val, group;
                    std::unordered_map<float, float> groupings;

                    while (nextVal(&val, nullptr, nullptr, &group, nullptr, TypeReal) != QE_EOF) {
                        groupings[group] += val;
                        ++groupCounts[group];
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[INT_BYTES + INT_BYTES + 1 + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = INT_BYTES + INT_BYTES + 1;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        memmove(data + 1, &g.first, INT_BYTES);
                        *reinterpret_cast<float *>(data + (1 + INT_BYTES)) = isAvg ? g.second / groupCounts[g.first] : g.second;
                        groupAggs.push_back(start);
                    }
                }
                break;
            } case TypeVarChar: {
                std::unordered_map<std::string, float> groupCounts;
                if (attrs[aggIndex].type == TypeInt) {
                    int val;
                    std::string group;
                    std::unordered_map<std::string, int> groupings;

                    while (nextVal(nullptr, &val, nullptr, nullptr, &group, TypeVarChar) != QE_EOF) {
                        groupings[group] += val;
                        ++groupCounts[group];
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[1 + INT_BYTES + g.first.size() + INT_BYTES + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES + g.first.size() + INT_BYTES;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        *reinterpret_cast<unsigned *>(data + 1) = g.first.size();
                        memmove(data + (1 + INT_BYTES), g.first.c_str(), g.first.size());
                        if (isAvg) {
                            float avg = g.second / groupCounts[g.first];
                            memmove(data + (1 + INT_BYTES + g.first.size()), &avg, INT_BYTES);
                        } else
                            memmove(data + (1 + INT_BYTES + g.first.size()), &g.second, INT_BYTES);
                        groupAggs.push_back(start);
                    }
                } else {
                    float val;
                    std::string group;
                    std::unordered_map<std::string, float> groupings;

                    while (nextVal(&val, nullptr, nullptr, nullptr, &group, TypeVarChar) != QE_EOF) {
                        groupings[group] += val;
                        ++groupCounts[group];
                    }
                    for (auto g : groupings) {
                        unsigned char *start = new unsigned char[1 + INT_BYTES + g.first.size() + INT_BYTES + OFFSET_BYTES];
                        *reinterpret_cast<SizeType *>(start) = 1 + INT_BYTES + g.first.size() + INT_BYTES;
                        unsigned char *data = start + OFFSET_BYTES;
                        memset(data, 0, 1);
                        *reinterpret_cast<unsigned *>(data + 1) = g.first.size();
                        memmove(data + (1 + INT_BYTES), g.first.c_str(), g.first.size());
                        *reinterpret_cast<float *>(data + (1 + INT_BYTES + g.first.size())) = isAvg ? g.second / groupCounts[g.first] : g.second;
                        groupAggs.push_back(start);
                    }
                }
                break;
            }
        }
    }

    RC Aggregate::getNextTuple(void *data) {
        if (groupAggsIndex >= groupAggs.size()) return QE_EOF;
        const unsigned char *groupAgg = groupAggs[groupAggsIndex++];
        memmove(data, groupAgg + OFFSET_BYTES, *reinterpret_cast<const SizeType *>(groupAgg));
        return 0;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        if (groupIndex != -1) attrs.push_back(this->attrs[groupIndex]);
        Attribute attr = this->attrs[aggIndex];
        std::string opStr;
        switch (op) {
            case MIN:
                opStr = "MIN";
                break;
            case MAX:
                opStr = "MAX";
                break;
            case COUNT:
                opStr = "COUNT";
                attr.type = TypeInt;
                break;
            case SUM:
                opStr = "SUM";
                break;
            case AVG:
                opStr = "AVG";
                attr.type = TypeReal;
        }
        attr.name = opStr + "(" + attr.name + ")";
        attrs.push_back(attr);
        return 0;
    }
} // namespace PeterDB
