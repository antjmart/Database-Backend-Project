#include "src/include/qe.h"
#include <cstring>
#include <iostream>

constexpr PeterDB::SizeType BITS_PER_BYTE = 8;
constexpr PeterDB::SizeType INT_BYTES = 4;

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
        Attribute attr;

        if (cond.bRhsIsAttr) {
            AttrType leftType, rightType;
            void *leftValue = nullptr, *rightValue = nullptr;

            for (SizeType i = 0; i < numAttrs; ++i) {
                if (i % BITS_PER_BYTE == 0)
                    memmove(&nullByte, static_cast<const char *>(data) + i / BITS_PER_BYTE, 1);
                bitNum = i % BITS_PER_BYTE + 1;
                attr = attrs[i];
                bool nullAttr = rbfm.nullBitOn(nullByte, bitNum);

                if (attr.name == cond.lhsAttr) {
                    if (nullAttr) return false;
                    leftType = attr.type;
                    leftValue = static_cast<void *>(dataPtr);
                }

                if (attr.name == cond.rhsAttr) {
                    if (nullAttr) return false;
                    rightType = attr.type;
                    rightValue = static_cast<void *>(dataPtr);
                }

                if (!nullAttr) {
                    if (attr.type == TypeVarChar)
                        dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                    else
                        dataPtr += attr.length;
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
                attr = attrs[i];

                if (attr.name == cond.lhsAttr) {
                    if (rbfm.nullBitOn(nullByte, bitNum) || attr.type != cond.rhsValue.type)
                        return false;
                    return compareValues(dataPtr, cond.rhsValue.data, attr.type);
                }

                if (!rbfm.nullBitOn(nullByte, bitNum)) {
                    if (attr.type == TypeVarChar)
                        dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                    else
                        dataPtr += attr.length;
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
        Attribute & attr = attrs[0];
        bool nullAttr;
        std::unordered_map<std::string, unsigned char *> projectLocations;

        for (SizeType i = 0; i < numAttrs; ++i) {
            if (i % BITS_PER_BYTE == 0)
                memmove(&nullByte, static_cast<const char *>(data) + i / BITS_PER_BYTE, 1);
            bitNum = i % BITS_PER_BYTE + 1;
            attr = attrs[i];
            nullAttr = rbfm.nullBitOn(nullByte, bitNum);

            if (projectAttrNames.find(attr.name) != projectAttrNames.end())
                projectLocations[attr.name] = nullAttr ? nullptr : dataPtr;

            if (!nullAttr) {
                if (attr.type == TypeVarChar)
                    dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                else
                    dataPtr += attr.length;
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
                bytesToCopy = attr.type == TypeVarChar ? INT_BYTES + *reinterpret_cast<const int *>(dataPtr) : attr.length;
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

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
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
                minAggregation();
                break;
            case MAX:
                maxAggregation();
                break;
            case COUNT:
                countAggregation();
                break;
            case SUM:
                sumAggregation();
                break;
            case AVG:
                avgAggregation();
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
                minAggregation();
                break;
            case MAX:
                maxAggregation();
                break;
            case COUNT:
                countAggregation();
                break;
            case SUM:
                sumAggregation();
                break;
            case AVG:
                avgAggregation();
        }
    }

    Aggregate::~Aggregate() {
        for (const unsigned char *ptr : groupAggs)
            delete[] ptr;
    }

    RC Aggregate::nextVal(float *realVal, int *intVal) {
        RecordBasedFileManager & rbfm = RecordBasedFileManager::instance();
        unsigned char nullByte;
        int bitNum;
        unsigned char data[PAGE_SIZE];
        unsigned char *dataStart = static_cast<unsigned char *>(data) + rbfm.nullBytesNeeded(attrs.size());
        unsigned char *dataPtr = dataStart;

        while (iter.getNextTuple(data) != QE_EOF) {
            for (unsigned i = 0; i < aggIndex; ++i) {
                if (i % BITS_PER_BYTE == 0)
                    memmove(&nullByte, data + i / BITS_PER_BYTE, 1);
                bitNum = i % BITS_PER_BYTE + 1;

                if (!rbfm.nullBitOn(nullByte, bitNum)) {
                    if (attrs[i].type == TypeVarChar)
                        dataPtr += INT_BYTES + *reinterpret_cast<const int *>(dataPtr);
                    else
                        dataPtr += INT_BYTES;
                }
            }

            if (aggIndex % BITS_PER_BYTE == 0)
                memmove(&nullByte, data + aggIndex / BITS_PER_BYTE, 1);
            bitNum = aggIndex % BITS_PER_BYTE + 1;
            if (!rbfm.nullBitOn(nullByte, bitNum)) {
                if (intVal == nullptr)
                    memmove(realVal, dataPtr, INT_BYTES);
                else
                    memmove(intVal, dataPtr, INT_BYTES);
                return 0;
            }
            dataPtr = dataStart;
        }
        return QE_EOF;
    }

    void Aggregate::minAggregation(void *data) {
        if (attrs[aggIndex].type == TypeInt) {
            int min = std::numeric_limits<int>::max();
            int val;
            while (nextVal(nullptr, &val) != QE_EOF)
                min = std::min(min, val);
            memmove(static_cast<char *>(data) + 1, &min, INT_BYTES);
        } else {
            float min = std::numeric_limits<float>::max();
            float val;
            while (nextVal(&val, nullptr) != QE_EOF)
                min = std::min(min, val);
            memmove(static_cast<char *>(data) + 1, &min, INT_BYTES);
        }
    }

    void Aggregate::maxAggregation(void *data) {
        if (attrs[aggIndex].type == TypeInt) {
            int max = std::numeric_limits<int>::min();
            int val;
            while (nextVal(nullptr, &val) != QE_EOF)
                max = std::max(max, val);
            memmove(static_cast<char *>(data) + 1, &max, INT_BYTES);
        } else {
            float max = std::numeric_limits<float>::min();
            float val;
            while (nextVal(&val, nullptr) != QE_EOF)
                max = std::max(max, val);
            memmove(static_cast<char *>(data) + 1, &max, INT_BYTES);
        }
    }

    void Aggregate::countAggregation(void *data) {
        int count = 0, filler;
        while (nextVal(nullptr, &filler) != QE_EOF)
            ++count;
        memmove(static_cast<char *>(data) + 1, &count, INT_BYTES);
    }

    void Aggregate::sumAggregation(void *data) {
        if (attrs[aggIndex].type == TypeInt) {
            int sum = 0, val;
            while (nextVal(nullptr, &val) != QE_EOF)
                sum += val;
            memmove(static_cast<char *>(data) + 1, &sum, INT_BYTES);
        } else {
            float sum = 0, val;
            while (nextVal(&val, nullptr) != QE_EOF)
                sum += val;
            memmove(static_cast<char *>(data) + 1, &sum, INT_BYTES);
        }
    }

    void Aggregate::avgAggregation(void *data) {
        float numVals = 0;
        if (attrs[aggIndex].type == TypeInt) {
            int sum = 0, val;
            while (nextVal(nullptr, &val) != QE_EOF) {
                sum += val;
                ++numVals;
            }
            float avg = sum / numVals;
            memmove(static_cast<char *>(data) + 1, &avg, INT_BYTES);
        } else {
            float sum = 0, val;
            while (nextVal(&val, nullptr) != QE_EOF) {
                sum += val;
                ++numVals;
            }
            float avg = sum / numVals;
            memmove(static_cast<char *>(data) + 1, &avg, INT_BYTES);
        }
    }

    RC Aggregate::getNextTuple(void *data) {
        if (groupAggsIndex >= groupAggs.size()) return QE_EOF;
        const unsigned char *groupAgg = groupAggs[groupAggsIndex++];
        memmove(data, groupAgg + sizeof(SizeType), *reinterpret_cast<const SizeType *>(groupAgg));
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
