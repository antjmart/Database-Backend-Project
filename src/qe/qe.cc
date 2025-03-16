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

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages)
        : left(*leftIn), right(*rightIn), cond(condition), byteLimit(numPages * PAGE_SIZE) {
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
    }

    BNLJoin::~BNLJoin() {
        clearMemory();
    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = leftAttrs;
        for (const Attribute & attr : rightAttrs)
            attrs.push_back(attr);
        return 0;
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
