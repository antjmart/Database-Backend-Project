#include "src/include/qe.h"
#include <cstring>

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
        return iter.getAttributes(attrs);
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {

    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
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

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
