//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include <boost/multiprecision/cpp_dec_float.hpp>
#include <boost/multiprecision/number.hpp>

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

using namespace boost::multiprecision;

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (size > length) {
        long *oldArray = vector;
        uint64_t oldLength = length;
        vector = new long[size];
        memoryUsage += sizeof(long) * size;
        length = size;
        if (preserveData) {
            std::copy(oldArray, oldArray + oldLength, vector);
        }
    }
}

void DecimalColumnVector::add(std::string &value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    cpp_dec_float_50 decimal(value);
    auto get_scale = [](const cpp_dec_float_50 &decimal) {
        std::string str = decimal.str(); // 转为字符串
        auto pos = str.find('.'); // 找到小数点位置
        if (pos == std::string::npos) {
            return 0; // 如果没有小数点，scale 为 0
        }
        return (int)(str.size() - pos - 1); // 小数点右侧的位数
    };
    int decScale = get_scale(decimal);
    if (decScale != getScale()) {
        decimal = round(decimal * cpp_dec_float_50(pow(10, scale)) / pow(10, scale));
    }
    auto get_precision = [](const cpp_dec_float_50 &decimal) {
        std::string str = decimal.str(); // 转换为字符串表示
        // 去掉小数点和前导符号（如果有）
        str.erase(std::remove(str.begin(), str.end(), '.'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '-'), str.end());
        str.erase(std::remove(str.begin(), str.end(), '+'), str.end());
        // 去掉前导零
        str.erase(0, str.find_first_not_of('0'));
        return (int)str.size(); // 剩余字符数即为有效数字位数
    };
    int decPrecision = get_precision(decimal);
    if (decPrecision > getPrecision()) {
        throw std::invalid_argument("value exceeds the allowed precision" + std::to_string(precision));
    }
    size_t index = writeIndex++;
    vector[index] = decimal.convert_to<int64_t>();
    isNull[index] = false;
}

void DecimalColumnVector::add(bool value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    long longValue = value ? 1 : 0;
    if (getScale() != 0) {
        throw std::invalid_argument("Boolean value cannot have a scale.");
    }
    if (getPrecision() < 1) {
        throw std::invalid_argument("Boolean value exceed allowed precision.");
    }
    size_t index = writeIndex++;
    vector[index] = longValue;
    isNull[index] = false;
}

void DecimalColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    long longValue = static_cast<long>(value);
    if (getScale() != 0) {
        longValue *= static_cast<long>(pow(10, getScale()));
    }
    if (getPrecision() < static_cast<int>(std::to_string(longValue).size())) {
        throw std::invalid_argument("Integer value exceed allowed precision.");
    }
    size_t index = writeIndex++;
    vector[index] = longValue;
    isNull[index] = false;
}

void DecimalColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    long longValue = static_cast<long>(value);
    if (getScale() != 0) {
        longValue *= static_cast<long>(pow(10, getScale()));
    }
    if (getPrecision() < static_cast<int>(std::to_string(longValue).size())) {
        throw std::invalid_argument("Integer value exceed allowed precision.");
    }
    size_t index = writeIndex++;
    vector[index] = longValue;
    isNull[index] = false;
}
