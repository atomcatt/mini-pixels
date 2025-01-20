//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include "duckdb/common/types/decimal.hpp"

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

#include <iostream>
#include <vector>
#include <stdexcept>
#include <string>
#include <cmath>
#include <sstream>
#include <iomanip>

class BigDecimal {
public:
    BigDecimal(const std::string& value) {
        size_t pos = value.find('.');
        if (pos != std::string::npos) {
            integerPart = value.substr(0, pos);
            fractionalPart = value.substr(pos + 1);
            scale = fractionalPart.length();
        } else {
            integerPart = value;
            fractionalPart = "";
            scale = 0;
        }
    }

    int getScale() const {
        return scale;
    }

    BigDecimal setScale(int newScale, int roundingMode) const {
        std::string newIntegerPart = integerPart;
        std::string newFractionalPart = fractionalPart;
        if (newScale < scale) {
            if (roundingMode == ROUND_HALF_UP) {
                if (fractionalPart[newScale] >= '5') {
                    newFractionalPart = fractionalPart.substr(0, newScale);
                    if (newFractionalPart.empty()) {
                        newIntegerPart = std::to_string(std::stoll(integerPart) + 1);
                    } else {
                        int carry = 1;
                        for (int i = newScale - 1; i >= 0; --i) {
                            if (newFractionalPart[i] == '9') {
                                newFractionalPart[i] = '0';
                            } else {
                                newFractionalPart[i] += carry;
                                carry = 0;
                                break;
                            }
                        }
                        if (carry) {
                            newIntegerPart = std::to_string(std::stoll(integerPart) + 1);
                        }
                    }
                } else {
                    newFractionalPart = fractionalPart.substr(0, newScale);
                }
            }
        } else {
            newFractionalPart.append(newScale - scale, '0');
        }
        return BigDecimal(newIntegerPart + "." + newFractionalPart);
    }

    int getPrecision() const {
        return integerPart.length() + fractionalPart.length();
    }

    long long unscaledValue() const {
        return std::stoll(integerPart + fractionalPart);
    }
    static const int ROUND_HALF_UP = 1;
private:
    std::string integerPart;
    std::string fractionalPart;
    int scale;
};

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
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int64_t));
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(uint64_t));
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

void DecimalColumnVector::add(std::string& value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    BigDecimal decimal(value);
    if (decimal.getScale() != scale) {
        decimal = decimal.setScale(scale, BigDecimal::ROUND_HALF_UP);
    }
    if (decimal.getPrecision() > precision) {
        throw std::invalid_argument("value exceeds the allowed precision " + std::to_string(precision));
    }
    int index = writeIndex++;
    vector[index] = decimal.unscaledValue();
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
