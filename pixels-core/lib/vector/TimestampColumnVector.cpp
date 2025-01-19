//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <istream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cmath>

const long MICROS_PER_SEC = 1'000'000;

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    // if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    // } else {
    //     this->times = nullptr;
    // }
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
}

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (size <= length) {
        return ;
    }
    long *oldTimes = times;
    uint64_t oldLength = length;
    times = new long[size];
    memoryUsage += sizeof(long) * size;
    length = size;
    if (preserveData) {
        std::copy(oldTimes, oldTimes + oldLength, times);
    }
}

void TimestampColumnVector::add(std::string &value) {
    if (writeIndex >= length)
    {
        ensureSize(writeIndex * 2, true);
    }
    set(writeIndex++, stringTimestampToMicros(value));
}

void TimestampColumnVector::add(int64_t value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    set(writeIndex++, value);
}

void TimestampColumnVector::add(int value) {
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    set(writeIndex++, value);
}

// long TimestampColumnVector::stringTimestampToMicros(const std::string &timestamp) {
//     const std::string DATE_FORMAT = "%Y-%m-%d";
//     std::tm tm = {};
//     std::istringstream ss(timestamp);
//     ss >> std::get_time(&tm, DATE_FORMAT.c_str());
//     if (ss.fail()) {
//         throw std::runtime_error("Failed to parse timestamp");
//     }

//     tm.tm_hour = 0;
//     tm.tm_min = 0;
//     tm.tm_sec = 0;

//     auto timeT = std::mktime(&tm);
//     if (timeT == -1) {
//         throw std::runtime_error("Invalid time_t value");
//     }

//     return static_cast<long>(timeT) * MICROS_PER_SEC;
// }

long TimestampColumnVector::stringTimestampToMicros(const std::string& timestamp) {
    // 定义时间格式
    const std::string SQL_LOCAL_DATE_TIME = "%Y-%m-%d %H:%M:%S"; 

    std::tm tm = {};
    std::istringstream ss(timestamp);
    ss >> std::get_time(&tm, SQL_LOCAL_DATE_TIME.c_str());
    if (ss.fail()) {
        throw std::runtime_error("Failed to parse timestamp");
    }

    // 将时间点转换为 time_t
    auto timeT = std::mktime(&tm);

    // 提取小数秒部分（假设在 . 之后）
    size_t dotPos = timestamp.find('.');
    int64_t fractionalMicros = 0;
    if (dotPos != std::string::npos) {
        std::string fractionalPart = timestamp.substr(dotPos + 1);
        if (fractionalPart.length() > 6) {
            fractionalPart = fractionalPart.substr(0, 6); // 只保留前 6 位
        }
        fractionalMicros = std::stoll(fractionalPart) * (MICROS_PER_SEC / std::pow(10, fractionalPart.length()));
    }

    // 计算微秒时间戳
    long epochSeconds = static_cast<long>(timeT);
    return epochSeconds * MICROS_PER_SEC + fractionalMicros;
}