//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <istream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <ctime>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} 
    // else {
	// 	this->dates = nullptr;
	// }
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
    std::cout << "In DateColumnVector::set" << std::endl;
	if(elementNum >= writeIndex) {
		this->writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
    isNull[elementNum] = false;
	// TODO: isNull
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (size <= length) {
        return ;
    }
    int *oldTime = dates;
    uint64_t oldLength = length;
    posix_memalign(reinterpret_cast<void **>(&dates), 32, size * sizeof(int));
    dates = new int[size];
    memoryUsage += sizeof(int) * size;
    length = size;
    if (preserveData) {
        std::copy(oldTime, oldTime + oldLength, dates);
    }
}

void DateColumnVector::add(std::string &value) {
    std::cout << "In DateColumnVector::add(string)" << std::endl;
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    set(this->writeIndex++, stringDateToDay(value));
}

void DateColumnVector::add(bool value) {
    add(value ? 1 : 0);
}

void DateColumnVector::add(int value) {
    std::cout << "In DateColumnVector::add(int)" << std::endl;
    if (writeIndex >= length) {
        ensureSize(writeIndex * 2, true);
    }
    set(this->writeIndex++, value);
}

int DateColumnVector::stringDateToDay(const std::string& date) {
    std::tm tm = {};
    std::istringstream ss(date);
    ss >> std::get_time(&tm, "%Y-%m-%d");
    if (ss.fail()) {
        throw std::invalid_argument("Invalid date format: " + date);
    }

    std::time_t time = std::mktime(&tm);
    if (time == -1) {
        throw std::runtime_error("Failed to parse date: " + date);
    }

    auto seconds_since_epoch = std::chrono::system_clock::from_time_t(time).time_since_epoch();
    return static_cast<int>((std::chrono::duration_cast<std::chrono::seconds>(seconds_since_epoch).count() + 28800) / 86400);
}