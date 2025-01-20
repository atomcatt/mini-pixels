//
// Created by liyu on 3/17/23.
//

#include "vector/BinaryColumnVector.h"
#include <memory>
#include <cstring>

BinaryColumnVector::BinaryColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
    posix_memalign(reinterpret_cast<void **>(&vector), 32,
                   len * sizeof(duckdb::string_t));
    start = new int[len];
    lens = new int[len];
    startLength = len;
    lensLength = len;
    memoryUsage += (long) sizeof(uint8_t) * len + sizeof(int) * len * 2;
}

void BinaryColumnVector::close() {
	if(!closed) {
		ColumnVector::close();
		free(vector);
		vector = nullptr;

	}
}

void BinaryColumnVector::setRef(int elementNum, uint8_t * const &sourceBuf, int start, int length) {
    std::cout << "In BinaryColumnVector::setRef" << std::endl;
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    this->vector[elementNum]
        = duckdb::string_t((char *)(sourceBuf + start), length);
    this->start[elementNum] = start;
    lens[elementNum] = length;
    isNull[elementNum] = sourceBuf == nullptr;
    if (sourceBuf == nullptr)
    {
        this->noNulls = false;
    }
    // TODO: isNull should implemented, but not now.

}

void BinaryColumnVector::print(int rowCount) {
    std::cout << "In BinaryColumnVector::print" << std::endl;
	// throw InvalidArgumentException("not support print binarycolumnvector.");
    for (int i = 0; i < rowCount; i++) {
        std::cout << vector[rowCount].GetString() << std::endl;
    }
}

BinaryColumnVector::~BinaryColumnVector() {
	if(!closed) {
		BinaryColumnVector::close();
	}
}

void * BinaryColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

void BinaryColumnVector::ensureSize(uint64_t size, bool preserveData) {
    ColumnVector::ensureSize(size, preserveData);
    if (size > length) {
        int *oldStart = start;
        start = new int[size];
        int *oldLens = lens;
        lens = new int[size];
        duckdb::string_t *oldVector = vector;
        posix_memalign(reinterpret_cast<void **>(&vector), 32, size * sizeof(duckdb::string_t));
        memoryUsage += sizeof(uint8_t) * size + sizeof(int) * size * 2;
        length = size;
        startLength = size;
        lensLength = size;
        if (preserveData) {
            std::copy(oldStart, oldStart + startLength, start);
            std::copy(oldLens, oldLens + lensLength, lens);
            std::copy(oldVector, oldVector + length, vector);
        }
    }
}

void BinaryColumnVector::add(std::string &value) {
    size_t len = value.size();
    uint8_t* buffer = new uint8_t[len];
    this->len = len;
    std::memcpy(buffer, value.data(), len);
    add(buffer,len);
    delete[] buffer;
}

void BinaryColumnVector::add(uint8_t *v,int len) {
    if(writeIndex>=getLength()) {
        ensureSize(writeIndex*2,true);
    }
    setVal(writeIndex++,v,0,len);
}

void BinaryColumnVector::initBuffer(int estimatedValueSize) {
    nextFree = 0;
    smallBufferNextFree = 0;
    if (buffer != nullptr) {
        if (bufferAllocationCount > 0) {
            std::fill(vector, vector + length, nullptr);
            buffer = smallBuffer;
            this->bufferSize = this->smallBufferSize;
        }
    } else {
        int bufferSize = length * sizeof(duckdb::string_t);
        if (bufferSize < 16384) {
            bufferSize = 16384;
        }
        buffer = new uint8_t[bufferSize];
        memoryUsage += sizeof(uint8_t) * bufferSize;
        smallBuffer = buffer;
        this->bufferSize = bufferSize;
        this->smallBufferSize = bufferSize;
    }
    bufferAllocationCount = 0;
}

void BinaryColumnVector::increaseBufferSpace(int nextElemLength) {
    if (nextElemLength > 1024 * 1024) {
        uint8_t *newBuffer = new uint8_t[nextElemLength];
        memoryUsage += sizeof(uint8_t) * nextElemLength;
        ++bufferAllocationCount;
        if (smallBuffer == buffer) {
            smallBufferNextFree = nextFree;
        }
        buffer = newBuffer;
        this->bufferSize = nextElemLength;
        nextFree = 0;
    } else {
        if (smallBuffer != buffer) {
            buffer = smallBuffer;
            this->bufferSize = smallBufferSize;
            nextFree = smallBufferNextFree;
        }
        if ((nextFree + nextElemLength) > bufferSize) {
            int newBufferSize = smallBufferSize * 2;
            while (newBufferSize < nextElemLength) {
                std::cout << newBufferSize << std::endl;
                if (newBufferSize < 0) {
                    throw std::runtime_error("Overflow of newLength. smallBuffer.length=" 
                                                + std::to_string(smallBufferSize) 
                                                + ", nextElemLength=" 
                                                + std::to_string(nextElemLength));
                }
                newBufferSize *= 2;
            }
            smallBuffer = new uint8_t[newBufferSize];
            memoryUsage += sizeof(uint8_t) * newBufferSize;
            ++bufferAllocationCount;
            this->smallBufferSize = newBufferSize;
            smallBufferNextFree = 0;
            buffer = smallBuffer;
            this->bufferSize = smallBufferSize;
            nextFree = 0;
        }
    }
}

void BinaryColumnVector::setVal(int elementNum, uint8_t *sourceBuf,int start,int length) {
    std::cout << "In BinaryColumnVector::setVal" << std::endl;
    if (elementNum >= writeIndex) {
            writeIndex = elementNum + 1;
    }
    if (buffer == nullptr) {
        initBuffer(0);
    } else if ((nextFree + length) > bufferSize) {
        increaseBufferSpace(length);
    }
    std::memcpy(buffer + nextFree, sourceBuf + start, length);
    vector[elementNum] = duckdb::string_t((char *)(buffer + nextFree), length);
    std::cout << vector[elementNum].GetString() << std::endl;
    this->start[elementNum] = nextFree;
    this->lens[elementNum] = length;
    isNull[elementNum] = false;
    nextFree += length;
}

void BinaryColumnVector::setVal(int elementNum, uint8_t *sourceBuf)
{
    std::cout << "In BinaryColumnVector::setVal" << std::endl;
    setVal(elementNum, sourceBuf, 0, this->len);
}

void BinaryColumnVector::reset() {
    std::cout << "In BinaryColumnVector::reset" << std::endl;
    ColumnVector::reset();
    // std::fill(vector, vector+length, nullptr);
    for (int i = 0; i< length; i++) {
        vector[i].SetPointer(nullptr);
    }
    resetBuffer();
}

void BinaryColumnVector::resetBuffer() {
    std::cout << "In BinaryColumnVector::resetBuffer" << std::endl;
    nextFree = 0;
    smallBufferNextFree = 0;
    if (buffer == nullptr) {
        if (bufferAllocationCount > 0) {
            for (int i = 0; i < length; i++) {
                vector[i].SetPointer(nullptr);
            }
        }
        buffer = new uint8_t[bufferSize];
        memoryUsage += bufferSize * sizeof(uint8_t);
        smallBuffer = buffer;
    }
    bufferAllocationCount = 0;
}

void BinaryColumnVector::setValPreallocated(int elementNum, int length) {
    std::cout << "In BinaryColumnVector::setValPreallocated" << std::endl;
    if (elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    vector[elementNum] = duckdb::string_t((char*)buffer, bufferSize);
    start[elementNum] = nextFree;
    lens[elementNum] = length;
    isNull[elementNum] = false;
    nextFree += length;
}