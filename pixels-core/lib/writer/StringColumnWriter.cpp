/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/StringColumnWriter.h"

StringColumnWriter::StringColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption) : ColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    encodingUtils = std::make_shared<EncodingUtils>();
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    // runlengthEncoding = false;
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>();
    }
    startsArray = std::make_shared<DynamicIntArray>();
}

void StringColumnWriter::flush()
{
    std::cout << "StringColumnWriter::flush" << std::endl;
    ColumnWriter::flush();
    flushStarts();
}

void StringColumnWriter::flushStarts()
{
    int startsFieldOffset = outputStream->getWritePos();
    startsArray->add(startOffset);
    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
    {
        for (int i = 0; i < startsArray->size(); i++)
        {
            encodingUtils->writeIntLE(outputStream, startsArray->get(i));
        }
    }
    else
    {
        for (int i = 0; i < startsArray->size(); i++)
        {
            encodingUtils->writeIntBE(outputStream, startsArray->get(i));
        }
    }
    startsArray->clear();
    std::shared_ptr<ByteBuffer> offsetBuffer = std::make_shared<ByteBuffer>(sizeof(int));
    offsetBuffer->putInt(startsFieldOffset);
    // offsetBuffer->printAH();
    outputStream->putBytes(offsetBuffer->getPointer(), offsetBuffer->getWritePos());
    // outputStream->printAH();
}

int StringColumnWriter::write(std::shared_ptr<ColumnVector> columnVector,int length) {
    std::cout << "StringColumnWriter::write" <<std::endl;
    auto binaryColumnVector = std::static_pointer_cast<BinaryColumnVector>(columnVector);
    duckdb::string_t *vector = binaryColumnVector->vector;
    int *vLens = binaryColumnVector->lens;
    int *vOffsets = binaryColumnVector->start;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = length;
    // for (int i = 0;i < length;i++) {
    //     std::cout << "vector[i].GetString(): " << vector[i].GetString() << std::endl;
    // }
    while((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartWithoutDict(binaryColumnVector, vector, vLens, vOffsets, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = length - curPartOffset;
    }
    curPartLength = nextPartLength;
    writeCurPartWithoutDict(binaryColumnVector, vector, vLens, vOffsets, curPartLength, curPartOffset);
    return outputStream->getWritePos();
}

void StringColumnWriter::writeCurPartWithoutDict(std::shared_ptr<BinaryColumnVector> columnVector,duckdb::string_t *values,
                               int* vLens,int* vOffsets,int curPartLength,int curPartOffset) {
    std::cout << "In StringColumnWriter::writeCurPartWithoutDict" << std::endl;
    for (int i = 0; i < curPartLength; i++) {
        curPixelEleIndex++;
        if (columnVector->isNull[curPartOffset + i]) {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding) {
                startsArray->add(startOffset);
            }
        } else {
            uint8_t *temp = (uint8_t *)(values[curPartOffset + i].GetString().c_str());
            outputStream->putBytes(temp, vLens[curPartOffset + i]);
           startsArray->add(startOffset);
            startOffset += vLens[curPartOffset + i];
            // std::string valueStr(reinterpret_cast<char*>(values[curPartOffset + i].GetString()), vLens[curPartOffset + i]);
            // pixelStatRecorder.updateString(values[curPartOffset + i].GetString(), vLens[curPartOffset + i]);
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void StringColumnWriter::newPixel() {
    std::cout << "In StringColumnWriter::newPixel" << std::endl;
    if (runlengthEncoding) {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(int));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    } else {
        // bool littleEndian = (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN);
        // EncodingUtils encodingUtils;
        // std::shared_ptr<ByteBuffer> curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(int));
        // for (int i = 0; i < curPixelVectorIndex; ++i) {
        //     if (littleEndian) {
        //         encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
        //         // encodingUtils.writeLongLE(outputStream, curPixelVector[i]);
        //     } else {
        //         encodingUtils.writeLongBE(curVecPartitionBuffer, curPixelVector[i]);
        //         // encodingUtils.writeLongBE(outputStream, curPixelVector[i]);
        //     }
        // }
        // outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }
    ColumnWriter::newPixel();
}

void StringColumnWriter::close() {
    startsArray->clear();
    if (runlengthEncoding) {
        encoder->clear();
    }
    ColumnWriter::close();
}

bool StringColumnWriter::decideNullsPadding(std::shared_ptr <PixelsWriterOption> writerOption) {
    if (writerOption->getEncodingLevel().ge(EncodingLevel::EL2)) {
        return false;
    }
    return writerOption->isNullsPadding();
}

pixels::proto::ColumnEncoding StringColumnWriter::getColumnChunkEncoding()
{
    pixels::proto::ColumnEncoding columnEncoding;
    // if (runlengthEncoding)
    // {
    //     columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    // }
    // else
    // {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    // }
    return columnEncoding;
}
