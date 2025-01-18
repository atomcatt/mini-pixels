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
#include "writer/DateColumnWriter.h"

DateColumnWriter::DateColumnWriter(std::shared_ptr <TypeDescription> type,
                                   std::shared_ptr <PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), curPixelVector(pixelStride) {
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding) {
        encoder = std::make_unique<RunLenIntEncoder>(true, true);
    }
}

int DateColumnWriter::write(std::shared_ptr <ColumnVector> vector, int length) {
    std::shared_ptr<DateColumnVector> columnVector = std::static_pointer_cast<DateColumnVector>(vector);
    int *dates = columnVector->dates;
    int curPartLength;
    int curPartOffset = 0;
    int nextPartLength = length;
    while((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTime(columnVector, dates, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = length - curPartOffset;
    }
    curPartLength = nextPartLength;
    writeCurPartTime(columnVector, dates, curPartLength, curPartOffset);
    return outputStream->getWritePos();
}

void DateColumnWriter::writeCurPartTime(std::shared_ptr <ColumnVector> columnVector, int *values, int curPartLength,
                                        int curPartOffset) {
    for (int i = 0; i < curPartLength; ++i) {
        curPixelEleIndex++;
        if (columnVector->isNull[i + curPartOffset]) {
            hasNull = true;
            pixelStatRecorder.increment();
            if (nullsPadding) {
                curPixelVector[curPixelVectorIndex++] = 0;
            }
        } else {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}

void DateColumnWriter::newPixel() {
    if (runlengthEncoding) {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(int));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    } else {
        bool littleEndian = (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN);
        EncodingUtils encodingUtils;
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(int));
        for (int i = 0; i < curPixelVectorIndex; ++i) {
            if (littleEndian) {
                encodingUtils.writeLongLE(curVecPartitionBuffer, curPixelVector[i]);
            } else {
                encodingUtils.writeLongBE(curVecPartitionBuffer, curPixelVector[i]);
            }
        }
        outputStream->putBytes(curVecPartitionBuffer->getPointer(), curVecPartitionBuffer->getWritePos());
    }
    ColumnWriter::newPixel();
}

void DateColumnWriter::close()
{
    if (runlengthEncoding && encoder)
    {
        encoder->clear();
    }
    ColumnWriter::close();
}

pixels::proto::ColumnEncoding DateColumnWriter::getColumnChunkEncoding()
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding)
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}

bool DateColumnWriter::decideNullsPadding(std::shared_ptr <PixelsWriterOption> writerOption) {
    if (writerOption->getEncodingLevel().ge(EncodingLevel::EL2)) {
        return false;
    }
    return writerOption->isNullsPadding();
}