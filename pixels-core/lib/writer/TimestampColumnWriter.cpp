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
#include "TimestampColumnWriter.h"
#include "utils/BitUtils.h"
#include <memory>

TimestampColumnWriter::TimestampColumnWriter(
    std::shared_ptr<TypeDescription> type,
    std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), curPixelVector(pixelStride) {
  runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
  if (runlengthEncoding) {
    encoder = std::make_unique<RunLenIntEncoder>();
  }
}

bool TimestampColumnWriter::decideNullsPadding(
    std::shared_ptr<PixelsWriterOption> writerOption) {
  if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2)) {
    return false;
  }
  return writerOption->isNullsPadding();
}

void TimestampColumnWriter::close() {
  if (runlengthEncoding && encoder != nullptr) {
    encoder->clear();
  }
  ColumnWriter::close();
}

pixels::proto::ColumnEncoding TimestampColumnWriter::getColumnChunkEncoding() {
  pixels::proto::ColumnEncoding columnEncoding;
  if (runlengthEncoding) {
    columnEncoding.set_kind(
        pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
  } else {
    columnEncoding.set_kind(
        pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
  }
  return columnEncoding;
}

int TimestampColumnWriter::write(std::shared_ptr<ColumnVector> vector,
                                 int size) {
  std::cout << "In TimestampColumnWriter::write" << std::endl;
  auto timestampVector =
      std::dynamic_pointer_cast<TimestampColumnVector>(vector);
  if (timestampVector == nullptr) {
    throw std::runtime_error("Invalid ColumnVector type");
  }

  int curPartLength;
  int curPartOffset = 0;
  int nextPartLength = size;
  long *timestamps = timestampVector->times;
  while ((curPixelIsNullIndex + nextPartLength) >= pixelStride) {
    curPartLength = pixelStride - curPixelIsNullIndex;
    writeCurPartTimestamp(timestampVector, timestamps, curPartLength,
                          curPartOffset);
    newPixel();
    curPartOffset += curPartLength;
    nextPartLength -= curPartLength;
  }

  return outputStream->getWritePos();
}

void TimestampColumnWriter::writeCurPartTimestamp(
    std::shared_ptr<ColumnVector> columnVector, long *values, int curPartLength,
    int curPartOffset) {
  for (int i = 0; i < curPartLength; i++) {
    if (columnVector->isNull[curPartOffset + i]) {
      curPixelVector[curPixelVectorIndex] = 0;
    } else {
      curPixelVector[curPixelVectorIndex] = values[curPartOffset + i];
    }
    curPixelVectorIndex++;
  }

  std::copy(columnVector->isNull + curPartOffset,
            columnVector->isNull + curPartOffset + curPartLength,
            isNull.begin() + curPixelIsNullIndex);
  curPixelIsNullIndex += curPartLength;
}

void TimestampColumnWriter::newPixel() {
  // write out current pixel vector
  if (runlengthEncoding) {
    std::vector<byte> buffer(curPixelVectorIndex * sizeof(int));
    int resLen;
    encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex,
                    resLen);
    outputStream->putBytes(buffer.data(), resLen);
  } else {
    std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
    EncodingUtils encodingUtils;
    curVecPartitionBuffer =
        std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(int));
    if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN) {
      for (int i = 0; i < curPixelVectorIndex; i++) {
        encodingUtils.writeIntLE(curVecPartitionBuffer, (int)curPixelVector[i]);
      }
    } else {
      for (int i = 0; i < curPixelVectorIndex; i++) {
        encodingUtils.writeIntBE(curVecPartitionBuffer, (int)curPixelVector[i]);
      }
    }
    outputStream->putBytes(curVecPartitionBuffer->getPointer(),
                           curVecPartitionBuffer->getWritePos());
  }

  ColumnWriter::newPixel();
}