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

#include "writer/DecimalColumnWriter.h"
#include "utils/BitUtils.h"

DecimalColumnWriter::DecimalColumnWriter(
    std::shared_ptr<TypeDescription> type,
    std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption) {}

int DecimalColumnWriter::write(std::shared_ptr<ColumnVector> vector,
                                int length) {
  std::cout << "In DecimalColumnWriter::write" << std::endl;
  auto decimalColumnVector = std::static_pointer_cast<DecimalColumnVector>(vector);
  EncodingUtils encodingUtils;
  if (decimalColumnVector == nullptr) {
    throw std::runtime_error("Invalid ColumnVector type");
  }
  long *values = decimalColumnVector->vector;
  for (int i = 0; i < length; i++) {
    isNull[curPixelIsNullIndex] = decimalColumnVector->isNull[i];
    std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
    curPixelEleIndex++;
    if (nullsPadding) {
      hasNull = true;
      switch (byteOrder) {
        case ByteOrder::PIXELS_LITTLE_ENDIAN:
          encodingUtils.writeLongLE(curVecPartitionBuffer, 0L);
          break;
        case ByteOrder::PIXELS_BIG_ENDIAN:
          encodingUtils.writeLongBE(curVecPartitionBuffer, 0L);
      }
    } else {
      hasNull = false;
      switch (byteOrder) {
        case ByteOrder::PIXELS_LITTLE_ENDIAN:
          encodingUtils.writeLongLE(outputStream, values[i]);
          break;
        case ByteOrder::PIXELS_BIG_ENDIAN:
          encodingUtils.writeLongBE(outputStream, values[i]);
      }
    }
    if (curPixelEleIndex >= pixelStride) {
      newPixel();
    }
  }
  return outputStream->getWritePos();
}

bool DecimalColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption) {
  return writerOption->isNullsPadding();
}
