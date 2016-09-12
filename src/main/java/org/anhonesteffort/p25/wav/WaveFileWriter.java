/*
 * Copyright (C) 2016 An Honest Effort LLC.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.anhonesteffort.p25.wav;

import org.anhonesteffort.p25.CheckpointingAudioChunk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.ShortBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

public class WaveFileWriter {

  private final WaveHeaderFactory headers;

  public WaveFileWriter(WaveHeaderFactory headers) {
    this.headers = headers;
  }

  private ByteArrayOutputStream streamForHeader(WaveHeader header) {
    return new ByteArrayOutputStream(WaveHeader.HEADER_LENGTH + header.getNumBytes());
  }

  private byte[] toBytes(FloatBuffer buffer) {
    ByteBuffer  bytes  = ByteBuffer.allocate(buffer.remaining() * 2).order(ByteOrder.LITTLE_ENDIAN);
    ShortBuffer shorts = bytes.asShortBuffer();

    IntStream.range(0, buffer.remaining()).forEach(i -> shorts.put((short) (buffer.get(i) * Short.MAX_VALUE)));

    return bytes.array();
  }

  public Optional<ByteArrayOutputStream> write(List<CheckpointingAudioChunk> chunks) throws IOException {
    WaveHeader header = headers.create(chunks);

    if (header.getNumBytes() <= 0) {
      return Optional.empty();
    } else {
      ByteArrayOutputStream stream = streamForHeader(header);

      header.write(stream);
      for (CheckpointingAudioChunk chunk : chunks) {
        stream.write(toBytes(chunk.getBuffer()));
      }

      stream.flush();
      stream.close();

      return Optional.of(stream);
    }
  }

}
