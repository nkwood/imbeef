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

import org.anhonesteffort.kinesis.consumer.Checkpointer;
import org.anhonesteffort.kinesis.proto.ProtoP25Factory;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.FloatBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.anhonesteffort.kinesis.proto.ProtoP25.P25ChannelId;

public class WaveFileWriterTest {

  private CheckpointingAudioChunk newChunk(float[] floats, List<Checkpointer> checks) {
    final P25ChannelId.Reader CHANNEL = new ProtoP25Factory().controlId(1, 2, 3, 4);
    final FloatBuffer         FLOATS;

    if (floats == null) {
      FLOATS = FloatBuffer.allocate(0);
    } else {
      FLOATS = FloatBuffer.wrap(floats);
    }

    return new CheckpointingAudioChunk(CHANNEL, true, true, true, 10l, 20l, 10d, 20d, 30, FLOATS, checks);
  }

  @Test
  public void testEmptyChunksReturnEmpty() throws Exception {
    final WaveHeaderFactory             HEADERS = new WaveHeaderFactory();
    final WaveFileWriter                WRITER  = new WaveFileWriter(HEADERS);
    final List<CheckpointingAudioChunk> CHUNKS  = new LinkedList<>();

    CHUNKS.add(newChunk(null, new LinkedList<>()));
    assert !WRITER.write(CHUNKS).isPresent();

    CHUNKS.add(newChunk(null, new LinkedList<>()));
    assert !WRITER.write(CHUNKS).isPresent();
  }

  @Test
  public void testNonEmptyChunksReturnNonEmpty() throws Exception {
    final WaveHeaderFactory             HEADERS = new WaveHeaderFactory();
    final WaveFileWriter                WRITER  = new WaveFileWriter(HEADERS);
    final List<CheckpointingAudioChunk> CHUNKS  = new LinkedList<>();

    CHUNKS.add(newChunk(new float[10], new LinkedList<>()));
    assert WRITER.write(CHUNKS).isPresent();

    CHUNKS.add(newChunk(new float[10], new LinkedList<>()));
    assert WRITER.write(CHUNKS).isPresent();
  }

  @Test
  public void testAllChunksFullyWritten() throws Exception {
    final WaveHeaderFactory             HEADERS = new WaveHeaderFactory();
    final WaveFileWriter                WRITER  = new WaveFileWriter(HEADERS);
    final List<CheckpointingAudioChunk> CHUNKS  = new LinkedList<>();

    final float[] CHUNK1 = new float[] {0, 1, 2, 3, 4};
    final float[] CHUNK2 = new float[] {5, 6, 7, 8, 9};

    CHUNKS.add(newChunk(CHUNK1, new LinkedList<>()));
    CHUNKS.add(newChunk(CHUNK2, new LinkedList<>()));

    final WaveHeader                      HEADER     = HEADERS.create(CHUNKS);
    final Optional<ByteArrayOutputStream> OUT_STREAM = WRITER.write(CHUNKS);
    final byte[]                          OUT_BYTES  = OUT_STREAM.get().toByteArray();

    assert HEADER.getNumBytes() == Short.BYTES * (CHUNK1.length + CHUNK2.length);
    assert WaveHeader.HEADER_LENGTH + HEADER.getNumBytes() == OUT_BYTES.length;
  }

}
