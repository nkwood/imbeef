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

package org.anhonesteffort.p25.call;

import io.radiowitness.kinesis.consumer.Checkpointer;
import io.radiowitness.proto.p25.ProtoP25Factory;
import org.anhonesteffort.dsp.Sink;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.FloatBuffer;
import java.util.LinkedList;
import java.util.List;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class SaneCallFilterTest {

  private static class CheckpointingSink implements Sink<CheckpointingAudioChunk> {
    private CheckpointingAudioChunk last;

    @Override
    public void consume(CheckpointingAudioChunk chunk) {
      last = chunk;
      last.checkpoint();
    }

    public CheckpointingAudioChunk takeLast() {
      CheckpointingAudioChunk temp = last;
      last = null;
      return temp;
    }
  }

  private CheckpointingAudioChunk newChunk(
      boolean first, boolean last, boolean empty, List<Checkpointer> checks
  ) {
    final P25ChannelId CHANNEL = new ProtoP25Factory().controlId(1, 2, 3, 4).build();
    final FloatBuffer  FLOATS  = FloatBuffer.allocate(10);

    if (empty) {
      FLOATS.limit(0);
    } else {
      FLOATS.limit(10);
    }

    return new CheckpointingAudioChunk(CHANNEL, first, last, false, 10l, 20l, 10d, 20d, 30, FLOATS, checks);
  }

  @Test
  public void testNonEmptyChunksAlwaysBroadcast() {
    final SaneCallFilter    SANITY = new SaneCallFilter();
    final CheckpointingSink SINK   = new CheckpointingSink();

    SANITY.addSink(SINK);
    CheckpointingAudioChunk CHUNK;

    CHUNK = newChunk(false, false, false, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() != null;

    CHUNK = newChunk(true, false, false, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() != null;

    CHUNK = newChunk(false, true, false, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() != null;

    CHUNK = newChunk(true, true, false, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() != null;
  }

  @Test
  public void testLastAndNotFirstChunksAlwaysBroadcast() {
    final SaneCallFilter    SANITY = new SaneCallFilter();
    final CheckpointingSink SINK   = new CheckpointingSink();

    SANITY.addSink(SINK);
    CheckpointingAudioChunk CHUNK;

    CHUNK = newChunk(false, true, false, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() != null;

    CHUNK = newChunk(false, true, true, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() != null;
  }

  @Test
  public void testFirstAndNotLastAndEmptyChunkNotBroadcast() {
    final SaneCallFilter    SANITY = new SaneCallFilter();
    final CheckpointingSink SINK   = new CheckpointingSink();

    SANITY.addSink(SINK);
    CheckpointingAudioChunk CHUNK;

    CHUNK = newChunk(true, false, true, new LinkedList<>());
    SANITY.consume(CHUNK);
    assert SINK.takeLast() == null;
  }

  @Test
  public void testAllChunksAlwaysCheckpointed() {
    final SaneCallFilter           SANITY = new SaneCallFilter();
    final CheckpointingSink        SINK   = new CheckpointingSink();
    final Checkpointer             CHECK  = Mockito.mock(Checkpointer.class);
    final LinkedList<Checkpointer> CHECKS = new LinkedList<>();

    CHECKS.add(CHECK);
    SANITY.addSink(SINK);
    CheckpointingAudioChunk CHUNK;

    Mockito.verify(CHECK, Mockito.never()).checkpoint();

    CHUNK = newChunk(false, false, false, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(1)).checkpoint();

    CHUNK = newChunk(false, false, true, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(2)).checkpoint();

    CHUNK = newChunk(false, true, false, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(3)).checkpoint();

    CHUNK = newChunk(false, true, true, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(4)).checkpoint();

    CHUNK = newChunk(true, false, false, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(5)).checkpoint();

    CHUNK = newChunk(true, false, true, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(6)).checkpoint();

    CHUNK = newChunk(true, true, false, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(7)).checkpoint();

    CHUNK = newChunk(true, true, true, CHECKS);
    SANITY.consume(CHUNK);
    Mockito.verify(CHECK, Mockito.times(8)).checkpoint();
  }

}
