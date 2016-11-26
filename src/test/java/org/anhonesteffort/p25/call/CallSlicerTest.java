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
import org.anhonesteffort.p25.ImbeefConfig;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.anhonesteffort.p25.CheckpointedDataUnit;
import org.anhonesteffort.p25.ImbeefMetrics;
import org.anhonesteffort.p25.MockMetrics;
import org.anhonesteffort.p25.protocol.Duid;
import org.anhonesteffort.p25.protocol.Nid;
import org.anhonesteffort.p25.protocol.frame.DataUnit;
import org.anhonesteffort.p25.protocol.frame.HeaderDataUnit;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit1;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit2;
import org.anhonesteffort.p25.protocol.frame.SimpleTerminatorDataUnit;
import org.anhonesteffort.p25.protocol.frame.VoiceFrame;
import org.anhonesteffort.p25.protocol.frame.linkcontrol.GroupVoiceUserLwc;
import org.anhonesteffort.p25.protocol.frame.linkcontrol.LinkControlWord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class CallSlicerTest {

  private P25ChannelId channel() {
    return new ProtoP25Factory().groupId(
        1, 2, 3, 4, 5, 6d
    ).build();
  }

  private DataUnit hdu() {
    Duid           duid     = new Duid(Duid.ID_HEADER);
    Nid            nid      = new Nid(1337, duid, true);
    HeaderDataUnit dataUnit = Mockito.mock(HeaderDataUnit.class);

    Mockito.when(dataUnit.getNid()).thenReturn(nid);
    Mockito.when(dataUnit.getAlgorithmId()).thenReturn(0x80);

    return dataUnit;
  }

  private DataUnit hduCrypt() {
    Duid           duid     = new Duid(Duid.ID_HEADER);
    Nid            nid      = new Nid(1337, duid, true);
    HeaderDataUnit dataUnit = Mockito.mock(HeaderDataUnit.class);

    Mockito.when(dataUnit.getNid()).thenReturn(nid);
    Mockito.when(dataUnit.getAlgorithmId()).thenReturn(0x83);

    return dataUnit;
  }

  private DataUnit lldu1() {
    Duid                 duid        = new Duid(Duid.ID_LLDU1);
    Nid                  nid         = new Nid(1337, duid, true);
    VoiceFrame[]         voiceFrames = new VoiceFrame[] {new VoiceFrame(new byte[10])};
    LogicalLinkDataUnit1 dataUnit    = Mockito.mock(LogicalLinkDataUnit1.class);
    GroupVoiceUserLwc    word        = Mockito.mock(GroupVoiceUserLwc.class);

    Mockito.when(word.getLinkControlOpcode()).thenReturn(LinkControlWord.LCF_GROUP);
    Mockito.when(word.getSourceId()).thenReturn(0x1020);

    Mockito.when(dataUnit.getNid()).thenReturn(nid);
    Mockito.when(dataUnit.getVoiceFrames()).thenReturn(voiceFrames);
    Mockito.when(dataUnit.getLinkControlWord()).thenReturn(word);

    return dataUnit;
  }

  private DataUnit lldu2() {
    Duid                 duid        = new Duid(Duid.ID_LLDU2);
    Nid                  nid         = new Nid(1337, duid, true);
    VoiceFrame[]         voiceFrames = new VoiceFrame[] {new VoiceFrame(new byte[10])};
    LogicalLinkDataUnit2 dataUnit    = Mockito.mock(LogicalLinkDataUnit2.class);

    Mockito.when(dataUnit.getNid()).thenReturn(nid);
    Mockito.when(dataUnit.getVoiceFrames()).thenReturn(voiceFrames);
    Mockito.when(dataUnit.getAlgorithmId()).thenReturn(0x80);

    return dataUnit;
  }

  private DataUnit lldu2Crypt() {
    Duid                 duid        = new Duid(Duid.ID_LLDU2);
    Nid                  nid         = new Nid(1337, duid, true);
    VoiceFrame[]         voiceFrames = new VoiceFrame[] {new VoiceFrame(new byte[10])};
    LogicalLinkDataUnit2 dataUnit    = Mockito.mock(LogicalLinkDataUnit2.class);

    Mockito.when(dataUnit.getNid()).thenReturn(nid);
    Mockito.when(dataUnit.getVoiceFrames()).thenReturn(voiceFrames);
    Mockito.when(dataUnit.getAlgorithmId()).thenReturn(0x83);

    return dataUnit;
  }

  private DataUnit terminator() {
    Duid     duid     = new Duid(Duid.ID_TERMINATOR_WO_LINK);
    Nid      nid      = new Nid(1337, duid, true);
    DataUnit dataUnit = Mockito.mock(SimpleTerminatorDataUnit.class);

    Mockito.when(dataUnit.getNid()).thenReturn(nid);

    return dataUnit;
  }

  @Before
  public void mockMetrics() {
    final ImbeefMetrics mock = Mockito.mock(ImbeefMetrics.class);
    MockMetrics.mockWith(mock);
  }

  @Test
  public void testMinCallDataUnitRate() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    assert SLICER.isInactive(System.currentTimeMillis());
    SLICER.consume(new CheckpointedDataUnit(System.currentTimeMillis(), 10d, 20d, lldu1(), CHECK));
    assert SLICER.isInactive(System.currentTimeMillis() + 800);
    SLICER.consume(new CheckpointedDataUnit(System.currentTimeMillis(), 10d, 20d, lldu1(), CHECK));
    assert !SLICER.isInactive(System.currentTimeMillis() + 400);
  }

  @Test
  public void testCallWithSimpleTerminator() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME = System.currentTimeMillis();
    final Long END_TIME   = System.currentTimeMillis() + 3;

    SLICER.consume(new CheckpointedDataUnit(START_TIME, 10d, 20d, hdu(), CHECK));
    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 1, 10d, 20d, lldu1(), CHECK));
    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 2, 10d, 20d, lldu2(), CHECK));
    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(END_TIME, 10d, 20d, terminator(), CHECK));

    assert OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert OUT.getLast().wasTerminated();

    assert OUT.getLast().getStartTime().equals(START_TIME);
    assert OUT.getLast().getEndTime().equals(END_TIME);

    assert OUT.getLast().getLatitude()  == 10d;
    assert OUT.getLast().getLongitude() == 20d;

    assert OUT.getLast().getChannelId().getType().equals(CHANNEL.getType());
    assert OUT.getLast().getChannelId().getSystemId()      == CHANNEL.getSystemId();
    assert OUT.getLast().getChannelId().getRfSubsystemId() == CHANNEL.getRfSubsystemId();
    assert OUT.getLast().getChannelId().getSourceId()      == CHANNEL.getSourceId();
    assert OUT.getLast().getChannelId().getGroupId()       == CHANNEL.getGroupId();

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(4)).checkpoint();
  }

  @Test
  public void testTwoCallsWithTerminators() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME1 = System.currentTimeMillis();
    final Long END_TIME1   = System.currentTimeMillis() + 3;

    SLICER.consume(new CheckpointedDataUnit(START_TIME1,     10d, 20d, hdu(),        CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME1 + 1, 10d, 20d, lldu1(),      CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME1 + 2, 10d, 20d, lldu2(),      CHECK));
    SLICER.consume(new CheckpointedDataUnit(END_TIME1,       10d, 20d, terminator(), CHECK));

    assert OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert OUT.getLast().wasTerminated();
    assert OUT.getLast().getStartTime().equals(START_TIME1);
    assert OUT.getLast().getEndTime().equals(END_TIME1);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(4)).checkpoint();

    final Long START_TIME2 = END_TIME1   + 1;
    final Long END_TIME2   = START_TIME2 + 3;

    SLICER.consume(new CheckpointedDataUnit(START_TIME2,     10d, 20d, hdu(),        CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME2 + 1, 10d, 20d, lldu1(),      CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME2 + 2, 10d, 20d, lldu2(),      CHECK));
    SLICER.consume(new CheckpointedDataUnit(END_TIME2,       10d, 20d, terminator(), CHECK));

    assert OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert OUT.getLast().wasTerminated();
    assert OUT.getLast().getStartTime().equals(START_TIME2);
    assert OUT.getLast().getEndTime().equals(END_TIME2);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(8)).checkpoint();
  }

  @Test
  public void testCallSplitInTwoChunks() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 20;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME1 = System.currentTimeMillis();
    final Long END_TIME1   = System.currentTimeMillis() + 3;

    SLICER.consume(new CheckpointedDataUnit(START_TIME1,     10d, 20d, hdu(),   CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME1 + 1, 10d, 20d, lldu1(), CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME1 + 2, 10d, 20d, lldu2(), CHECK));
    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(END_TIME1, 10d, 20d, lldu1(), CHECK));

    assert OUT.getLast().isFirst();
    assert !OUT.getLast().isLast();
    assert OUT.getLast().getStartTime().equals(START_TIME1);
    assert OUT.getLast().getEndTime().equals(END_TIME1);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(4)).checkpoint();

    final Long START_TIME2 = END_TIME1   + 1;
    final Long END_TIME2   = START_TIME2 + 1;

    SLICER.consume(new CheckpointedDataUnit(START_TIME2, 10d, 20d, lldu2(),      CHECK));
    SLICER.consume(new CheckpointedDataUnit(END_TIME2,   10d, 20d, terminator(), CHECK));

    assert !OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert OUT.getLast().wasTerminated();
    assert OUT.getLast().getStartTime().equals(START_TIME2);
    assert OUT.getLast().getEndTime().equals(END_TIME2);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(6)).checkpoint();
  }

  @Test
  public void testEncryptedCallIgnoredButCheckpointed() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME = System.currentTimeMillis();

    SLICER.consume(new CheckpointedDataUnit(START_TIME,     10d, 20d, hduCrypt(),   CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 1, 10d, 20d, lldu1(),      CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 2, 10d, 20d, lldu2Crypt(), CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 3, 10d, 20d, terminator(), CHECK));

    assert OUT.getLast() == null;
    Mockito.verify(CHECK, Mockito.times(4)).checkpoint();
  }

  @Test
  public void testCallEndedOnInactivity() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME = System.currentTimeMillis();
    final Long END_TIME   = System.currentTimeMillis() + 2;

    SLICER.consume(new CheckpointedDataUnit(START_TIME,     10d, 20d, hdu(),   CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 1, 10d, 20d, lldu1(), CHECK));
    SLICER.consume(new CheckpointedDataUnit(END_TIME,       10d, 20d, lldu2(), CHECK));

    assert OUT.getLast() == null;
    SLICER.isInactive(END_TIME + 600);

    assert OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert !OUT.getLast().wasTerminated();
    assert OUT.getLast().getStartTime().equals(START_TIME);
    assert OUT.getLast().getEndTime().equals(END_TIME);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(3)).checkpoint();
  }

  @Test
  public void testCallEndedWithTerminatorTimeout() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME = System.currentTimeMillis();
    final Long END_TIME   = System.currentTimeMillis() + 2;

    SLICER.consume(new CheckpointedDataUnit(START_TIME,     10d, 20d, hdu(),   CHECK));
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 1, 10d, 20d, lldu1(), CHECK));
    SLICER.consume(new CheckpointedDataUnit(END_TIME,       10d, 20d, lldu2(), CHECK));

    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(END_TIME + TERMINATE_MS + 1, 10d, 20d, lldu1(), CHECK));

    assert OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert !OUT.getLast().wasTerminated();
    assert OUT.getLast().getStartTime().equals(START_TIME);
    assert OUT.getLast().getEndTime().equals(END_TIME);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(3)).checkpoint();
  }

  @Test
  public void testCallWithoutHeader() throws Exception {
    final ImbeefConfig CONFIG        = Mockito.mock(ImbeefConfig.class);
    final Integer      CHUNK_SIZE    = 8192;
    final Double       MIN_DATA_RATE = 2.0;
    final Long         TERMINATE_MS  = 1000l;

    Mockito.when(CONFIG.getMaxAudioChunkSize()).thenReturn(CHUNK_SIZE);
    Mockito.when(CONFIG.getMinCallDataUnitRate()).thenReturn(MIN_DATA_RATE);
    Mockito.when(CONFIG.getTerminatorTimeoutMs()).thenReturn(TERMINATE_MS);

    final P25ChannelId       CHANNEL = channel();
    final SafeAudioConverter DECODER = Mockito.mock(SafeAudioConverter.class);
    final Checkpointer       CHECK   = Mockito.mock(Checkpointer.class);
    final CallSlicer         SLICER  = new CallSlicer(CONFIG, CHANNEL, DECODER);
    final SaneCallFilter     SANITY  = new SaneCallFilter();
    final SimpleSink         OUT     = new SimpleSink();

    Mockito.when(DECODER.decode(Mockito.any())).thenReturn(new float[10]);

    SLICER.addSink(SANITY);
    SANITY.addSink(OUT);

    final Long START_TIME = System.currentTimeMillis();
    final Long END_TIME   = System.currentTimeMillis() + 2;

    SLICER.consume(new CheckpointedDataUnit(START_TIME, 10d, 20d, lldu1(), CHECK));
    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(START_TIME + 1, 10d, 20d, lldu2(), CHECK));
    assert OUT.getLast() == null;
    SLICER.consume(new CheckpointedDataUnit(END_TIME, 10d, 20d, terminator(), CHECK));

    assert OUT.getLast().isFirst();
    assert OUT.getLast().isLast();
    assert OUT.getLast().wasTerminated();
    assert OUT.getLast().getStartTime().equals(START_TIME);
    assert OUT.getLast().getEndTime().equals(END_TIME);

    OUT.getLast().checkpoint();
    Mockito.verify(CHECK, Mockito.times(3)).checkpoint();
  }

  private static class SimpleSink implements Sink<CheckpointingAudioChunk> {
    private CheckpointingAudioChunk last = null;
    @Override
    public void consume(CheckpointingAudioChunk chunk) {
      last = chunk;
    }

    public CheckpointingAudioChunk getLast() {
      return last;
    }
  }

}
