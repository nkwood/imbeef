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

package org.anhonesteffort.p25.consumer;

import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.google.common.util.concurrent.SettableFuture;
import org.anhonesteffort.kinesis.consumer.Checkpointer;
import org.anhonesteffort.kinesis.proto.ProtoP25Factory;
import org.anhonesteffort.p25.call.CallManager;
import org.anhonesteffort.p25.protocol.frame.DataUnit;
import org.capnproto.MessageBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.anhonesteffort.kinesis.proto.Proto.BaseMessage;
import static org.anhonesteffort.kinesis.proto.ProtoP25.P25DataUnit;
import static org.anhonesteffort.kinesis.proto.ProtoP25.P25ChannelId;

public class KinesisP25ConsumerTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testGetType() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE   = SettableFuture.create();
    final DataUnitFactory                FACTORY  = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS    = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);

    assert CONSUMER.getType().get().equals(BaseMessage.Type.P25_DATA_UNIT);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testControlDataCheckpointed() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE     = SettableFuture.create();
    final DataUnitFactory                FACTORY    = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS      = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER   = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);
    final Checkpointer                   CHECKPOINT = Mockito.mock(Checkpointer.class);

    final ProtoP25Factory     PROTO      = new ProtoP25Factory();
    final P25ChannelId.Reader CHANNEL_ID = PROTO.controlId(1, 2, 3, 4);
    final P25DataUnit.Reader  PROTO_DU   = PROTO.dataUnit(CHANNEL_ID, 5d, 6d, 7, 8, new byte[7]);
    final MessageBuilder      MESSAGE    = PROTO.message(System.currentTimeMillis(), PROTO_DU);

    CONSUMER.process(MESSAGE.getRoot(BaseMessage.factory).asReader(), CHECKPOINT);
    Mockito.verify(CHECKPOINT, Mockito.times(1)).checkpoint();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testShutdownOnCorruptDataUnit() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE     = SettableFuture.create();
    final DataUnitFactory                FACTORY    = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS      = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER   = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);
    final Checkpointer                   CHECKPOINT = Mockito.mock(Checkpointer.class);

    final DataUnit DATA_UNIT = Mockito.mock(DataUnit.class);
    Mockito.when(DATA_UNIT.isIntact()).thenReturn(false);
    Mockito.when(FACTORY.create(Mockito.any())).thenReturn(DATA_UNIT);

    final ProtoP25Factory     PROTO      = new ProtoP25Factory();
    final P25ChannelId.Reader CHANNEL_ID = PROTO.directId(1, 2, 3, 4, 5);
    final P25DataUnit.Reader  PROTO_DU   = PROTO.dataUnit(CHANNEL_ID, 6d, 7d, 8, 9, new byte[8]);
    final MessageBuilder      MESSAGE    = PROTO.message(System.currentTimeMillis(), PROTO_DU);

    CONSUMER.process(MESSAGE.getRoot(BaseMessage.factory).asReader(), CHECKPOINT);
    Mockito.verify(CHECKPOINT, Mockito.never()).checkpoint();

    try {

      assert FUTURE.isDone();
      FUTURE.get();
      assert false;

    } catch (ExecutionException e) {
      assert (e.getCause() instanceof IOException);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testControlDataNotProcessed() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE     = SettableFuture.create();
    final DataUnitFactory                FACTORY    = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS      = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER   = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);
    final Checkpointer                   CHECKPOINT = Mockito.mock(Checkpointer.class);

    final ProtoP25Factory     PROTO      = new ProtoP25Factory();
    final P25ChannelId.Reader CHANNEL_ID = PROTO.controlId(1, 2, 3, 4);
    final P25DataUnit.Reader  PROTO_DU   = PROTO.dataUnit(CHANNEL_ID, 5d, 6d, 7, 8, new byte[7]);
    final MessageBuilder      MESSAGE    = PROTO.message(System.currentTimeMillis(), PROTO_DU);

    CONSUMER.process(MESSAGE.getRoot(BaseMessage.factory).asReader(), CHECKPOINT);

    assert !FUTURE.isDone();
    Mockito.verify(FACTORY, Mockito.never()).create(Mockito.any());
    Mockito.verify(CALLS, Mockito.never()).process(Mockito.any(), Mockito.any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCorruptDataUnitNotProcessed() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE     = SettableFuture.create();
    final DataUnitFactory                FACTORY    = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS      = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER   = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);
    final Checkpointer                   CHECKPOINT = Mockito.mock(Checkpointer.class);

    final DataUnit DATA_UNIT = Mockito.mock(DataUnit.class);
    Mockito.when(DATA_UNIT.isIntact()).thenReturn(false);
    Mockito.when(FACTORY.create(Mockito.any())).thenReturn(DATA_UNIT);

    final ProtoP25Factory     PROTO      = new ProtoP25Factory();
    final P25ChannelId.Reader CHANNEL_ID = PROTO.directId(1, 2, 3, 4, 5);
    final P25DataUnit.Reader  PROTO_DU   = PROTO.dataUnit(CHANNEL_ID, 6d, 7d, 8, 9, new byte[8]);
    final MessageBuilder      MESSAGE    = PROTO.message(System.currentTimeMillis(), PROTO_DU);

    CONSUMER.process(MESSAGE.getRoot(BaseMessage.factory).asReader(), CHECKPOINT);

    assert FUTURE.isDone();
    Mockito.verify(CALLS, Mockito.never()).process(Mockito.any(), Mockito.any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDirectTrafficProcessed() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE     = SettableFuture.create();
    final DataUnitFactory                FACTORY    = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS      = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER   = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);
    final Checkpointer                   CHECKPOINT = Mockito.mock(Checkpointer.class);

    final DataUnit DATA_UNIT = Mockito.mock(DataUnit.class);
    Mockito.when(DATA_UNIT.isIntact()).thenReturn(true);
    Mockito.when(FACTORY.create(Mockito.any())).thenReturn(DATA_UNIT);

    final ProtoP25Factory     PROTO      = new ProtoP25Factory();
    final P25ChannelId.Reader CHANNEL_ID = PROTO.directId(1, 2, 3, 4, 5);
    final P25DataUnit.Reader  PROTO_DU   = PROTO.dataUnit(CHANNEL_ID, 6d, 7d, 8, 9, new byte[8]);
    final MessageBuilder      MESSAGE    = PROTO.message(System.currentTimeMillis(), PROTO_DU);

    CONSUMER.process(MESSAGE.getRoot(BaseMessage.factory).asReader(), CHECKPOINT);
    Mockito.verify(CALLS, Mockito.times(1)).process(Mockito.any(), Mockito.any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGroupTrafficProcessed() throws Exception {
    final SettableFuture<ShutdownReason> FUTURE     = SettableFuture.create();
    final DataUnitFactory                FACTORY    = Mockito.mock(DataUnitFactory.class);
    final CallManager                    CALLS      = Mockito.mock(CallManager.class);
    final KinesisP25Consumer             CONSUMER   = new KinesisP25Consumer(FUTURE, FACTORY, CALLS);
    final Checkpointer                   CHECKPOINT = Mockito.mock(Checkpointer.class);

    final DataUnit DATA_UNIT = Mockito.mock(DataUnit.class);
    Mockito.when(DATA_UNIT.isIntact()).thenReturn(true);
    Mockito.when(FACTORY.create(Mockito.any())).thenReturn(DATA_UNIT);

    final ProtoP25Factory     PROTO      = new ProtoP25Factory();
    final P25ChannelId.Reader CHANNEL_ID = PROTO.groupId(1, 2, 3, 4, 5, 6d);
    final P25DataUnit.Reader  PROTO_DU   = PROTO.dataUnit(CHANNEL_ID, 7d, 8d, 9, 10, new byte[8]);
    final MessageBuilder      MESSAGE    = PROTO.message(System.currentTimeMillis(), PROTO_DU);

    CONSUMER.process(MESSAGE.getRoot(BaseMessage.factory).asReader(), CHECKPOINT);
    Mockito.verify(CALLS, Mockito.times(1)).process(Mockito.any(), Mockito.any());
  }

}
