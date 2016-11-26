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

import io.radiowitness.proto.p25.ProtoP25Factory;
import org.anhonesteffort.p25.CallPipelineFactory;
import org.anhonesteffort.p25.ImbeefConfig;
import org.anhonesteffort.p25.CheckpointedDataUnit;
import org.anhonesteffort.p25.ImbeefMetrics;
import org.anhonesteffort.p25.MockMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class CallManagerTest {

  @Before
  public void mockMetrics() {
    final ImbeefMetrics mock = Mockito.mock(ImbeefMetrics.class);
    MockMetrics.mockWith(mock);
  }

  @Test
  public void testSlicerCreatedOnProcessCall() throws Exception {
    final ImbeefConfig CONFIG = Mockito.mock(ImbeefConfig.class);

    Mockito.when(CONFIG.getCallInactiveCheckRate()).thenReturn(1.0);

    final CallPipelineFactory CALL_FACTORY = Mockito.mock(CallPipelineFactory.class);
    final CallSlicer          CALL         = Mockito.mock(CallSlicer.class);

    Mockito.when(CALL_FACTORY.create(Mockito.any())).thenReturn(CALL);

    final CallManager          CALL_MANAGER = new CallManager(CONFIG, CALL_FACTORY);
    final ProtoP25Factory      PROTO        = new ProtoP25Factory();
    final P25ChannelId         ID           = PROTO.directId(1, 2, 3, 4, 5).build();
    final CheckpointedDataUnit DATA_UNIT    = Mockito.mock(CheckpointedDataUnit.class);

    CALL_MANAGER.process(ID, DATA_UNIT);

    Mockito.verify(CALL_FACTORY, Mockito.times(1)).create(Mockito.eq(ID));
    Mockito.verify(CALL, Mockito.times(1)).consume(Mockito.eq(DATA_UNIT));
  }

  @Test
  public void testSlicerNotDuplicatedOnProcessCall() throws Exception {
    final ImbeefConfig CONFIG = Mockito.mock(ImbeefConfig.class);

    Mockito.when(CONFIG.getCallInactiveCheckRate()).thenReturn(1.0);

    final CallPipelineFactory CALL_FACTORY = Mockito.mock(CallPipelineFactory.class);
    final CallSlicer          CALL         = Mockito.mock(CallSlicer.class);

    Mockito.when(CALL_FACTORY.create(Mockito.any())).thenReturn(CALL);

    final CallManager          CALL_MANAGER = new CallManager(CONFIG, CALL_FACTORY);
    final ProtoP25Factory      PROTO        = new ProtoP25Factory();
    final P25ChannelId         ID           = PROTO.directId(1, 2, 3, 4, 5).build();
    final CheckpointedDataUnit DATA_UNIT    = Mockito.mock(CheckpointedDataUnit.class);

    CALL_MANAGER.process(ID, DATA_UNIT);
    Mockito.verify(CALL_FACTORY, Mockito.times(1)).create(Mockito.eq(ID));

    CALL_MANAGER.process(ID, DATA_UNIT);
    Mockito.verify(CALL_FACTORY, Mockito.times(1)).create(Mockito.eq(ID));
  }

  @Test
  public void testSlicerRemovedOnInactivity() throws Exception {
    final ImbeefConfig CONFIG = Mockito.mock(ImbeefConfig.class);

    Mockito.when(CONFIG.getCallInactiveCheckRate()).thenReturn(2.0);

    final CallPipelineFactory CALL_FACTORY = Mockito.mock(CallPipelineFactory.class);
    final CallSlicer          CALL         = Mockito.mock(CallSlicer.class);

    Mockito.when(CALL_FACTORY.create(Mockito.any())).thenReturn(CALL);
    Mockito.when(CALL.isInactive(Mockito.anyLong())).thenReturn(true);

    final CallManager          CALL_MANAGER = new CallManager(CONFIG, CALL_FACTORY);
    final ProtoP25Factory      PROTO        = new ProtoP25Factory();
    final P25ChannelId         ID           = PROTO.directId(1, 2, 3, 4, 5).build();
    final CheckpointedDataUnit DATA_UNIT    = Mockito.mock(CheckpointedDataUnit.class);

    Mockito.when(CALL.getChannelId()).thenReturn(ID);

    CALL_MANAGER.process(ID, DATA_UNIT);
    Mockito.verify(CALL_FACTORY, Mockito.times(1)).create(Mockito.eq(ID));

    Thread.sleep(1000);

    CALL_MANAGER.process(ID, DATA_UNIT);
    Mockito.verify(CALL_FACTORY, Mockito.times(2)).create(Mockito.eq(ID));
  }

}
