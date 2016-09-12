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
import org.anhonesteffort.kinesis.consumer.KinesisRecordConsumer;
import org.anhonesteffort.kinesis.proto.ProtoP25Factory;
import org.anhonesteffort.p25.call.CallManager;
import org.anhonesteffort.p25.CheckpointedDataUnit;
import org.anhonesteffort.p25.protocol.frame.DataUnit;

import java.io.IOException;
import java.util.Optional;

import static org.anhonesteffort.kinesis.proto.Proto.BaseMessage;
import static org.anhonesteffort.kinesis.proto.Proto.BaseMessage.Type;
import static org.anhonesteffort.kinesis.proto.ProtoP25.P25ChannelId;

public class KinesisP25Consumer extends KinesisRecordConsumer {

  private final ProtoP25Factory proto = new ProtoP25Factory();
  private final DataUnitFactory factory;
  private final CallManager     callManager;

  public KinesisP25Consumer(SettableFuture<ShutdownReason> shutdown,
                            DataUnitFactory                factory,
                            CallManager                    callManager)
  {
    super(shutdown);
    this.factory     = factory;
    this.callManager = callManager;
  }

  @Override
  protected Optional<BaseMessage.Type> getType() {
    return Optional.of(Type.P25_DATA_UNIT);
  }

  @Override
  protected void process(BaseMessage.Reader reader, Checkpointer checkpointer) throws RuntimeException {
    P25ChannelId.Reader channelId = reader.getP25DataUnit().getChannelId();

    switch (channelId.getType()) {
      case TRAFFIC_DIRECT:
      case TRAFFIC_GROUP:
        DataUnit dataUnit = factory.create(reader.getP25DataUnit());

        if (dataUnit.isIntact()) {
          callManager.process(channelId, new CheckpointedDataUnit(
              reader.getTimestamp(), reader.getP25DataUnit().getLatitude(),
              reader.getP25DataUnit().getLongitude(), dataUnit, checkpointer
          ));
        } else {
          shutdown.setException(new IOException(
              proto.toString(channelId) + " corrupted data unit " + dataUnit.toString())
          );
        }
        break;

      default:
        checkpointer.checkpoint();
    }
  }

}
