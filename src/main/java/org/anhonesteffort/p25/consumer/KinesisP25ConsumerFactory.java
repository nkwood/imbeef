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
import org.anhonesteffort.kinesis.consumer.KinesisRecordConsumer;
import org.anhonesteffort.kinesis.consumer.KinesisRecordConsumerFactory;
import org.anhonesteffort.p25.call.CallManager;

public class KinesisP25ConsumerFactory extends KinesisRecordConsumerFactory {

  private final DataUnitFactory factory = new DataUnitFactory();
  private final CallManager callManager;

  public KinesisP25ConsumerFactory(CallManager callManager) {
    this.callManager = callManager;
  }

  @Override
  protected KinesisRecordConsumer create(SettableFuture<ShutdownReason> shutdown) {
    return new KinesisP25Consumer(shutdown, factory, callManager);
  }

}
