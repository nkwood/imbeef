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

import com.amazonaws.services.s3.transfer.TransferManager;
import io.radiowitness.proto.p25.ProtoP25Factory;
import org.anhonesteffort.dsp.Sink;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.anhonesteffort.p25.ImbeefConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class WaveFileS3Sink implements Sink<CheckpointingAudioChunk> {

  private final ProtoP25Factory               proto     = new ProtoP25Factory();
  private final Map<String, WaveFileS3Sender> senderMap = new HashMap<>();

  private final ImbeefConfig      config;
  private final WaveHeaderFactory headers;
  private final TransferManager   transferManager;

  public WaveFileS3Sink(ImbeefConfig      config,
                        WaveHeaderFactory headers,
                        TransferManager   transferManager)
  {
    this.config          = config;
    this.headers         = headers;
    this.transferManager = transferManager;
  }

  private String key(P25ChannelId channelId) {
    return proto.toString(channelId);
  }

  @Override
  public void consume(CheckpointingAudioChunk chunk) {
    if (chunk.isFirst() && !chunk.isLast()) {
      WaveFileWriter   writer   = new WaveFileWriter(headers);
      WaveFileS3Sender sender   = new WaveFileS3Sender(config, writer, transferManager);
      WaveFileS3Sender previous = senderMap.put(key(chunk.getChannelId()), sender);

      if (previous != null) {
        throw new RuntimeException("sender map should not have previous value on first chunk");
      } else {
        sender.queue(chunk);
      }
    } else if (!chunk.isFirst() && !chunk.isLast()) {
      senderMap.get(key(chunk.getChannelId())).queue(chunk);
    } else if (!chunk.isFirst() && chunk.isLast()) {
      WaveFileS3Sender sender = senderMap.remove(key(chunk.getChannelId()));

      try {

        sender.queue(chunk);
        sender.writeAndSend();

      } catch (IOException e) {
        throw new RuntimeException("error writing wav file", e);
      }
    } else {
      WaveFileWriter   writer = new WaveFileWriter(headers);
      WaveFileS3Sender sender = new WaveFileS3Sender(config, writer, transferManager);

      if (senderMap.containsKey(key(chunk.getChannelId()))) {
        throw new RuntimeException("sender map should not have previous value on first chunk");
      } else {
        try {

          sender.queue(chunk);
          sender.writeAndSend();

        } catch (IOException e) {
          throw new RuntimeException("error writing wav file", e);
        }
      }
    }

  }
}
