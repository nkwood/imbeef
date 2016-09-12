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

package org.anhonesteffort.p25;

import com.amazonaws.services.s3.transfer.TransferManager;
import org.anhonesteffort.jmbe.iface.AudioConverter;
import org.anhonesteffort.p25.audio.ImbeConverterFactory;
import org.anhonesteffort.p25.call.CallSlicer;
import org.anhonesteffort.p25.call.SafeAudioConverter;
import org.anhonesteffort.p25.call.SaneCallFilter;
import org.anhonesteffort.p25.wav.WaveFileS3Sink;
import org.anhonesteffort.p25.wav.WaveHeaderFactory;

import static org.anhonesteffort.kinesis.proto.ProtoP25.P25ChannelId;

public class CallPipelineFactory {

  private final ImbeefConfig         config;
  private final ImbeConverterFactory converters;
  private final WaveHeaderFactory    headers;
  private final TransferManager      transfers;

  public CallPipelineFactory(ImbeefConfig         config,
                             ImbeConverterFactory converters,
                             WaveHeaderFactory    headers,
                             TransferManager      transfers)
  {
    this.config     = config;
    this.converters = converters;
    this.headers    = headers;
    this.transfers  = transfers;
  }

  public CallPipeline create(P25ChannelId.Reader channelId) {
    ImbeefMetrics.getInstance().createPipeline();

    AudioConverter converter = converters.create(ImbeConverterFactory.AUDIO_FORMAT_8khz).get();
    CallSlicer     slicer    = new CallSlicer(config, channelId, new SafeAudioConverter(converter));
    SaneCallFilter sanity    = new SaneCallFilter();
    WaveFileS3Sink s3Sink    = new WaveFileS3Sink(config, headers, transfers);

    slicer.addSink(sanity);
    sanity.addSink(s3Sink);

    return slicer;
  }

}
