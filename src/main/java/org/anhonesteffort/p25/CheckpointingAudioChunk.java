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

import io.radiowitness.kinesis.consumer.Checkpointer;
import org.anhonesteffort.dsp.Copyable;

import java.nio.FloatBuffer;
import java.util.Arrays;
import java.util.Collection;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class CheckpointingAudioChunk implements Copyable<CheckpointingAudioChunk> {

  private final P25ChannelId             channelId;
  private final Boolean                  isFirst;
  private final Boolean                  isLast;
  private final Boolean                  terminated;
  private final Long                     startTime;
  private final Long                     endTime;
  private final Double                   latitude;
  private final Double                   longitude;
  private final Integer                  sourceId;
  private final FloatBuffer              buffer;
  private final Collection<Checkpointer> checkpoints;

  public CheckpointingAudioChunk(
      P25ChannelId channelId, Boolean isFirst, Boolean isLast, Boolean terminated,
      Long startTime, Long endTime, Double latitude, Double longitude, Integer sourceId,
      FloatBuffer buffer, Collection<Checkpointer> checkpoints)
  {
    this.channelId   = channelId;
    this.isFirst     = isFirst;
    this.isLast      = isLast;
    this.terminated  = terminated;
    this.startTime   = startTime;
    this.endTime     = endTime;
    this.latitude    = latitude;
    this.longitude   = longitude;
    this.sourceId    = sourceId;
    this.buffer      = buffer;
    this.checkpoints = checkpoints;
  }

  public P25ChannelId getChannelId() {
    return channelId;
  }

  public Boolean isFirst() {
    return isFirst;
  }

  public Boolean isLast() {
    return isLast;
  }

  public Boolean wasTerminated() {
    return terminated;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public Integer getSourceId() {
    return sourceId;
  }

  public FloatBuffer getBuffer() {
    return buffer;
  }

  public void checkpoint() throws RuntimeException {
    checkpoints.forEach(Checkpointer::checkpoint);
  }

  @Override
  public CheckpointingAudioChunk copy() {
    float[] floats = buffer.array();
    return new CheckpointingAudioChunk(
        channelId, isFirst, isLast, terminated,
        startTime, endTime, latitude, longitude, sourceId,
        FloatBuffer.wrap(Arrays.copyOf(floats, floats.length)), checkpoints
    );
  }

}
