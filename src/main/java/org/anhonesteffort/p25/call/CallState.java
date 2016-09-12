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

import org.anhonesteffort.kinesis.consumer.Checkpointer;
import org.anhonesteffort.p25.P25Config;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class CallState {

  private Queue<Checkpointer> checkpoints = new ConcurrentLinkedQueue<>();

  private boolean isFirst     = true;
  private boolean isEncrypted = false;
  private double  latitude    = 0d;
  private double  longitude   = 0d;
  private int     sourceId    = P25Config.UNIT_ID_NONE;

  private long earliestRemoteTime = Long.MAX_VALUE;
  private long latestRemoteTime   = Long.MIN_VALUE;

  public void nextChunk() {
    isFirst            = false;
    earliestRemoteTime = Long.MAX_VALUE;
    latestRemoteTime   = Long.MIN_VALUE;
    checkpoints        = new ConcurrentLinkedQueue<>();
  }

  public void nextCall() {
    isFirst            = true;
    isEncrypted        = false;
    latitude           = 0d;
    longitude          = 0d;
    sourceId           = P25Config.UNIT_ID_NONE;
    earliestRemoteTime = Long.MAX_VALUE;
    latestRemoteTime   = Long.MIN_VALUE;
    checkpoints        = new ConcurrentLinkedQueue<>();
  }

  public boolean isFirst() {
    return isFirst;
  }

  public boolean isEncrypted() {
    return isEncrypted;
  }

  public void setIsEncrypted(boolean isEncrypted) {
    this.isEncrypted = isEncrypted;
  }

  public double getLatitude() {
    return latitude;
  }

  public void coordinates(double latitude, double longitude) {
    this.latitude  = latitude;
    this.longitude = longitude;
  }

  public double getLongitude() {
    return longitude;
  }

  public int getSourceId() {
    return sourceId;
  }

  public void setSourceId(int sourceId) {
    if (sourceId != P25Config.UNIT_ID_NONE) {
      this.sourceId = sourceId;
    }
  }

  public long getEarliestRemoteTime() {
    return earliestRemoteTime;
  }

  public void setEarliestRemoteTimeIfLess(long earliestRemoteTime) {
    if (earliestRemoteTime < this.earliestRemoteTime) {
      this.earliestRemoteTime = earliestRemoteTime;
    }
  }

  public long getLatestRemoteTime() {
    return latestRemoteTime;
  }

  public void setLatestRemoteTimeIfMore(long latestRemoteTime) {
    if (latestRemoteTime > this.latestRemoteTime) {
      this.latestRemoteTime = latestRemoteTime;
    }
  }

  public void addCheckpoint(Checkpointer check) {
    checkpoints.add(check);
  }

  public Queue<Checkpointer> getCheckpoints() {
    return checkpoints;
  }

}