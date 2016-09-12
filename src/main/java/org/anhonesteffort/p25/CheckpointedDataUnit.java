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

import org.anhonesteffort.kinesis.consumer.Checkpointer;
import org.anhonesteffort.p25.protocol.frame.DataUnit;

public class CheckpointedDataUnit {

  private final Long         timestamp;
  private final Double       latitude;
  private final Double       longitude;
  private final DataUnit     dataUnit;
  private final Checkpointer checkpointer;

  public CheckpointedDataUnit(
      Long timestamp, Double latitude, Double longitude, DataUnit dataUnit, Checkpointer checkpointer
  ) {
    this.timestamp    = timestamp;
    this.latitude     = latitude;
    this.longitude    = longitude;
    this.dataUnit     = dataUnit;
    this.checkpointer = checkpointer;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public DataUnit getDataUnit() {
    return dataUnit;
  }

  public Checkpointer getCheckpointer() {
    return checkpointer;
  }

}
