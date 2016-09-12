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

import org.anhonesteffort.jmbe.iface.AudioConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SafeAudioConverter {

  private static final Logger log = LoggerFactory.getLogger(SafeAudioConverter.class);
  private final AudioConverter audioConverter;

  public SafeAudioConverter(AudioConverter audioConverter) {
    this.audioConverter = audioConverter;
  }

  public float[] decode(byte[] bytes) {
    try {

      return audioConverter.decode(bytes);

    } catch (Throwable e) {
      log.error("this is why I did this", e);
    }

    return new float[10];
  }

}
