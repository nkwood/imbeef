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

import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.anhonesteffort.p25.audio.ImbeConverterFactory;

import java.util.List;

public class WaveHeaderFactory {

  public WaveHeader create(List<CheckpointingAudioChunk> chunks) {
    int byteCount = Short.BYTES * chunks.stream().mapToInt(chunk -> chunk.getBuffer().remaining()).sum();
    return new WaveHeader(
        WaveHeader.FORMAT_PCM,
        (short)ImbeConverterFactory.CHANNEL_COUNT,
        ImbeConverterFactory.SAMPLE_RATE_8khz,
        (short) ImbeConverterFactory.SAMPLE_BIT_LENGTH,
        byteCount
    );
  }

}
