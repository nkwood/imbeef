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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import org.anhonesteffort.kinesis.consumer.KinesisConsumerConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ImbeefConfig implements KinesisConsumerConfig {

  private final Region  region;
  private final String  streamName;
  private final String  accessKeyId;
  private final String  secretKey;
  private final String  appName;
  private final String  appVersion;

  private final Long    readIntervalMs;
  private final Double  minCallDataUnitRate;
  private final Double  callInactiveCheckRate;
  private final Long    terminatorTimeoutMs;
  private final Integer maxAudioChunkSize;

  private final Integer s3PoolSize;
  private final String  s3Bucket;
  private final String  s3KeyPrefix;

  public ImbeefConfig() throws IOException {
    Properties properties = new Properties();
    properties.load(new FileInputStream("imbeef.properties"));

    region      = RegionUtils.getRegion(properties.getProperty("region"));
    streamName  = properties.getProperty("stream_name");
    accessKeyId = properties.getProperty("access_key_id");
    secretKey   = properties.getProperty("secret_key");
    appName     = properties.getProperty("app_name");
    appVersion  = properties.getProperty("app_version");

    readIntervalMs        = Long.parseLong(properties.getProperty("read_interval_ms"));
    minCallDataUnitRate   = Double.parseDouble(properties.getProperty("min_call_data_unit_rate"));
    callInactiveCheckRate = Double.parseDouble(properties.getProperty("call_inactive_check_rate"));
    terminatorTimeoutMs   = Long.parseLong(properties.getProperty("terminator_timeout_ms"));
    maxAudioChunkSize     = Integer.parseInt(properties.getProperty("max_audio_chunk_size"));

    s3PoolSize  = Integer.parseInt(properties.getProperty("s3_pool_size"));
    s3Bucket    = properties.getProperty("s3_bucket");
    s3KeyPrefix = properties.getProperty("s3_key_prefix");

    if (region == null) {
      throw new IOException("invalid region");
    }
  }

  @Override
  public Region getRegion() {
    return region;
  }

  @Override
  public String getStreamName() {
    return streamName;
  }

  @Override
  public String getAccessKeyId() {
    return accessKeyId;
  }

  @Override
  public String getSecretKey() {
    return secretKey;
  }

  @Override
  public String getAppName() {
    return appName;
  }

  @Override
  public String getAppVersion() {
    return appVersion;
  }

  @Override
  public Long getReadIntervalMs() {
    return readIntervalMs;
  }

  public Double getMinCallDataUnitRate() {
    return minCallDataUnitRate;
  }

  public Double getCallInactiveCheckRate() {
    return callInactiveCheckRate;
  }

  public Long getTerminatorTimeoutMs() {
    return terminatorTimeoutMs;
  }

  public Integer getMaxAudioChunkSize() {
    return maxAudioChunkSize;
  }

  public Integer getS3PoolSize() {
    return s3PoolSize;
  }

  public String getS3Bucket() {
    return s3Bucket;
  }

  public String getS3KeyPrefix() {
    return s3KeyPrefix;
  }

}
