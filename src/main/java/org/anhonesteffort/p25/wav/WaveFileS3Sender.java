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

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import io.radiowitness.proto.p25.ProtoP25Factory;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.anhonesteffort.p25.ImbeefConfig;
import org.anhonesteffort.p25.ImbeefMetrics;
import org.anhonesteffort.p25.P25Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class WaveFileS3Sender implements S3ProgressListener {

  public static final String METADATA_CHANNEL_ID = "channel_id";
  public static final String METADATA_TERMINATED = "terminated";
  public static final String METADATA_START_TIME = "start_time";
  public static final String METADATA_END_TIME   = "end_time";
  public static final String METADATA_LATITUDE   = "latitude";
  public static final String METADATA_LONGITUDE  = "longitude";

  private static final Logger log = LoggerFactory.getLogger(WaveFileS3Sender.class);
  private final List<CheckpointingAudioChunk> chunks = new LinkedList<>();
  private final ProtoP25Factory proto = new ProtoP25Factory();

  private final ImbeefConfig    config;
  private final WaveFileWriter  waveWriter;
  private final TransferManager transferManager;
  private       P25ChannelId    channelId;

  public WaveFileS3Sender(ImbeefConfig    config,
                          WaveFileWriter  waveWriter,
                          TransferManager transferManager)
  {
    this.config          = config;
    this.waveWriter      = waveWriter;
    this.transferManager = transferManager;
  }

  private P25ChannelId correctChannelId(P25ChannelId channelId) {
    OptionalInt sourceId = chunks.stream()
                                 .mapToInt(CheckpointingAudioChunk::getSourceId)
                                 .filter(i -> (i != P25Config.UNIT_ID_NONE))
                                 .findAny();

    if (!sourceId.isPresent()) {
      return channelId;
    }

    switch (channelId.getType()) {
      case TRAFFIC_DIRECT:
        return proto.directId(
            channelId.getWacn(), channelId.getSystemId(),
            channelId.getRfSubsystemId(), sourceId.getAsInt(), channelId.getDestinationId()
        ).build();

      case TRAFFIC_GROUP:
        return proto.groupId(
            channelId.getWacn(), channelId.getSystemId(), channelId.getRfSubsystemId(),
            sourceId.getAsInt(), channelId.getGroupId(), channelId.getFrequency()
        ).build();

      default:
        throw new IllegalStateException("what is this " + channelId.getType() + "?!");
    }
  }

  public void queue(CheckpointingAudioChunk chunk) {
    chunks.add(chunk);
    if (channelId == null || channelId.getSourceId() == P25Config.UNIT_ID_NONE) {
      channelId = correctChannelId(chunk.getChannelId());
    }
  }

  private Long getStartTime() {
    return chunks.stream()
                 .mapToLong(CheckpointingAudioChunk::getStartTime)
                 .min()
                 .getAsLong();
  }

  private Long getEndTime() {
    return chunks.stream()
                 .mapToLong(CheckpointingAudioChunk::getEndTime)
                 .max()
                 .getAsLong();
  }

  private Double getLatitude() {
    return chunks.stream().mapToDouble(CheckpointingAudioChunk::getLatitude).findAny().getAsDouble();
  }

  private Double getLongitude() {
    return chunks.stream().mapToDouble(CheckpointingAudioChunk::getLongitude).findAny().getAsDouble();
  }

  private Boolean wasTerminated() {
    return chunks.stream().anyMatch(CheckpointingAudioChunk::wasTerminated);
  }

  private ObjectMetadata metadata(long length) {
    ObjectMetadata metadata = new ObjectMetadata();

    metadata.setContentLength(length);
    metadata.addUserMetadata(METADATA_CHANNEL_ID, proto.toString(channelId));
    metadata.addUserMetadata(METADATA_TERMINATED, wasTerminated().toString());
    metadata.addUserMetadata(METADATA_START_TIME, getStartTime().toString());
    metadata.addUserMetadata(METADATA_END_TIME,   getEndTime().toString());
    metadata.addUserMetadata(METADATA_LATITUDE,   getLatitude().toString());
    metadata.addUserMetadata(METADATA_LONGITUDE,  getLongitude().toString());

    return metadata;
  }

  private String key() {
    String prefix = config.getS3KeyPrefix()      +
                    channelId.getWacn()          + "/" +
                    channelId.getSystemId()      + "/" +
                    channelId.getRfSubsystemId() + "/" +
                    channelId.getType().name()   + ":";

    switch (channelId.getType()) {
      case TRAFFIC_DIRECT:
        prefix += channelId.getSourceId() + ":" + channelId.getDestinationId();
        break;

      case TRAFFIC_GROUP:
        prefix += channelId.getSourceId() + ":" + channelId.getGroupId() + ":" + channelId.getFrequency();
        break;

      default:
        throw new IllegalStateException("don't know how to work with this " + proto.toString(channelId));
    }

    return prefix + ":" + System.currentTimeMillis() + ".wav";
  }

  public void writeAndSend() throws IOException {
    Optional<ByteArrayOutputStream> outStream = waveWriter.write(chunks);

    if (!outStream.isPresent()) {
      chunks.forEach(CheckpointingAudioChunk::checkpoint);
    } else {
      byte[]               outBytes = outStream.get().toByteArray();
      ByteArrayInputStream inStream = new ByteArrayInputStream(outBytes);
      ObjectMetadata       metadata = metadata(outBytes.length);

      ImbeefMetrics.getInstance().wavSize(outBytes.length);
      ImbeefMetrics.getInstance().wavQueued();
      log.info(proto.toString(channelId) + " wave file queued for s3 upload");
      transferManager.upload(
          new PutObjectRequest(config.getS3Bucket(), key(), inStream, metadata),
          this
      );
    }
  }

  @Override
  public void onPersistableTransfer(PersistableTransfer transfer) { }

  @Override
  public void progressChanged(ProgressEvent event) {
    switch (event.getEventType()) {
      case TRANSFER_COMPLETED_EVENT:
        ImbeefMetrics.getInstance().wavPutSuccess();
        log.info(proto.toString(channelId) + " wave file successfully put to s3");
        chunks.forEach(CheckpointingAudioChunk::checkpoint);
        break;

      case TRANSFER_CANCELED_EVENT:
      case TRANSFER_FAILED_EVENT:
      case TRANSFER_PART_FAILED_EVENT:
        ImbeefMetrics.getInstance().wavPutFailure();
        log.error(proto.toString(channelId) + " error sending wave file " + event.toString());
        break;
    }
  }

}
