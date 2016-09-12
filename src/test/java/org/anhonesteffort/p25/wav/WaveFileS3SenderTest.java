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
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import org.anhonesteffort.kinesis.consumer.Checkpointer;
import org.anhonesteffort.kinesis.proto.ProtoP25Factory;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.anhonesteffort.p25.ImbeefConfig;
import org.anhonesteffort.p25.ImbeefMetrics;
import org.anhonesteffort.p25.MockMetrics;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.nio.FloatBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.anhonesteffort.kinesis.proto.ProtoP25.P25ChannelId;

public class WaveFileS3SenderTest {

  private ImbeefConfig config() {
    final ImbeefConfig CONFIG = Mockito.mock(ImbeefConfig.class);
    Mockito.when(CONFIG.getS3Bucket()).thenReturn("bucket");
    Mockito.when(CONFIG.getS3KeyPrefix()).thenReturn("key");
    return CONFIG;
  }

  private CheckpointingAudioChunk newChunk(boolean empty, List<Checkpointer> checks) {
    final P25ChannelId.Reader CHANNEL = new ProtoP25Factory().groupId(1, 2, 3, 4, 5, 6d);
    final FloatBuffer         FLOATS  = FloatBuffer.allocate(10);

    if (empty) {
      FLOATS.limit(0);
    } else {
      FLOATS.limit(10);
    }

    return new CheckpointingAudioChunk(CHANNEL, true, true, true, 10l, 20l, 10d, 20d, 30, FLOATS, checks);
  }

  @Before
  public void mockMetrics() {
    final ImbeefMetrics mock = Mockito.mock(ImbeefMetrics.class);
    MockMetrics.mockWith(mock);
  }

  @Test
  public void testEmptyWavCheckpointedButNotUploaded() throws Exception {
    final ImbeefConfig     CONFIG    = config();
    final WaveFileWriter   WRITER    = Mockito.mock(WaveFileWriter.class);
    final TransferManager  TRANSFERS = Mockito.mock(TransferManager.class);
    final WaveFileS3Sender SENDER    = new WaveFileS3Sender(CONFIG, WRITER, TRANSFERS);

    Mockito.when(WRITER.write(Mockito.any())).thenReturn(Optional.<ByteArrayOutputStream>empty());

    final Checkpointer       CHECK  = Mockito.mock(Checkpointer.class);
    final List<Checkpointer> CHECKS = new LinkedList<>();

    CHECKS.add(CHECK);
    SENDER.queue(newChunk(true, CHECKS));

    Mockito.verify(CHECK, Mockito.never()).checkpoint();
    SENDER.writeAndSend();
    Mockito.verify(CHECK, Mockito.times(1)).checkpoint();
    Mockito.verify(TRANSFERS, Mockito.never()).upload(Mockito.any(PutObjectRequest.class), Mockito.any(S3ProgressListener.class));
  }

  @Test
  public void testNonEmptyWavUploaded() throws Exception {
    final ImbeefConfig     CONFIG    = config();
    final WaveFileWriter   WRITER    = Mockito.mock(WaveFileWriter.class);
    final TransferManager  TRANSFERS = Mockito.mock(TransferManager.class);
    final WaveFileS3Sender SENDER    = new WaveFileS3Sender(CONFIG, WRITER, TRANSFERS);

    final ByteArrayOutputStream WRITER_STREAM = Mockito.mock(ByteArrayOutputStream.class);
    final byte[]                WRITER_BYTES  = new byte[10];

    Mockito.when(WRITER_STREAM.toByteArray()).thenReturn(WRITER_BYTES);
    Mockito.when(WRITER.write(Mockito.any())).thenReturn(Optional.of(WRITER_STREAM));

    SENDER.queue(newChunk(false, new LinkedList<>()));

    Mockito.verify(TRANSFERS, Mockito.never()).upload(Mockito.any(PutObjectRequest.class), Mockito.any(S3ProgressListener.class));
    SENDER.writeAndSend();
    Mockito.verify(TRANSFERS, Mockito.times(1)).upload(Mockito.any(PutObjectRequest.class), Mockito.eq(SENDER));
  }

  @Test
  public void testNonEmptyWavCheckpointedOnTransferComplete() throws Exception {
    final ImbeefConfig     CONFIG    = config();
    final WaveFileWriter   WRITER    = Mockito.mock(WaveFileWriter.class);
    final TransferManager  TRANSFERS = Mockito.mock(TransferManager.class);
    final WaveFileS3Sender SENDER    = new WaveFileS3Sender(CONFIG, WRITER, TRANSFERS);

    final List<Checkpointer> CHECKS = new LinkedList<>();
    final Checkpointer       CHECK  = Mockito.mock(Checkpointer.class);

    CHECKS.add(CHECK);
    SENDER.queue(newChunk(false, CHECKS));

    Mockito.verify(CHECK, Mockito.never()).checkpoint();
    SENDER.progressChanged(new ProgressEvent(ProgressEventType.TRANSFER_COMPLETED_EVENT));
    Mockito.verify(CHECK, Mockito.times(1)).checkpoint();
  }

  @Test
  public void testNonEmptyWavNotCheckpointedOnTransferFailed() throws Exception {
    final ImbeefConfig     CONFIG    = config();
    final WaveFileWriter   WRITER    = Mockito.mock(WaveFileWriter.class);
    final TransferManager  TRANSFERS = Mockito.mock(TransferManager.class);
    final WaveFileS3Sender SENDER    = new WaveFileS3Sender(CONFIG, WRITER, TRANSFERS);

    final List<Checkpointer> CHECKS = new LinkedList<>();
    final Checkpointer       CHECK  = Mockito.mock(Checkpointer.class);

    CHECKS.add(CHECK);
    SENDER.queue(newChunk(false, CHECKS));

    Mockito.verify(CHECK, Mockito.never()).checkpoint();
    SENDER.progressChanged(new ProgressEvent(ProgressEventType.TRANSFER_FAILED_EVENT));
    Mockito.verify(CHECK, Mockito.never()).checkpoint();
  }

}
