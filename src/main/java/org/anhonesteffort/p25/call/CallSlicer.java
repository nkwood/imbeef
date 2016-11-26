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

import io.radiowitness.proto.p25.ProtoP25Factory;
import org.anhonesteffort.dsp.Sink;
import org.anhonesteffort.dsp.Source;
import org.anhonesteffort.p25.CallPipeline;
import org.anhonesteffort.p25.ImbeefConfig;
import org.anhonesteffort.p25.CheckpointingAudioChunk;
import org.anhonesteffort.p25.CheckpointedDataUnit;
import org.anhonesteffort.p25.ImbeefMetrics;
import org.anhonesteffort.p25.protocol.Duid;
import org.anhonesteffort.p25.protocol.frame.HeaderDataUnit;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit1;
import org.anhonesteffort.p25.protocol.frame.LogicalLinkDataUnit2;
import org.anhonesteffort.p25.protocol.frame.VoiceFrame;
import org.anhonesteffort.p25.protocol.frame.linkcontrol.GroupVoiceUserLwc;
import org.anhonesteffort.p25.protocol.frame.linkcontrol.LinkControlWord;
import org.anhonesteffort.p25.protocol.frame.linkcontrol.UnitToUnitVoiceUserLwc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.FloatBuffer;

import static io.radiowitness.proto.p25.ProtoP25.P25ChannelId;

public class CallSlicer
    extends Source<CheckpointingAudioChunk, Sink<CheckpointingAudioChunk>>
        implements CallPipeline
{

  private static final Logger log = LoggerFactory.getLogger(CallSlicer.class);

  private final ProtoP25Factory proto   = new ProtoP25Factory();
  private final Object          txnLock = new Object();
  private final CallState       state   = new CallState();

  private final ImbeefConfig       config;
  private final P25ChannelId       channelId;
  private final SafeAudioConverter converter;

  private FloatBuffer queue;
  private long callInactiveTime = Long.MIN_VALUE;

  public CallSlicer(ImbeefConfig config, P25ChannelId channelId, SafeAudioConverter converter) {
    this.config    = config;
    this.channelId = channelId;
    this.converter = converter;
    queue          = FloatBuffer.allocate(config.getMaxAudioChunkSize());
  }

  private void queueOrBroadcast(LogicalLinkDataUnit lldu) {
    for (VoiceFrame voiceFrame : lldu.getVoiceFrames()) {
      float[] audio = converter.decode(voiceFrame.getBytes());
      if (queue.remaining() >= audio.length) {
        queue.put(audio);
      } else {
        queue.flip();
        broadcast(new CheckpointingAudioChunk(
            channelId, state.isFirst(), false, false, state.getEarliestRemoteTime(),
            state.getLatestRemoteTime(), state.getLatitude(), state.getLongitude(),
            state.getSourceId(), queue, state.getCheckpoints()
        ));

        queue = FloatBuffer.allocate(config.getMaxAudioChunkSize());
        state.nextChunk();
        queue.put(audio);
      }
    }
  }

  private void handlePrepareNextCall(boolean terminated) {
    queue.flip();
    broadcast(new CheckpointingAudioChunk(
        channelId, state.isFirst(), true, terminated, state.getEarliestRemoteTime(),
        state.getLatestRemoteTime(), state.getLatitude(), state.getLongitude(),
        state.getSourceId(), queue, state.getCheckpoints()
    ));

    queue = FloatBuffer.allocate(config.getMaxAudioChunkSize());
    state.nextCall();
  }

  private void handleCheckTerminateTimeout(CheckpointedDataUnit dataUnit) {
    long remoteTimeDiff = dataUnit.getTimestamp() - state.getLatestRemoteTime();
    if (state.getLatestRemoteTime() > 0 && remoteTimeDiff > config.getTerminatorTimeoutMs()) {
      ImbeefMetrics.getInstance().terminateTimeout();
      log.warn(proto.toString(channelId) + " timed out waiting for terminating data unit");
      handlePrepareNextCall(false);
    }
    state.setLatestRemoteTimeIfMore(dataUnit.getTimestamp());
  }

  @Override
  public void consume(CheckpointedDataUnit dataUnit) {
    synchronized (txnLock) {
      callInactiveTime = System.currentTimeMillis() + (long) (1000d / config.getMinCallDataUnitRate());
      handleCheckTerminateTimeout(dataUnit);

      state.coordinates(dataUnit.getLatitude(), dataUnit.getLongitude());
      state.addCheckpoint(dataUnit.getCheckpointer());

      switch (dataUnit.getDataUnit().getNid().getDuid().getId()) {
        case Duid.ID_HEADER:
          ImbeefMetrics.getInstance().headerDataUnit();
          handlePrepareNextCall(false);
          state.setIsEncrypted(((HeaderDataUnit) dataUnit.getDataUnit()).getAlgorithmId() != 0x80);
          state.setEarliestRemoteTimeIfLess(dataUnit.getTimestamp());
          break;

        case Duid.ID_LLDU2:
          LogicalLinkDataUnit2 lldu2 = (LogicalLinkDataUnit2) dataUnit.getDataUnit();
          state.setIsEncrypted(lldu2.getAlgorithmId() != 0x80);
          state.setEarliestRemoteTimeIfLess(dataUnit.getTimestamp());

          if (!state.isEncrypted()) {
            ImbeefMetrics.getInstance().unencryptedVoice();
            queueOrBroadcast(lldu2);
          } else {
            ImbeefMetrics.getInstance().encryptedVoice();
          }
          break;

        case Duid.ID_LLDU1:
          LogicalLinkDataUnit1 lldu1 = (LogicalLinkDataUnit1) dataUnit.getDataUnit();
          LinkControlWord      word  = lldu1.getLinkControlWord();
          state.setEarliestRemoteTimeIfLess(dataUnit.getTimestamp());

          switch (word.getLinkControlOpcode()) {
            case LinkControlWord.LCF_GROUP:
              state.setSourceId(((GroupVoiceUserLwc) word).getSourceId());
              break;

            case LinkControlWord.LCF_UNIT:
              state.setSourceId(((UnitToUnitVoiceUserLwc) word).getSourceId());
              break;
          }

          if (!state.isEncrypted()) {
            ImbeefMetrics.getInstance().unencryptedVoice();
            queueOrBroadcast(lldu1);
          } else {
            ImbeefMetrics.getInstance().encryptedVoice();
          }
          break;

        case Duid.ID_TERMINATOR_WO_LINK:
          ImbeefMetrics.getInstance().terminatorDataUnit();
          state.setEarliestRemoteTimeIfLess(dataUnit.getTimestamp());
          handlePrepareNextCall(true);
          break;
      }
    }
  }

  @Override
  public boolean isInactive(long localTime) {
    synchronized (txnLock) {
      if (localTime > callInactiveTime) {
        handlePrepareNextCall(false);
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public P25ChannelId getChannelId() {
    return channelId;
  }

}
