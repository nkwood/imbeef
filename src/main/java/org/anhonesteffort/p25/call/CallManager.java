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

import com.codahale.metrics.Gauge;
import org.anhonesteffort.kinesis.proto.ProtoP25Factory;
import org.anhonesteffort.p25.CallPipeline;
import org.anhonesteffort.p25.CallPipelineFactory;
import org.anhonesteffort.p25.ImbeefConfig;
import org.anhonesteffort.p25.CheckpointedDataUnit;
import org.anhonesteffort.p25.ImbeefMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.anhonesteffort.kinesis.proto.ProtoP25.P25ChannelId;

public class CallManager extends TimerTask {

  private static final Logger log = LoggerFactory.getLogger(CallManager.class);
  private final ProtoP25Factory proto = new ProtoP25Factory();

  private final Map<String, CallPipeline> pipelines = new ConcurrentHashMap<>();
  private final Object txnLock = new Object();
  private final CallPipelineFactory factory;

  public CallManager(ImbeefConfig config, CallPipelineFactory factory) {
    this.factory = factory;

    new Timer(true).scheduleAtFixedRate(
        this, 0, (long) (1000 / config.getCallInactiveCheckRate())
    );
    ImbeefMetrics.getInstance().registerCallManager(new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return pipelines.size();
      }
    });
  }

  private String key(P25ChannelId.Reader channelId) {
    return proto.toString(channelId);
  }

  public void process(P25ChannelId.Reader channelId, CheckpointedDataUnit dataUnit) {
    String key = key(channelId);
    synchronized (txnLock) {

      if (!pipelines.containsKey(key)) {
        log.info(key + " creating new call pipeline");
        pipelines.put(key, factory.create(channelId));
      }
      pipelines.get(key).consume(dataUnit);

    }
  }

  @Override
  public void run() {
    synchronized (txnLock) {
      List<CallPipeline> inactive =
          pipelines.keySet()
                   .stream()
                   .map(pipelines::get)
                   .filter(pipeline -> pipeline.isInactive(System.currentTimeMillis()))
                   .collect(Collectors.toList());

      inactive.forEach(pipeline -> {
        String key = key(pipeline.getChannelId());
        ImbeefMetrics.getInstance().inactivePipeline();
        log.info(key + " removing call pipeline for inactivity");
        pipelines.remove(key);
      });
    }
  }

}
