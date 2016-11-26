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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.radiowitness.kinesis.consumer.KclConfigFactory;
import org.anhonesteffort.p25.audio.ImbeConverterFactory;
import org.anhonesteffort.p25.call.CallManager;
import org.anhonesteffort.p25.consumer.KinesisP25ConsumerFactory;
import org.anhonesteffort.p25.wav.TransferManagerFactory;
import org.anhonesteffort.p25.wav.WaveHeaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.edu.icm.jlargearrays.ConcurrencyUtils;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Imbeef {

  private static final Logger log = LoggerFactory.getLogger(Imbeef.class);
  private final ImbeefConfig config;

  public Imbeef(ImbeefConfig config) {
    this.config = config;
  }

  public void run() {
    ImbeefMetrics.init(config, new MetricRegistry());

    ImbeConverterFactory converters = new ImbeConverterFactory();
    WaveHeaderFactory    headers    = new WaveHeaderFactory();

    ExecutorService        s3Pool    = Executors.newFixedThreadPool(config.getS3PoolSize());
    TransferManagerFactory transfers = new TransferManagerFactory(config, s3Pool);

    CallPipelineFactory       pipelines   = new CallPipelineFactory(config, converters, headers, transfers.create());
    CallManager               callManager = new CallManager(config, pipelines);
    KinesisP25ConsumerFactory consumers   = new KinesisP25ConsumerFactory(callManager);

    Worker worker = new Worker.Builder().recordProcessorFactory(consumers)
                              .config(new KclConfigFactory(config).create())
                              .build();

    Futures.addCallback(
        consumers.getErrorFuture(),
        new KclErrorCallback(worker, s3Pool)
    );

    worker.run();
  }

  private static class KclErrorCallback implements FutureCallback<Void> {
    private final Worker worker;
    private final ExecutorService s3Pool;

    public KclErrorCallback(Worker worker, ExecutorService s3Pool) {
      this.worker = worker;
      this.s3Pool = s3Pool;
    }

    @Override
    public void onSuccess(Void aVoid) {
      log.error("exiting early with unknown error");
      s3Pool.shutdownNow();
      worker.shutdown();
      ConcurrencyUtils.shutdownThreadPoolAndAwaitTermination();
      System.exit(1);
    }

    @Override
    public void onFailure(Throwable throwable) {
      log.error("exiting early due to kcl error", throwable);
      s3Pool.shutdownNow();
      worker.shutdown();
      ConcurrencyUtils.shutdownThreadPoolAndAwaitTermination();
      System.exit(1);
    }
  }

  public static void main(String[] args) throws IOException {
    new Imbeef(new ImbeefConfig()).run();
  }

}