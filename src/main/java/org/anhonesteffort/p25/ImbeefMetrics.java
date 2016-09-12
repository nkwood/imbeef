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

import com.blacklocus.metrics.CloudWatchReporterBuilder;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.TimeUnit;

public class ImbeefMetrics {

  private static ImbeefMetrics instance;
  private final MetricRegistry registry;

  protected ImbeefMetrics(ImbeefConfig config, MetricRegistry registry) {
    this.registry = registry;

    System.getProperties().setProperty("aws.accessKeyId", config.getAccessKeyId());
    System.getProperties().setProperty("aws.secretKey",   config.getSecretKey());

    new CloudWatchReporterBuilder()
        .withNamespace(ImbeefMetrics.class.getSimpleName())
        .withRegistry(registry)
        .build()
        .start(1, TimeUnit.MINUTES);
  }

  protected static void mock(ImbeefMetrics mock) {
    if (instance == null) {
      instance = mock;
    }
  }

  public static void init(ImbeefConfig config, MetricRegistry registry) {
    if (instance == null) {
      instance = new ImbeefMetrics(config, registry);
    }
  }

  public static ImbeefMetrics getInstance() {
    return instance;
  }

  public void createConsumer() {
    registry.counter("createConsumer").inc();
  }

  public void createPipeline() {
    registry.counter("createPipeline").inc();
  }

  public void inactivePipeline() {
    registry.counter("inactivePipeline").inc();
  }

  public void terminateTimeout() {
    registry.counter("terminateTimeout").inc();
  }

  public void headerDataUnit() {
    registry.counter("headerDataUnit").inc();
  }

  public void terminatorDataUnit() {
    registry.counter("terminatorDataUnit").inc();
  }

  public void unencryptedVoice() {
    registry.counter("unencryptedVoice").inc();
  }

  public void encryptedVoice() {
    registry.counter("encryptedVoice").inc();
  }

  public void wavQueued() {
    registry.counter("wavQueued").inc();
  }

  public void wavPutSuccess() {
    registry.counter("wavPutSuccess").inc();
  }

  public void wavPutFailure() {
    registry.counter("wavPutFailure").inc();
  }

  public void wavSize(int bytes) {
    registry.histogram("wavSizeBytes").update(bytes);
  }

  public void registerCallManager(Gauge<Integer> gauge) {
    registry.register("callManager", gauge);
  }

}
