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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.anhonesteffort.p25.ImbeefConfig;

import java.util.concurrent.ExecutorService;

public class TransferManagerFactory {

  private final ImbeefConfig    config;
  private final ExecutorService executor;

  public TransferManagerFactory(ImbeefConfig config, ExecutorService executor) {
    this.config   = config;
    this.executor = executor;
  }

  private AWSCredentialsProvider credentials() {
    return new StaticCredentialsProvider(
        new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretKey())
    );
  }

  private AmazonS3 amazonS3(AWSCredentialsProvider credentials) {
    return new AmazonS3Client(credentials);
  }

  public TransferManager create() {
    return new TransferManager(amazonS3(credentials()), executor);
  }

}
