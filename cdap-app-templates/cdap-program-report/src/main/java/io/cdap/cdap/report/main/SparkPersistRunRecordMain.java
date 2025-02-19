/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.report.main;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.FileSetProperties;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.api.spark.JavaSparkMain;
import io.cdap.cdap.report.ReportGenerationApp;
import io.cdap.cdap.report.util.Constants;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import javax.crypto.KeyGenerator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * spark main class, starts and waits for the tms subscriber thread to read run record meta and
 * write to files
 */
public class SparkPersistRunRecordMain implements JavaSparkMain {

  private TMSSubscriber tmsSubscriber;
  private static final Logger LOG = LoggerFactory.getLogger(SparkPersistRunRecordMain.class);
  private static final SampledLogging SAMPLED_LOGGING = new SampledLogging(LOG, 100);

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();
    Admin admin = sec.getAdmin();
    if (!admin.datasetExists(ReportGenerationApp.RUN_META_FILESET)) {
      admin.createDataset(ReportGenerationApp.RUN_META_FILESET, FileSet.class.getName(),
          FileSetProperties.builder().build());
    }
    Location reportFileSetLocation = getDatasetBaseLocationWithRetry(sec,
        ReportGenerationApp.REPORT_FILESET);
    createSecurityKeyFile(reportFileSetLocation);
    // enabled by default, configuration to help disable this thread in unit tests
    if (!Boolean.parseBoolean(
        sec.getRuntimeArguments().getOrDefault(Constants.DISABLE_TMS_SUBSCRIBER_THREAD, "false"))) {
      tmsSubscriber = new TMSSubscriber(sec.getMessagingContext().getMessageFetcher(),
          getDatasetBaseLocationWithRetry(sec, ReportGenerationApp.RUN_META_FILESET),
          sec.getRuntimeArguments(), sec.getMetrics());
      tmsSubscriber.start();
      try {
        tmsSubscriber.join();
      } catch (InterruptedException ie) {
        tmsSubscriber.requestStop();
        tmsSubscriber.interrupt();
      }
    }
  }

  /**
   * If the security key file doesn't exist already, security key is generated using AES Algorithm
   * and written to the location identified by KEY_FILE_NAME under ReportFileSet. Permission is
   * configured such that only the owner (cdap) can read this file.
   *
   * @param reportFileSetLocation reporting file set base location
   */
  private void createSecurityKeyFile(Location reportFileSetLocation)
      throws IOException, NoSuchAlgorithmException {
    Location keyLocation = reportFileSetLocation.append(Constants.Security.KEY_FILE_NAME);
    if (!keyLocation.exists()) {
      KeyGenerator keyGenerator = KeyGenerator.getInstance(Constants.Security.ENCRYPTION_ALGORITHM);
      keyGenerator.init(Constants.Security.ENCRYPTION_KEY_BITSIZE);
      Key key = keyGenerator.generateKey();
      byte[] encodedKey = key.getEncoded();
      writeKeyBytes(keyLocation, encodedKey);
    }
  }

  private void writeKeyBytes(Location keyLocation, byte[] encodedKey) throws IOException {
    try (OutputStream outputStream = keyLocation.getOutputStream(
        Constants.Security.KEY_FILE_PERMISSION)) {
      outputStream.write(encodedKey);
    }
  }

  /**
   * Retry on dataset instantiation exception un-till we get the dataset and return location from
   * the fileset.
   *
   * @return Location base location
   */
  private Location getDatasetBaseLocationWithRetry(JavaSparkExecutionContext sec,
      String datasetName) throws InterruptedException {
    while (true) {
      try {
        return Transactionals.execute(sec, context -> {
          FileSet fileSet = context.getDataset(datasetName);
          return fileSet.getBaseLocation();
        });
      } catch (RuntimeException e) {
        // retry on dataset exception
        if (e instanceof DatasetInstantiationException) {
          SAMPLED_LOGGING.logWarning(
              String.format("Exception while trying to get dataset %s", datasetName), e);
        } else {
          throw e;
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
    }
  }
}
