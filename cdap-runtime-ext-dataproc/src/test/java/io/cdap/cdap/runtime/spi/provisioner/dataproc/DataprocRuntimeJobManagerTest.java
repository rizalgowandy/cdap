/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocClusterInfo;
import io.cdap.cdap.runtime.spi.runtimejob.DataprocRuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.LaunchMode;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.twill.api.LocalFile;
import org.apache.twill.internal.Constants;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

/** Tests for DataprocRuntimeJobManager. */
public class DataprocRuntimeJobManagerTest {

  private static RuntimeJobInfo runtimeJobInfo;

  @BeforeClass
  public static void setUp() {
    runtimeJobInfo =
        new RuntimeJobInfo() {
          private final ProgramRunInfo runInfo =
              new ProgramRunInfo.Builder()
                  .setNamespace("namespace")
                  .setApplication("application")
                  .setVersion("1.0")
                  .setProgramType("workflow")
                  .setProgram("program")
                  .setRun(UUID.randomUUID().toString())
                  .build();

          @Override
          public Collection<? extends LocalFile> getLocalizeFiles() {
            return Collections.emptyList();
          }

          @Override
          public String getRuntimeJobClassname() {
            return DataprocRuntimeJobManager.getJobId(runInfo);
          }

          @Override
          public ProgramRunInfo getProgramRunInfo() {
            return runInfo;
          }

          @Override
          public Map<String, String> getJvmProperties() {
            return ImmutableMap.of("key", "val");
          }
        };
  }

  @Test
  public void jobNameTest() {
    ProgramRunInfo runInfo =
        new ProgramRunInfo.Builder()
            .setNamespace("namespace")
            .setApplication("application")
            .setVersion("1.0")
            .setProgramType("workflow")
            .setProgram("program")
            .setRun(UUID.randomUUID().toString())
            .build();
    String jobName = DataprocRuntimeJobManager.getJobId(runInfo);
    Assert.assertTrue(jobName.startsWith("namespace_application_program"));
  }

  @Test
  public void longJobNameTest() {
    ProgramRunInfo runInfo =
        new ProgramRunInfo.Builder()
            .setNamespace("namespace")
            .setApplication(
                "very_very_long_app_name_is_provided_this_should_be"
                    + "_trimed_so_that_correct_name_is_produced")
            .setVersion("1.0")
            .setProgramType("workflow")
            .setProgram("program")
            .setRun(UUID.randomUUID().toString())
            .build();
    String jobName = DataprocRuntimeJobManager.getJobId(runInfo);
    Assert.assertTrue(
        jobName.startsWith("namespace_very_very_long_app_name_is_provided_this_should_be_tr_"));
    Assert.assertEquals(100, jobName.length());
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidJobNameTest() {
    ProgramRunInfo runInfo =
        new ProgramRunInfo.Builder()
            .setNamespace("namespace")
            .setApplication("application$$$")
            .setVersion("1.0")
            .setProgramType("workflow")
            .setProgram("program")
            .setRun(UUID.randomUUID().toString())
            .build();
    String jobName = DataprocRuntimeJobManager.getJobId(runInfo);
    Assert.assertTrue(jobName.startsWith("namespace_application_program"));
  }

  @Test
  public void getArgumentsTest() {
    List<String> arguments =
        DataprocRuntimeJobManager.getArguments(
            runtimeJobInfo,
            Collections.emptyList(),
            SparkCompat.SPARK3_2_12.getCompat(),
            Constants.Files.APPLICATION_JAR,
            LaunchMode.CLIENT);
    Assert.assertEquals(5, arguments.size());
    Assert.assertTrue(arguments.contains("--propkey=\"val\""));
    Assert.assertTrue(
        arguments.contains("--runtimeJobClass=" + runtimeJobInfo.getRuntimeJobClassname()));
    Assert.assertTrue(arguments.contains("--sparkCompat=" + SparkCompat.SPARK3_2_12.getCompat()));
    Assert.assertTrue(
        arguments.contains(
            "--" + Constants.Files.APPLICATION_JAR + "=" + Constants.Files.APPLICATION_JAR));
    Assert.assertTrue(arguments.contains("--launchMode=CLIENT"));
  }

  @Test
  public void getPropertiesTest() {
    Map<String, String> properties = DataprocRuntimeJobManager.getProperties(runtimeJobInfo);
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    Assert.assertEquals(6, properties.size());
    Assert.assertEquals(
        runInfo.getNamespace(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_NAMESPACE));
    properties.put(
        runInfo.getApplication(),
        properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_APPLICATION));
    Assert.assertEquals(
        runInfo.getVersion(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_VERSION));
    Assert.assertEquals(
        runInfo.getProgramType(),
        properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_PROGRAM_TYPE));
    Assert.assertEquals(
        runInfo.getProgram(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_PROGRAM));
    Assert.assertEquals(
        runInfo.getRun(), properties.get(DataprocRuntimeJobManager.CDAP_RUNTIME_RUNID));
  }

  @Test
  public void uploadFileTest() throws Exception {
    final String bucketName = "bucket";
    GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
    Mockito.doReturn(true).when(credentials).createScopedRequired();
    DataprocRuntimeJobManager dataprocRuntimeJobManager = new DataprocRuntimeJobManager(
      new DataprocClusterInfo(new MockProvisionerContext(), "test-cluster", credentials,
          null, "test-project", "test-region", bucketName, Collections.emptyMap()),
        Collections.emptyMap(), null);

    DataprocRuntimeJobManager mockedDataprocRuntimeJobManager =
        Mockito.spy(dataprocRuntimeJobManager);

    Storage storage = Mockito.mock(Storage.class);
    Mockito.doReturn(storage).when(mockedDataprocRuntimeJobManager).getStorageClient();

    Bucket bucket = Mockito.mock(Bucket.class);
    Mockito.doReturn(bucket).when(storage).get(Matchers.eq(bucketName));
    Mockito.doReturn("regional").when(bucket).getLocationType();
    Mockito.doReturn("test-region").when(bucket).getLocation();

    String targetFilePath = "cdap-job/target";
    BlobId blobId = BlobId.of(bucketName, targetFilePath);
    BlobId newBlobId = BlobId.of(bucketName, targetFilePath, 1L);
    Blob blob = Mockito.mock(Blob.class);
    Mockito.doReturn(blob).when(storage).get(blobId);
    Mockito.doReturn(newBlobId).when(blob).getBlobId();

    BlobInfo expectedBlobInfo =
        BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
    Mockito.doThrow(
        new StorageException(HttpURLConnection.HTTP_PRECON_FAILED, "blob already exists"))
        .when(mockedDataprocRuntimeJobManager).uploadToGcsUtil(Mockito.any(),
            Mockito.any(), Mockito.any(), Matchers.eq(expectedBlobInfo),
            Matchers.eq(Storage.BlobWriteOption.doesNotExist()));

    expectedBlobInfo =
        BlobInfo.newBuilder(newBlobId).setContentType("application/octet-stream").build();
    Mockito.doNothing().when(mockedDataprocRuntimeJobManager).uploadToGcsUtil(Mockito.any(),
        Mockito.any(), Mockito.any(), Matchers.eq(expectedBlobInfo));

    // call the method
    LocalFile localFile = Mockito.mock(LocalFile.class);
    mockedDataprocRuntimeJobManager.uploadFile(bucketName, targetFilePath, localFile, false);
  }
}
