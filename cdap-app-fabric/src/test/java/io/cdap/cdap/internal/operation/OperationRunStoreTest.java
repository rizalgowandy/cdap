/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationMeta;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class OperationRunStoreTest extends OperationTestBase {

  protected static TransactionRunner transactionRunner;
  private final AtomicInteger sourceId = new AtomicInteger();
  private final AtomicLong runIdTime = new AtomicLong(System.currentTimeMillis());

  @Before
  public void before() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore operationRunsStore = new OperationRunStore(context);
      operationRunsStore.clearData();
    });
  }

  @Test
  public void testGetOperation() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, OperationType.PUSH_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);
      try {
        store.getOperation(new OperationRunId(Namespace.DEFAULT.getId(), testId));
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testUpdateResources() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, OperationType.PUSH_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationMeta updatedMeta = OperationMeta.builder(expectedDetail.getRun().getMetadata())
          .setResources(ImmutableSet.of(new OperationResource("test"))).build();
      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setMetadata(updatedMeta).build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.updateOperationResources(runId, updatedMeta.getResources(),
          updatedDetail.getSourceId());
      gotDetail = store.getOperation(runId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.updateOperationResources(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            updatedMeta.getResources(),
            updatedDetail.getSourceId());
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testUpdateStatus() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, OperationType.PUSH_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      OperationRunDetail gotDetail = store.getOperation(runId);
      Assert.assertEquals(expectedDetail, gotDetail);

      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.STOPPING)
          .build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.updateOperationStatus(runId, updatedRun.getStatus(),
          updatedDetail.getSourceId());
      gotDetail = (OperationRunDetail) store.getOperation(runId);
      Assert.assertEquals(updatedDetail, gotDetail);

      try {
        store.updateOperationStatus(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            updatedRun.getStatus(),
            updatedDetail.getSourceId())
        ;
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testFailOperation() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, OperationType.PUSH_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      Assert.assertEquals(expectedDetail, store.getOperation(runId));

      OperationError error = new OperationError("operation failed", Collections.emptyList());
      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.FAILED)
          .setError(error)
          .setMetadata(OperationMeta.builder(expectedDetail.getRun().getMetadata())
              .setEndTime(Instant.ofEpochMilli(runIdTime.incrementAndGet()))
              .build())
          .build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.failOperationRun(runId, error, updatedRun.getMetadata().getEndTime(),
          updatedDetail.getSourceId());
      Assert.assertEquals(updatedDetail, store.getOperation(runId));

      try {
        store.failOperationRun(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            error,
            Instant.now(), // no need to verify this
            updatedDetail.getSourceId()
        );
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testKillOperation() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, OperationType.PUSH_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      Assert.assertEquals(expectedDetail, store.getOperation(runId));

      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.KILLED)
          .setMetadata(OperationMeta.builder(expectedDetail.getRun().getMetadata())
              .setEndTime(Instant.ofEpochMilli(runIdTime.incrementAndGet()))
              .build())
          .build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.killOperationRun(runId, updatedRun.getMetadata().getEndTime(),
          updatedDetail.getSourceId());
      Assert.assertEquals(updatedDetail, store.getOperation(runId));

      try {
        store.killOperationRun(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            Instant.now(), // no need to verify this
            updatedDetail.getSourceId()
        );
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }

  @Test
  public void testSucceedOperation() throws Exception {
    OperationRunDetail expectedDetail = insertRun(testNamespace, OperationType.PUSH_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    String testId = expectedDetail.getRun().getId();
    OperationRunId runId = new OperationRunId(testNamespace, testId);

    TransactionRunners.run(transactionRunner, context -> {
      OperationRunStore store = new OperationRunStore(context);
      Assert.assertEquals(expectedDetail, store.getOperation(runId));

      OperationRun updatedRun = OperationRun.builder(expectedDetail.getRun())
          .setStatus(OperationRunStatus.SUCCEEDED)
          .setMetadata(OperationMeta.builder(expectedDetail.getRun().getMetadata())
              .setEndTime(Instant.ofEpochMilli(runIdTime.incrementAndGet()))
              .build())
          .build();
      OperationRunDetail updatedDetail = OperationRunDetail.builder(expectedDetail)
          .setRun(updatedRun)
          .setSourceId(AppFabricTestHelper.createSourceId(sourceId.incrementAndGet()))
          .build();
      store.succeedOperationRun(runId, updatedRun.getMetadata().getEndTime(),
          updatedDetail.getSourceId());
      Assert.assertEquals(updatedDetail, store.getOperation(runId));

      try {
        store.succeedOperationRun(
            new OperationRunId(Namespace.DEFAULT.getId(), testId),
            Instant.now(), // no need to verify this
            updatedDetail.getSourceId()
        );
        Assert.fail("Found unexpected run in default namespace");
      } catch (OperationRunNotFoundException e) {
        // expected
      }
    }, Exception.class);
  }


  @Test
  public void testScanOperation() throws Exception {
    List<OperationRunDetail> insertedRuns = insertTestRuns(transactionRunner);
    // get a filtered list of testNamespace runs
    List<OperationRunDetail> testNamespaceRuns = insertedRuns.stream()
        .filter(detail -> detail.getRunId().getNamespace().equals(testNamespace))
        .collect(Collectors.toList());

    TransactionRunners.run(transactionRunner, context -> {
      List<OperationRunDetail> gotRuns = new ArrayList<>();
      List<OperationRunDetail> expectedRuns;
      ScanOperationRunsRequest request;

      OperationRunStore store = new OperationRunStore(context);

      // verify the scan without filters picks all runs for testNamespace
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns;
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      // verify limit
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace).setLimit(2).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns.stream().limit(2).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      // verify the scan with type filter
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace)
          .setFilter(new OperationRunFilter(OperationType.PUSH_APPS, null)).build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns.stream()
          .filter(detail -> detail.getRun().getType().equals(OperationType.PUSH_APPS))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());

      // verify the scan with status filter
      gotRuns.clear();
      request = ScanOperationRunsRequest.builder()
          .setNamespace(testNamespace)
          .setFilter(new OperationRunFilter(OperationType.PULL_APPS, OperationRunStatus.FAILED))
          .build();
      store.scanOperations(request, gotRuns::add);
      expectedRuns = testNamespaceRuns.stream()
          .filter(detail -> detail.getRun().getType().equals(OperationType.PULL_APPS))
          .filter(detail -> detail.getRun().getStatus().equals(OperationRunStatus.FAILED))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRuns.toArray(), gotRuns.toArray());
    }, Exception.class);
  }

  @Test
  public void testScanOperationByStatus() throws Exception {
    TransactionRunners.run(transactionRunner, context -> {
      Set<OperationRunDetail> expectedRuns = insertTestRuns(transactionRunner).stream().filter(
          d -> d.getRun().getStatus().equals(OperationRunStatus.STARTING)
      ).collect(Collectors.toSet());
      Set<OperationRunDetail> gotRuns = new HashSet<>();
      OperationRunStore store = new OperationRunStore(context);
      store.scanOperationByStatus(OperationRunStatus.STARTING, gotRuns::add);
      Assert.assertEquals(expectedRuns.size(), gotRuns.size());
      Assert.assertTrue(expectedRuns.containsAll(gotRuns));
    }, InvalidFieldException.class, IOException.class);
  }
}
