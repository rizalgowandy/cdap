/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AccessControllerInstantiatorTest;
import io.cdap.cdap.security.authorization.AuthorizationContextFactory;
import io.cdap.cdap.security.spi.authorization.AccessControllerSpi;
import io.cdap.cdap.security.spi.authorization.AuditLogContext;
import io.cdap.cdap.security.spi.authorization.AuditLogRequest;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class AuditLogSingleTopicSubscriberServiceTest {

  private static CConfiguration cConf;
  private static TransactionRunner transactionRunner;
  private static List<AuditLogRequest> auditLogRequests;
  private static EmbeddedPostgres pg;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.AuditLogging.AUDIT_LOG_FETCH_SIZE, "3");
    cConf.set(Constants.AuditLogging.AUDIT_LOG_POLL_DELAY_MILLIS, "1");
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
            .in(Scopes.SINGLETON);
        }
      }
    );

    transactionRunner = injector.getInstance(TransactionRunner.class);
    auditLogRequests = new ArrayList<>();
  }

  @AfterClass
  public static void afterClass() {
    Closeables.closeQuietly(pg);
  }


  @Before
  public void beforeTest(){
    auditLogRequests = new ArrayList<>();
  }

  /**
   * Create an iterator of AuditLogRequest and pass it to get published.
   * In the mock publishing, we return UNSUCCESSFUL which should throw exception
   */
  @Test(expected = Exception.class)
  public void testProcessMessages() throws Exception {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);
    AccessControllerInstantiatorMock accessControllerInstantiatorMock =
      new AccessControllerInstantiatorMock(cConf, null);
    AuditLogSingleTopicSubscriberService auditLogSingleTopicSubscriberService =
      new AuditLogSingleTopicSubscriberService(
        cConf,
        mockMsgService,
        Mockito.mock(MetricsCollectionService.class),
        transactionRunner,
        accessControllerInstantiatorMock,
        "topic"
      );
    Queue<AuditLogContext> auditLogContextsOrg = new LinkedList<>();
    auditLogContextsOrg.add(AuditLogContext.Builder.defaultNotRequired());
    auditLogContextsOrg.add(new AuditLogContext.Builder()
                              .setAuditLoggingRequired(true)
                              .setAuditLogBody("Test Audit Logs")
                              .build());
    AuditLogRequest auditLogRequest = new AuditLogRequest(
      200,
      "testuserIp",
      "v3/test",
      "Testhandler",
      "create",
      "POST",
      auditLogContextsOrg,
      1000000L,
      1000002L);

    Iterator<ImmutablePair<String, AuditLogRequest>> messages = null;
      ImmutableList.of(ImmutablePair.of("1",auditLogRequest)).iterator();

    TransactionRunners.run(transactionRunner, (context) -> {
      auditLogSingleTopicSubscriberService.processMessages(
        context, messages);
    }, Exception.class);
  }

  public static void setAuditLogRequests(List<AuditLogRequest> auditLogRequests) {
    AuditLogSingleTopicSubscriberServiceTest.auditLogRequests = auditLogRequests;
  }

  private static class AccessControllerInstantiatorMock extends AccessControllerInstantiator {

    public AccessControllerInstantiatorMock(CConfiguration cConf,
                                            AuthorizationContextFactory authorizationContextFactory) {
      super(cConf, authorizationContextFactory);
    }

    @Override
    public AccessControllerSpi get() {
      return new AccessControllerSpiMock();
    }
  }

  private static class AccessControllerSpiMock extends AccessControllerInstantiatorTest.AccessControllerSpiImp {
    @Override
    public PublishStatus publishAuditLogs(AuditLogRequest auditLogRequest) {
      AuditLogSingleTopicSubscriberServiceTest.setAuditLogRequests(auditLogRequests);
      return PublishStatus.UNSUCCESSFUL;
    }
  }
}
