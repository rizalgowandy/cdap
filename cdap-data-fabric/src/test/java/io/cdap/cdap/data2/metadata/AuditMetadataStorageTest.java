/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.audit.AuditTestModule;
import io.cdap.cdap.data2.audit.InMemoryAuditPublisher;
import io.cdap.cdap.proto.audit.AuditMessage;
import io.cdap.cdap.proto.audit.AuditType;
import io.cdap.cdap.proto.audit.payload.metadata.MetadataPayload;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.MetadataStorageTest;
import io.cdap.cdap.spi.metadata.MutationOptions;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import io.cdap.cdap.spi.metadata.dataset.DatasetMetadataStorageTest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuditMetadataStorageTest extends MetadataStorageTest {

  private static final Map<String, String> EMPTY_PROPERTIES = Collections.emptyMap();
  private static final Set<String> EMPTY_TAGS = Collections.emptySet();
  private static final Map<MetadataScope, Metadata> EMPTY_USER_METADATA =
    ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, EMPTY_TAGS));

  private final ApplicationId app = NamespaceId.DEFAULT.app("app");
  private final ProgramId service = app.service("service");
  private final DatasetId dataset = NamespaceId.DEFAULT.dataset("ds");
  private final Set<String> datasetTags = ImmutableSet.of("dTag");
  private final Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
  private final Set<String> appTags = ImmutableSet.of("aTag");
  private final Set<String> tags = ImmutableSet.of("fTag");

  private final AuditMessage auditMessage1 = new AuditMessage(
    0, dataset, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA, ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage2 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA, ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage3 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, appTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage7 = new AuditMessage(
    0, service, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, tags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage8 = new AuditMessage(
    0, service, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, tags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, tags))
    )
  );
  private final AuditMessage auditMessage9 = new AuditMessage(
    0, dataset, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags))
    )
  );
  private final AuditMessage auditMessage11 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, appTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, appTags))
    )
  );
  private final List<AuditMessage> expectedAuditMessages = ImmutableList.of(
    auditMessage1, auditMessage2, auditMessage3, auditMessage7,
    auditMessage8, auditMessage9, auditMessage11
  );

  private static CConfiguration cConf;
  private static MetadataStorage storage;
  private static InMemoryAuditPublisher auditPublisher;

  @BeforeClass
  public static void setup() throws IOException {
    Injector injector = DatasetMetadataStorageTest.doSetup(new AuditTestModule());
    cConf = injector.getInstance(CConfiguration.class);
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
    storage = new AuditMetadataStorage(DatasetMetadataStorageTest.storage,
                                       injector.getInstance(MetricsCollectionService.class));
    ((AuditMetadataStorage) storage).setAuditPublisher(auditPublisher);
  }

  @AfterClass
  public static void teardown() throws IOException {
    DatasetMetadataStorageTest.teardown();
  }

  @Override
  protected MetadataStorage getMetadataStorage() {
    return storage;
  }

  @Override
  protected void validateCursor(String cursor, int expectedOffset, int expectedPageSize) {
    // no-op - this is tested in DatasetMetadataStorageTest
  }

  @Test
  public void testPublishing() throws IOException {
    generateMetadataUpdates();

    // Audit messages for metadata changes
    List<AuditMessage> actualAuditMessages = new ArrayList<>();
    String systemNs = NamespaceId.SYSTEM.getNamespace();
    for (AuditMessage auditMessage : auditPublisher.popMessages()) {
      // Ignore system audit messages
      if (auditMessage.getType() == AuditType.METADATA_CHANGE) {
        if (!systemNs.equalsIgnoreCase(auditMessage.getEntity().getValue(MetadataEntity.NAMESPACE))) {
          actualAuditMessages.add(auditMessage);
        }
      }
    }
    Assert.assertEquals(expectedAuditMessages, actualAuditMessages);
  }

  @Test
  public void testPublishingDisabled() throws IOException {
    boolean auditEnabled = cConf.getBoolean(Constants.Audit.ENABLED);
    cConf.setBoolean(Constants.Audit.ENABLED, false);
    generateMetadataUpdates();

    try {
      List<AuditMessage> publishedAuditMessages = auditPublisher.popMessages();
      Assert.fail(String.format("Expected no changes to be published, but found %d changes: %s.",
                                publishedAuditMessages.size(), publishedAuditMessages));
    } catch (AssertionError e) {
      // expected
    }
    // reset config
    cConf.setBoolean(Constants.Audit.ENABLED, auditEnabled);
  }

  private void generateMetadataUpdates() throws IOException {
    storage.apply(
      new MetadataMutation.Update(
        dataset.toMetadataEntity(),
        new io.cdap.cdap.spi.metadata.Metadata(MetadataScope.USER, datasetTags)), MutationOptions.DEFAULT);
    storage.apply(
      new MetadataMutation.Update(
        app.toMetadataEntity(),
        new io.cdap.cdap.spi.metadata.Metadata(MetadataScope.USER, appProperties)), MutationOptions.DEFAULT);
    storage.apply(
      new MetadataMutation.Update(
        app.toMetadataEntity(),
        new io.cdap.cdap.spi.metadata.Metadata(MetadataScope.USER, appTags)), MutationOptions.DEFAULT);
    storage.apply(
      new MetadataMutation.Update(
        service.toMetadataEntity(),
        new io.cdap.cdap.spi.metadata.Metadata(MetadataScope.USER, tags)), MutationOptions.DEFAULT);
    storage.apply(
      new MetadataMutation.Remove(
        service.toMetadataEntity(),
        MetadataScope.USER,
        MetadataKind.TAG), MutationOptions.DEFAULT);
    storage.apply(
      new MetadataMutation.Remove(
        dataset.toMetadataEntity(),
        datasetTags.stream()
          .map(tag -> new ScopedNameOfKind(MetadataKind.TAG, MetadataScope.USER, tag))
          .collect(Collectors.toSet())), MutationOptions.DEFAULT);
    storage.apply(
      new MetadataMutation.Remove(
        app.toMetadataEntity(),
        MetadataScope.USER), MutationOptions.DEFAULT);
  }
}
