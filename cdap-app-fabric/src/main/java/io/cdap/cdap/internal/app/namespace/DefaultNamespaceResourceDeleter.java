/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.namespace;

import com.google.inject.Inject;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricsSystemClient;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.SourceControlManagementService;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.impersonation.Impersonator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract class for deleting namespace components
 */
public class DefaultNamespaceResourceDeleter implements NamespaceResourceDeleter {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceResourceDeleter.class);

  private final Impersonator impersonator;
  private final Store store;
  private final PreferencesService preferencesService;
  private final DatasetFramework dsFramework;
  private final MetricsSystemClient metricsSystemClient;
  private final ApplicationLifecycleService applicationLifecycleService;
  private final SourceControlManagementService sourceControlService;
  private final ArtifactRepository artifactRepository;
  private final StorageProviderNamespaceAdmin storageProviderNamespaceAdmin;
  private final MessagingService messagingService;
  private final ProfileService profileService;

  @Inject
  DefaultNamespaceResourceDeleter(Impersonator impersonator, Store store,
      PreferencesService preferencesService,
      DatasetFramework dsFramework,
      MetricsSystemClient metricsSystemClient,
      ApplicationLifecycleService applicationLifecycleService,
      SourceControlManagementService sourceControlService,
      ArtifactRepository artifactRepository,
      StorageProviderNamespaceAdmin storageProviderNamespaceAdmin,
      MessagingService messagingService, ProfileService profileService) {
    this.impersonator = impersonator;
    this.store = store;
    this.preferencesService = preferencesService;
    this.dsFramework = dsFramework;
    this.metricsSystemClient = metricsSystemClient;
    this.applicationLifecycleService = applicationLifecycleService;
    this.sourceControlService = sourceControlService;
    this.artifactRepository = artifactRepository;
    this.storageProviderNamespaceAdmin = storageProviderNamespaceAdmin;
    this.messagingService = messagingService;
    this.profileService = profileService;
  }

  @Override
  public void deleteResources(NamespaceMeta namespaceMeta) throws Exception {
    final NamespaceId namespaceId = namespaceMeta.getNamespaceId();

    // Delete Preferences associated with this namespace
    preferencesService.deleteProperties(namespaceId);
    // Delete Source Control repository configuration
    sourceControlService.deleteRepository(namespaceId);
    // Delete all applications
    applicationLifecycleService.removeAll(namespaceId);
    // Delete datasets and modules
    dsFramework.deleteAllInstances(namespaceId);
    dsFramework.deleteAllModules(namespaceId);

    // Delete all meta data
    store.removeAll(namespaceId);

    deleteMetrics(namespaceId);
    // delete all artifacts in the namespace
    artifactRepository.clear(namespaceId);

    // delete all profiles in the namespace
    profileService.deleteAllProfiles(namespaceId);

    // delete all messaging topics in the namespace
    for (TopicId topicId : messagingService.listTopics(namespaceId)) {
      messagingService.deleteTopic(topicId);
    }

    LOG.info("All data for namespace '{}' deleted.", namespaceId);

    // Delete the namespace itself, only if it is a non-default namespace. This is because we do not allow users to
    // create default namespace, and hence deleting it may cause undeterministic behavior.
    // Another reason for not deleting the default namespace is that we do not want to call a delete on the default
    // namespace in the storage provider (Hive, HBase, etc), since we re-use their default namespace.
    if (!NamespaceId.DEFAULT.equals(namespaceId)) {
      impersonator.doAs(namespaceId, (Callable<Void>) () -> {
        // Delete namespace in storage providers
        storageProviderNamespaceAdmin.delete(namespaceId);
        return null;
      });
    }
  }

  private void deleteMetrics(NamespaceId namespaceId) throws IOException {
    long endTs = System.currentTimeMillis() / 1000;
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(Constants.Metrics.Tag.NAMESPACE, namespaceId.getNamespace());
    MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.emptySet(), tags,
        new ArrayList<>(tags.keySet()));
    metricsSystemClient.delete(deleteQuery);
  }
}
