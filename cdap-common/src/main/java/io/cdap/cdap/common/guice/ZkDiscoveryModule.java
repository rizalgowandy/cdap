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

package io.cdap.cdap.common.guice;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.common.twill.TwillAppNames;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.discovery.ZKDiscoveryService;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Guice module for providing bindings for {@link DiscoveryService} and {@link
 * DiscoveryServiceClient} that uses ZooKeeper as the service discovery mechanism.
 */
public final class ZkDiscoveryModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(ZKDiscoveryService.class).toProvider(ZkDiscoveryServiceProvider.class)
        .in(Scopes.SINGLETON);

    bind(DiscoveryService.class).to(ZKDiscoveryService.class);
    bind(DiscoveryServiceClient.class).to(ProgramDiscoveryServiceClient.class).in(Scopes.SINGLETON);

    expose(DiscoveryService.class);
    expose(DiscoveryServiceClient.class);
  }

  /**
   * A Guice Provider to provide instance of {@link ZKDiscoveryService}.
   */
  private static final class ZkDiscoveryServiceProvider implements Provider<ZKDiscoveryService> {

    private final ZKClient zkClient;

    @Inject
    ZkDiscoveryServiceProvider(ZKClient zkClient) {
      this.zkClient = zkClient;
    }

    @Override
    public ZKDiscoveryService get() {
      return new ZKDiscoveryService(zkClient);
    }
  }

  /**
   * A DiscoveryServiceClient implementation that will namespace correctly for program service
   * discovery. Otherwise it'll delegate to default one.
   */
  private static final class ProgramDiscoveryServiceClient implements DiscoveryServiceClient {

    private static final Logger LOG = LoggerFactory.getLogger(ProgramDiscoveryServiceClient.class);
    private static final long CACHE_EXPIRES_MINUTES = 1;

    private final ZKClient zkClient;
    private final ZKDiscoveryService masterDiscoveryService;
    private final String twillNamespace;
    private final LoadingCache<String, ZKDiscoveryService> clients;

    @Inject
    ProgramDiscoveryServiceClient(ZKClient zkClient,
        CConfiguration configuration,
        ZKDiscoveryService masterDiscoveryService) {
      this.zkClient = zkClient;
      this.masterDiscoveryService = masterDiscoveryService;
      this.twillNamespace = configuration.get(Constants.CFG_TWILL_ZK_NAMESPACE);
      this.clients = CacheBuilder.newBuilder()
          .expireAfterAccess(CACHE_EXPIRES_MINUTES, TimeUnit.MINUTES)
          .removalListener((RemovalListener<String, ZKDiscoveryService>) notification ->
              Optional.ofNullable(notification.getValue()).ifPresent(ZKDiscoveryService::close))
          .build(createClientLoader());
    }

    @Override
    public ServiceDiscovered discover(final String name) {
      for (ProgramType programType : ProgramType.values()) {
        if (programType.isDiscoverable() && name.startsWith(
            programType.getDiscoverableTypeName() + ".")) {
          return clients.getUnchecked(name).discover(name);
        }
      }
      return masterDiscoveryService.discover(name);
    }

    private CacheLoader<String, ZKDiscoveryService> createClientLoader() {
      return new CacheLoader<String, ZKDiscoveryService>() {
        @Override
        public ZKDiscoveryService load(String key) {
          ProgramId programId = ServiceDiscoverable.getId(key);
          String ns = String.format("%s/%s", twillNamespace,
              TwillAppNames.toTwillAppName(programId));
          LOG.info("Create ZKDiscoveryClient for {}", ns);
          return new ZKDiscoveryService(ZKClients.namespace(zkClient, ns));
        }
      };
    }
  }
}
