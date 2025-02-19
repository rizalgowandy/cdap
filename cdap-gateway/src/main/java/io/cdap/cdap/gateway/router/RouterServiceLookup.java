/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.router;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.discovery.EndpointStrategy;
import io.cdap.cdap.common.discovery.RandomEndpointStrategy;
import io.cdap.cdap.common.service.ServiceDiscoverable;
import io.cdap.cdap.gateway.discovery.VersionFilteredServiceDiscovered;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Port -> service lookup.
 */
public class RouterServiceLookup {

  private static final Logger LOG = LoggerFactory.getLogger(RouterServiceLookup.class);

  private final DiscoveryServiceClient discoveryServiceClient;
  private final LoadingCache<RouteDestination, EndpointStrategy> discoverableCache;
  private final RouterPathLookup routerPathLookup;

  @Inject
  RouterServiceLookup(CConfiguration cConf, DiscoveryServiceClient discoveryServiceClient,
      RouterPathLookup routerPathLookup) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.routerPathLookup = routerPathLookup;
    this.discoverableCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build(new CacheLoader<RouteDestination, EndpointStrategy>() {
          @Override
          public EndpointStrategy load(RouteDestination key) {
            return discover(key);
          }
        });
  }

  /**
   * Returns the {@link EndpointStrategy} for picking endpoints that can serve the given request
   *
   * @param httpRequest supplies the header information for the lookup.
   * @return instance of EndpointStrategy if available null otherwise.
   */
  @Nullable
  public EndpointStrategy getDiscoverable(HttpRequest httpRequest) {
    // Normalize the path once and strip off any query string. Just keep the URI path.
    String path = URI.create(httpRequest.uri()).normalize().getPath();

    try {
      // Check if the requested path shouldn't be routed (internal URL).
      RouteDestination destService = routerPathLookup.getRoutingService(path, httpRequest);
      if (destService == null || Strings.isNullOrEmpty(destService.getServiceName())
          || destService.getServiceName().equals(Constants.Router.DONT_ROUTE_SERVICE)) {
        return null;
      }

      LOG.trace("Request was routed from {} to: {}", path, destService);

      return discoverableCache.get(destService);
    } catch (ExecutionException e) {
      return null;
    }
  }

  private EndpointStrategy discover(RouteDestination routeDestination) {
    String serviceName = routeDestination.getServiceName();

    if (ServiceDiscoverable.isUserService(serviceName)) {
      String version = routeDestination.getVersion();

      // If the request is from the versioned endpoint, filter the discoverables by the version
      if (version != null) {
        return new RandomEndpointStrategy(
            () -> new VersionFilteredServiceDiscovered(discoveryServiceClient.discover(serviceName),
                version));
      }
    }

    // For all other cases, use the random strategy
    return new RandomEndpointStrategy(() -> discoveryServiceClient.discover(serviceName));
  }
}
