/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.api.service.http;

import io.cdap.cdap.internal.api.AbstractPluginConfigurable;
import java.util.Map;

/**
 * An abstract implementation of {@link HttpServiceHandler}. Classes that extend this class only
 * have to implement a configure method which can be used to add optional arguments.
 *
 * @param <T> type of service context
 * @param <V> type of service configurer
 */
public abstract class AbstractHttpServiceHandler<T extends HttpServiceContext, V extends HttpServiceConfigurer>
    extends AbstractPluginConfigurable<V> implements HttpServiceHandler<T, V> {

  private V configurer;
  private T context;

  /**
   * This can be overridden in child classes to add custom user properties during configure time.
   */
  protected void configure() {
    // no-op
  }

  /**
   * An implementation of {@link HttpServiceHandler#configure(HttpServiceConfigurer)}. Stores the
   * configurer so that it can be used later and then runs the configure method which is overwritten
   * by children classes.
   *
   * @param configurer the {@link HttpServiceConfigurer} which is used to configure this
   *     Handler
   */
  @Override
  public final void configure(V configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * An implementation of {@link HttpServiceHandler#initialize(HttpServiceContext)}. Stores the
   * context so that it can be used later.
   *
   * @param context the HTTP service runtime context
   */
  @Override
  public void initialize(T context) throws Exception {
    this.context = context;
  }

  /**
   * An implementation of {@link HttpServiceHandler#destroy()} which does nothing
   */
  @Override
  public void destroy() {
    // no-op
  }

  /**
   * @return the {@link HttpServiceContext} which was used when this class was initialized
   */
  protected final T getContext() {
    return context;
  }

  /**
   * @return the {@link HttpServiceConfigurer} used to configure this class
   */
  @Override
  protected final V getConfigurer() {
    return configurer;
  }

  /**
   * @param properties the properties to set
   * @see HttpServiceConfigurer#setProperties(java.util.Map)
   */
  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }
}
