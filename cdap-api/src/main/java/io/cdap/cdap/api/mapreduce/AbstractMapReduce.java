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

package io.cdap.cdap.api.mapreduce;

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.internal.api.AbstractPluginConfigurable;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class provides a default implementation of {@link MapReduce} methods for easy
 * extension.
 */
public abstract class AbstractMapReduce extends AbstractPluginConfigurable<MapReduceConfigurer>
    implements MapReduce, ProgramLifecycle<MapReduceContext> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMapReduce.class);
  private MapReduceConfigurer configurer;
  private MapReduceContext context;

  @Override
  public final void configure(MapReduceConfigurer configurer) {
    this.configurer = configurer;
    configure();
  }

  /**
   * Override this method to configure this {@link MapReduce} job.
   */
  protected void configure() {
    // Default no-op
  }

  /**
   * Returns the {@link MapReduceConfigurer}, only available at configuration time.
   */
  @Override
  protected final MapReduceConfigurer getConfigurer() {
    return configurer;
  }

  /**
   * Sets the name of the {@link MapReduce}.
   */
  protected final void setName(String name) {
    configurer.setName(name);
  }

  /**
   * Sets the description of the {@link MapReduce}.
   */
  protected final void setDescription(String description) {
    configurer.setDescription(description);
  }

  /**
   * Sets a set of properties that will be available through the {@link
   * MapReduceSpecification#getProperties()} at runtime.
   *
   * @param properties the properties to set
   */
  protected final void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  /**
   * Sets the resources requirement for the driver of the {@link MapReduce}.
   */
  protected final void setDriverResources(Resources resources) {
    configurer.setDriverResources(resources);
  }

  /**
   * Sets the resources requirement for Mapper task of the {@link MapReduce}.
   */
  protected final void setMapperResources(Resources resources) {
    configurer.setMapperResources(resources);
  }

  /**
   * Sets the resources requirement for Reducer task of the {@link MapReduce}.
   */
  protected final void setReducerResources(Resources resources) {
    configurer.setReducerResources(resources);
  }

  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public final void initialize(MapReduceContext context) throws Exception {
    this.context = context;
    initialize();
  }

  /**
   * Classes derived from {@link AbstractMapReduce} can override this method to initialize the
   * {@link MapReduce}. {@link MapReduceContext} will be available in this method using {@link
   * AbstractMapReduce#getContext}.
   *
   * @throws Exception if there is any error in initializing the MapReduce
   */
  @TransactionPolicy(TransactionControl.IMPLICIT)
  protected void initialize() throws Exception {
    // do nothing by default
  }

  /**
   * Classes derived from {@link AbstractMapReduce} can override this method to destroy the {@link
   * MapReduce}.
   */
  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public void destroy() {
    // do nothing by default
  }

  /**
   * Return an instance of the {@link MapReduceContext}.
   */
  protected final MapReduceContext getContext() {
    return context;
  }
}
