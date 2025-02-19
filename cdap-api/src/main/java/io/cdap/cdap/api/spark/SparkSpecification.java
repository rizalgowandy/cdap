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

package io.cdap.cdap.api.spark;

import io.cdap.cdap.api.AbstractProgramSpecification;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.common.PropertyProvider;
import io.cdap.cdap.api.plugin.Plugin;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A default specification for {@link Spark} programs
 */
@Beta
public final class SparkSpecification extends AbstractProgramSpecification implements
    PropertyProvider {

  private final String mainClassName;
  private final Set<String> datasets;
  private final Map<String, String> properties;
  private final Resources clientResources;
  private final Resources driverResources;
  private final Resources executorResources;
  private final List<SparkHttpServiceHandlerSpecification> handlers;

  public SparkSpecification(String className, String name, String description,
      @Nullable String mainClassName,
      Set<String> datasets,
      Map<String, String> properties,
      @Nullable Resources clientResources,
      @Nullable Resources driverResources,
      @Nullable Resources executorResources,
      List<SparkHttpServiceHandlerSpecification> handlers, Map<String, Plugin> plugins) {
    super(className, name, description, plugins);
    this.mainClassName = mainClassName;
    this.properties = Collections.unmodifiableMap(new HashMap<>(properties));
    this.datasets = Collections.unmodifiableSet(new HashSet<>(datasets));
    this.clientResources = clientResources;
    this.driverResources = driverResources;
    this.executorResources = executorResources;
    this.handlers = Collections.unmodifiableList(new ArrayList<>(handlers));
  }

  /**
   * Get the main class name for the Spark program.
   *
   * @return the fully qualified class name or {@code null} if it was not set
   */
  @Nullable
  public String getMainClassName() {
    return mainClassName;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * Returns the set of static datasets used by the Spark program.
   */
  public Set<String> getDatasets() {
    return datasets;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * @return Resources requirement for the Spark client process or {@code null} if not specified.
   */
  @Nullable
  public Resources getClientResources() {
    return clientResources;
  }

  /**
   * @return Resources requirement for the Spark driver process or {@code null} if not specified.
   */
  @Nullable
  public Resources getDriverResources() {
    return driverResources;
  }

  /**
   * @return Resources requirement for the Spark executor processes or {@code null} if not
   *     specified.
   */
  @Nullable
  public Resources getExecutorResources() {
    return executorResources;
  }

  /**
   * @return a {@link List} of {@link SparkHttpServiceHandlerSpecification} defined for the Spark
   *     program.
   */
  public List<SparkHttpServiceHandlerSpecification> getHandlers() {
    return handlers;
  }
}
