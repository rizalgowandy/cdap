/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.proto.v1;

import io.cdap.cdap.api.Resources;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.Connection;
import io.cdap.cdap.etl.proto.UpgradeContext;
import io.cdap.cdap.etl.proto.UpgradeableConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * ETL Batch Configuration. Public constructors are deprecated. Use the builder instead.
 */
public final class ETLBatchConfig extends ETLConfig
    implements UpgradeableConfig<io.cdap.cdap.etl.proto.v2.ETLBatchConfig> {

  /**
   * Enum for the execution engine to use.
   */
  public enum Engine {
    MAPREDUCE, SPARK
  }

  private final Engine engine;
  private final String schedule;
  private final List<ETLStage> actions;
  private final Resources driverResources;

  private ETLBatchConfig(Engine engine, String schedule,
      ETLStage source, List<ETLStage> sinks, List<ETLStage> transforms,
      List<Connection> connections, Resources resources,
      Resources driverResources,
      List<ETLStage> actions) {
    super(source, sinks, transforms, connections, resources, true);
    this.engine = engine;
    this.schedule = schedule;
    this.actions = actions;
    this.driverResources = driverResources;
  }

  public Engine getEngine() {
    return engine == null ? Engine.MAPREDUCE : engine;
  }

  public String getSchedule() {
    return schedule;
  }

  public List<ETLStage> getActions() {
    return actions;
  }

  public Resources getDriverResources() {
    return driverResources;
  }

  @Override
  public boolean canUpgrade() {
    return true;
  }

  @Override
  public io.cdap.cdap.etl.proto.v2.ETLBatchConfig upgrade(UpgradeContext upgradeContext) {
    io.cdap.cdap.etl.proto.v2.ETLBatchConfig.Builder builder =
        io.cdap.cdap.etl.proto.v2.ETLBatchConfig.builder(schedule)
            .setEngine(io.cdap.cdap.etl.api.Engine.valueOf(getEngine().name()))
            .setDriverResources(getDriverResources());

    return upgradeBase(builder, upgradeContext, BatchSource.PLUGIN_TYPE,
        BatchSink.PLUGIN_TYPE).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ETLBatchConfig that = (ETLBatchConfig) o;

    return Objects.equals(schedule, that.schedule)
        && Objects.equals(actions, that.actions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), schedule, actions);
  }

  @Override
  public String toString() {
    return "ETLBatchConfig{"
        + "engine=" + engine
        + ", schedule='" + schedule + '\''
        + ", actions=" + actions
        + ", driverResources=" + driverResources
        + "} " + super.toString();
  }

  public static Builder builder(String schedule) {
    return new Builder(schedule);
  }

  /**
   * Builder for creating configs.
   */
  public static class Builder extends ETLConfig.Builder<Builder> {

    private final String schedule;
    private Engine engine;
    private List<ETLStage> actions;
    private Resources driverResources;

    public Builder(String schedule) {
      super();
      this.schedule = schedule;
      this.actions = new ArrayList<>();
      this.engine = Engine.MAPREDUCE;
    }

    public Builder addAction(ETLStage action) {
      this.actions.add(action);
      return this;
    }

    public Builder addActions(Collection<ETLStage> actions) {
      this.actions.addAll(actions);
      return this;
    }

    public Builder setEngine(Engine engine) {
      this.engine = engine;
      return this;
    }

    public Builder setDriverResources(Resources driverResources) {
      this.driverResources = driverResources;
      return this;
    }

    public ETLBatchConfig build() {
      return new ETLBatchConfig(engine, schedule, source, sinks, transforms,
          connections, resources, driverResources, actions);
    }
  }
}
