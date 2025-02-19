/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import io.cdap.cdap.api.schedule.Trigger;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents a schedule in a REST request/response.
 *
 * All fields are nullable because after Json deserialization, they may be null. Also, this is used
 * both for creating a schedule and for updating a schedule. When updating, all fields are optional
 * - only the fields that are present will be updated.
 */
public class ScheduleDetail {

  private final String namespace;
  private final String application;
  private final String name;
  private final String description;
  private final ScheduleProgramInfo program;
  private final Map<String, String> properties;
  private final Trigger trigger;
  private final List<? extends Constraint> constraints;
  private final Long timeoutMillis;
  private final String status;
  private final Long lastUpdateTime;

  public ScheduleDetail(@Nullable String name,
      @Nullable String description,
      @Nullable ScheduleProgramInfo program,
      @Nullable Map<String, String> properties,
      @Nullable Trigger trigger,
      @Nullable List<? extends Constraint> constraints,
      @Nullable Long timeoutMillis) {
    this(null, null, name, description, program, properties, trigger, constraints, timeoutMillis,
        null, null);
  }

  public ScheduleDetail(@Nullable String namespace,
      @Nullable String application,
      @Nullable String name,
      @Nullable String description,
      @Nullable ScheduleProgramInfo program,
      @Nullable Map<String, String> properties,
      @Nullable Trigger trigger,
      @Nullable List<? extends Constraint> constraints,
      @Nullable Long timeoutMillis,
      @Nullable String status,
      @Nullable Long lastUpdateTime) {
    this.namespace = namespace;
    this.application = application;
    this.name = name;
    this.description = description;
    this.program = program;
    this.properties = properties;
    this.trigger = trigger;
    this.constraints = constraints;
    this.timeoutMillis = timeoutMillis;
    this.status = status;
    this.lastUpdateTime = lastUpdateTime;
  }

  @Nullable
  public String getNamespace() {
    return namespace;
  }

  @Nullable
  public String getApplication() {
    return application;
  }

  @Nullable
  public String getName() {
    return name;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Nullable
  public ScheduleProgramInfo getProgram() {
    return program;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return properties;
  }

  @Nullable
  public Trigger getTrigger() {
    return trigger;
  }

  @Nullable
  public List<? extends Constraint> getConstraints() {
    return constraints;
  }

  @Nullable
  public Long getTimeoutMillis() {
    return timeoutMillis;
  }

  @Nullable
  public String getStatus() {
    return status;
  }

  @Nullable
  public Long getLastUpdateTime() {
    return lastUpdateTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScheduleDetail that = (ScheduleDetail) o;
    return Objects.equals(name, that.name)
        && Objects.equals(description, that.description)
        && Objects.equals(program, that.program)
        && Objects.equals(properties, that.properties)
        && Objects.equals(trigger, that.trigger)
        && Objects.equals(constraints, that.constraints)
        && Objects.equals(timeoutMillis, that.timeoutMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, program, properties, trigger, constraints,
        timeoutMillis);
  }

  @Override
  public String toString() {
    return "ScheduleDetail{"
        + "name='" + name + '\''
        + ", description='" + description + '\''
        + ", program=" + program
        + ", properties=" + properties
        + ", trigger=" + trigger
        + ", constraints=" + constraints
        + ", timeoutMillis=" + timeoutMillis
        + '}';
  }
}
