/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package io.cdap.cdap.proto.id;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.element.EntityType;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Uniquely identifies a schedule.
 */
public class ScheduleId extends NamespacedEntityId implements ParentedId<ApplicationId> {

  private final String application;
  private final String schedule;
  private transient Integer hashCode;

  public ScheduleId(String namespace, String application, String schedule) {
    this(new ApplicationId(namespace, application), schedule);
  }

  ScheduleId(ApplicationId appId, String schedule) {
    super(appId.getNamespace(), EntityType.SCHEDULE);
    String application = appId.getApplication();
    if (application == null) {
      throw new NullPointerException("Application name cannot be null.");
    }
    if (application.isEmpty()) {
      throw new IllegalArgumentException("Application name cannot be empty.");
    }
    if (appId.getVersion() == null) {
      throw new NullPointerException("Version cannot be null.");
    }
    if (schedule == null) {
      throw new NullPointerException("Schedule id cannot be null.");
    }
    this.application = application;
    this.schedule = schedule;
  }

  @Override
  public MetadataEntity toMetadataEntity() {
    return MetadataEntity.builder().append(MetadataEntity.NAMESPACE, namespace)
        .append(MetadataEntity.APPLICATION, application)
        .appendAsType(MetadataEntity.SCHEDULE, schedule)
        .build();
  }

  public String getApplication() {
    return application;
  }

  public String getSchedule() {
    return schedule;
  }

  @Override
  public String getEntityName() {
    return getSchedule();
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    ScheduleId that = (ScheduleId) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(application, that.application)
        && Objects.equals(schedule, that.schedule);
  }

  @Override
  public int hashCode() {
    Integer hashCode = this.hashCode;
    if (hashCode == null) {
      this.hashCode = hashCode = Objects.hash(super.hashCode(), namespace, application, schedule);
    }
    return hashCode;
  }

  @Override
  public ApplicationId getParent() {
    return new ApplicationId(namespace, application);
  }

  @SuppressWarnings("unused")
  public static ScheduleId fromIdParts(Iterable<String> idString) {
    Iterator<String> iterator = idString.iterator();
    return new ScheduleId(
        next(iterator, "namespace"), next(iterator, "application"),
        nextAndEnd(iterator, "schedule"));
  }

  @Override
  public Iterable<String> toIdParts() {
    return Collections.unmodifiableList(Arrays.asList(namespace, application, schedule));
  }

  public static ScheduleId fromString(String string) {
    return EntityId.fromString(string, ScheduleId.class);
  }
}
