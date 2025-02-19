/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.plugins;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.spi.ContextAwareBase;
import java.io.File;
import org.apache.twill.filesystem.Location;

/**
 * Location triggering policy base class
 */
public abstract class LocationTriggeringPolicyBase extends ContextAwareBase implements
    LocationTriggeringPolicy {

  private Location activeLocation;
  private long activeLocationSize;
  private boolean start;

  @Override
  public void start() {
    start = true;
  }

  @Override
  public void stop() {
    start = false;
  }

  @Override
  public boolean isStarted() {
    return start;
  }

  @Override
  public void setLocation(Location location) {
    activeLocation = location;
  }

  public Location getActiveLocation() {
    return activeLocation;
  }

  @Override
  public boolean isTriggeringEvent(final File activeFile, final ILoggingEvent event)
      throws LogbackException {
    return isTriggeringEvent(event);
  }

  @Override
  public void setActiveLocationSize(long size) {
    activeLocationSize = size;
  }

  public long getActiveLocationSize() {
    return activeLocationSize;
  }
}
