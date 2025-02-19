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

package io.cdap.cdap.internal.app.runtime.schedule.trigger;

import io.cdap.cdap.api.schedule.TimeTriggerInfo;
import java.io.Serializable;

/**
 * The time trigger information to be passed to the triggered program.
 */
public class DefaultTimeTriggerInfo extends AbstractTriggerInfo implements TimeTriggerInfo,
    Serializable {

  private final String cronExpression;
  private final long logicalStartTime;

  public DefaultTimeTriggerInfo(String cronExpression, long logicalStartTime) {
    super(Type.TIME);
    this.cronExpression = cronExpression;
    this.logicalStartTime = logicalStartTime;
  }

  @Override
  public String getCronExpression() {
    return cronExpression;
  }

  @Override
  public long getLogicalStartTime() {
    return logicalStartTime;
  }
}
