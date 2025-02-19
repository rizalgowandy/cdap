/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.twill.AbstractMasterServiceManager;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * App Fabric Processor Service Management in Distributed Mode.
 */
public class AppFabricProcessorManager extends AbstractMasterServiceManager {

  @Inject
  AppFabricProcessorManager(CConfiguration cConf, TwillRunner twillRunner,
      DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient, Constants.Service.APP_FABRIC_PROCESSOR, twillRunner);
  }

  @Override
  public String getDescription() {
    return AppFabric.PROCESSOR_DESCRIPTION;
  }
}
