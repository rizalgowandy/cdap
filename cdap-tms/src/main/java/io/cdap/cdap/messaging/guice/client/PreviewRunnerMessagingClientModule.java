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

package io.cdap.cdap.messaging.guice.client;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.MessagingSystem;
import io.cdap.cdap.messaging.client.DefaultClientMessagingService;
import io.cdap.cdap.messaging.client.PreviewRunnerClientMessagingService;
import io.cdap.cdap.messaging.spi.MessagingService;

/**
 * The Guice module to provide binding for messaging system client in preview runners and is based
 * on {@link MessagingSystem#MESSAGING_SERVICE_ENABLED}. If set in cConf it binds to
 * {@link DefaultClientMessagingService} to prevent redundant network calls.
 */
public class PreviewRunnerMessagingClientModule extends AbstractModule {

  private final boolean messagingServiceEnabled;

  public PreviewRunnerMessagingClientModule(CConfiguration cConf) {
    this.messagingServiceEnabled = cConf.getBoolean(MessagingSystem.MESSAGING_SERVICE_ENABLED);
  }

  @Override
  protected void configure() {
    if (messagingServiceEnabled) {
      bind(MessagingService.class).to(DefaultClientMessagingService.class).in(Scopes.SINGLETON);
    } else {
      bind(MessagingService.class).to(PreviewRunnerClientMessagingService.class)
          .in(Scopes.SINGLETON);
    }
  }
}
