/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.gateway.handlers.PreferencesHttpHandlerInternal;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.proto.PreferencesDetail;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link PreferencesHttpHandlerInternal}
 */
public class PreferencesHttpHandlerInternalTest extends AppFabricTestBase {

  private static Store store;

  @BeforeClass
  public static void init() {
    store = getInjector().getInstance(Store.class);
  }

  private void addApplication(String namespace, Application app) throws ConflictException {
    ApplicationSpecification appSpec = Specifications.from(app);
    ApplicationMeta meta = new ApplicationMeta(appSpec.getName(), appSpec,
                                               new ChangeDetail(null, null, null,
                                                                System.currentTimeMillis()));
    store.addLatestApplication(new ApplicationId(namespace, appSpec.getName()), meta);
  }

  @Test
  public void testInstance() throws Exception {
    String uriInstance = "";

    // Verify preferences are unset.
    Map<String, String> properties = Maps.newHashMap();
    Assert.assertEquals(properties, getPreferences(uriInstance, false, 200));
    Assert.assertEquals(properties, getPreferences(uriInstance, true, 200));

    // Set preferences.
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    setPreferences(uriInstance, properties, 200);

    // Get preferences via internal REST APIs and validate.
    PreferencesDetail detail1 = null;
    detail1 = getPreferencesInternal(uriInstance, false, HttpResponseStatus.OK);
    Assert.assertEquals(properties, detail1.getProperties());
    Assert.assertFalse(detail1.getResolved());
    Assert.assertTrue(detail1.getSeqId() > 0);

    // Update preferences.
    properties.put("key3", "val3");
    setPreferences(uriInstance, properties, 200);

    // Get preferences via internal REST APIs and validate.
    PreferencesDetail detail2 = null;
    detail2 = getPreferencesInternal(uriInstance, false, HttpResponseStatus.OK);
    Assert.assertEquals(properties, detail2.getProperties());
    Assert.assertFalse(detail2.getResolved());
    Assert.assertTrue(detail2.getSeqId() > 0);
    Assert.assertTrue(detail2.getSeqId() > detail1.getSeqId());

    // "Resolved" should be ignored at instance level, as instance is the top level.
    detail2 = getPreferencesInternal(uriInstance, true, HttpResponseStatus.OK);
    Assert.assertEquals(properties, detail2.getProperties());
    Assert.assertFalse(detail2.getResolved());
    Assert.assertTrue(detail2.getSeqId() > 0);
    Assert.assertTrue(detail2.getSeqId() > detail1.getSeqId());

    // Delete preferences
    properties.clear();
    deletePreferences(uriInstance, 200);
    Assert.assertEquals(properties, getPreferences(uriInstance, false, 200));
    Assert.assertEquals(properties, getPreferences(uriInstance, true, 200));

    // Deleting preferences just set preferences to empty, the record row still exists and seqId should be there.
    PreferencesDetail detail3 = null;
    detail3 = getPreferencesInternal(uriInstance, false, HttpResponseStatus.OK);
    Assert.assertEquals(properties, detail3.getProperties());
    Assert.assertFalse(detail3.getResolved());
    Assert.assertTrue(detail3.getSeqId() > 0);
    Assert.assertTrue(detail3.getSeqId() > detail2.getSeqId());
  }

  @Test
  public void testNamespace() throws Exception {
    String uriInstance = getPreferenceUri();
    String uriNamespace1 = getPreferenceUri(TEST_NAMESPACE1);
    String uriNamespace2 = getPreferenceUri(TEST_NAMESPACE2);
    PreferencesDetail detail1 = null;
    PreferencesDetail detail2 = null;

    // Verify preferences are empty
    Map<String, String> namespaceProperties = Maps.newHashMap();
    Assert.assertEquals(namespaceProperties, getPreferences(uriNamespace1, false, 200));
    Assert.assertEquals(namespaceProperties, getPreferences(uriNamespace2, false, 200));
    Assert.assertEquals(namespaceProperties, getPreferences(uriNamespace1, true, 200));
    Assert.assertEquals(namespaceProperties, getPreferences(uriNamespace2, true, 200));

    // Set preferences on namespace1
    namespaceProperties.put("key0", "namespace-key0");
    namespaceProperties.put("namespace-key1", "namespace-val1");
    namespaceProperties.put("namespace-key2", "naemspace-val2");
    setPreferences(uriNamespace1, namespaceProperties, 200);

    // Get and verify preferences on namespace1 via internal REST API
    detail1 = getPreferencesInternal(uriNamespace1, true, HttpResponseStatus.OK);
    Assert.assertEquals(namespaceProperties, detail1.getProperties());
    Assert.assertTrue(detail1.getResolved());
    Assert.assertTrue(detail1.getSeqId() > 0);

    detail1 = getPreferencesInternal(uriNamespace1, false, HttpResponseStatus.OK);
    Assert.assertEquals(namespaceProperties, detail1.getProperties());
    Assert.assertFalse(detail1.getResolved());
    Assert.assertTrue(detail1.getSeqId() > 0);

    // Update preferences on namespace1
    namespaceProperties.put("namespace-key3", "namespace-val3");
    setPreferences(uriNamespace1, namespaceProperties, 200);

    // Get and verify seqId has increased via internal REST API
    detail2 = getPreferencesInternal(uriNamespace1, false, HttpResponseStatus.OK);
    Assert.assertEquals(namespaceProperties, detail2.getProperties());
    Assert.assertFalse(detail1.getResolved());
    Assert.assertTrue(detail2.getSeqId() > detail1.getSeqId());

    // Set preferences on top level instance
    Map<String, String> instanceProperties = Maps.newHashMap();
    instanceProperties.put("key0", "instance-val0");
    instanceProperties.put("instance-key1", "instance-val1");
    setPreferences(uriInstance, instanceProperties, 200);
    Assert.assertEquals(instanceProperties,
                        getPreferencesInternal(uriInstance, false, HttpResponseStatus.OK).getProperties());

    // Get preferences on namespace1 via internal REST API and verify it is unchanged.
    Assert.assertEquals(detail2,
                        getPreferencesInternal(uriNamespace1, false, HttpResponseStatus.OK));

    // Get resolved preferences on namespace1 via internal REST API and verify it includes preferences on instance.
    // Also namespace preferences take preferences over instance level for key "key0"
    Map<String, String> resolvedProperties = Maps.newHashMap();
    resolvedProperties.putAll(instanceProperties);
    resolvedProperties.putAll(namespaceProperties);
    detail2 = getPreferencesInternal(uriNamespace1, true, HttpResponseStatus.OK);
    Assert.assertEquals(resolvedProperties, detail2.getProperties());
    Assert.assertTrue(detail2.getResolved());

    // Get preferences on namespace2 via internal rest API.
    detail1 = getPreferencesInternal(uriNamespace2, false, HttpResponseStatus.OK);
    Assert.assertEquals(Collections.emptyMap(), detail1.getProperties());
    Assert.assertFalse(detail1.getResolved());

    // Update the preferences on the instance. Check the resolved for namespace2 via internal REST API,
    // which should reflect the change.
    instanceProperties.put("instance-key2", "instance-val2");
    setPreferences(uriInstance, instanceProperties, 200);
    detail2 = getPreferencesInternal(uriNamespace2, true, HttpResponseStatus.OK);
    Assert.assertEquals(instanceProperties, detail2.getProperties());
    Assert.assertTrue(detail2.getResolved());

    // Delete preferences for both namespace1 and namespace2
    deletePreferences(uriNamespace1, 200);
    deletePreferences(uriNamespace2, 200);

    // Set new preferences on the instance
    instanceProperties.put("instance-key3", "instance-val3");
    setPreferences(uriInstance, instanceProperties, 200);

    // Check that the change on instance shows up in the resolved for both namespaces.
    detail1 = getPreferencesInternal(uriNamespace1, true, HttpResponseStatus.OK);
    detail2 = getPreferencesInternal(uriNamespace2, true, HttpResponseStatus.OK);
    Assert.assertEquals(instanceProperties, detail1.getProperties());
    Assert.assertEquals(instanceProperties, detail2.getProperties());
    Assert.assertEquals(detail1, detail2);

    // Delete preferences for the instance.
    deletePreferences(uriInstance, 200);
    detail1 = getPreferencesInternal(uriNamespace1, true, HttpResponseStatus.OK);
    detail2 = getPreferencesInternal(uriNamespace2, true, HttpResponseStatus.OK);
    Assert.assertEquals(Collections.emptyMap(), detail1.getProperties());
    Assert.assertTrue(detail1.getSeqId() > 0);
    Assert.assertEquals(detail1, detail2);

    // Get preferences on invalid namespace should succeed, but get back a PreferencesDetail with empty property.
    PreferencesDetail detail = getPreferencesInternal(getPreferenceUri("invalidNamespace"), false,
                                                      HttpResponseStatus.OK);
    Assert.assertTrue(detail.getProperties().isEmpty());
    Assert.assertFalse(detail.getResolved());
    Assert.assertEquals(0, detail.getSeqId());
  }

  @Test
  public void testApplication() throws Exception {
    String appName = AllProgramsApp.NAME;
    String namespace1 = TEST_NAMESPACE1;
    String uriInstance = getPreferenceUri();
    String uriNamespace1 = getPreferenceUri(namespace1);
    String uriApp = getPreferenceUri(namespace1, appName);
    PreferencesDetail detail;
    Map<String, String> combinedProperties = Maps.newHashMap();

    // Application not created yet. Get preferences should succeed and get back one with empty properties.
    detail = getPreferencesInternal(
        getPreferenceUri(namespace1, "some_non_existing_app"), false,
                                    HttpResponseStatus.OK);
    Assert.assertTrue(detail.getProperties().isEmpty());
    Assert.assertFalse(detail.getResolved());
    Assert.assertEquals(0, detail.getSeqId());

    // Create the app.
    addApplication(namespace1, new AllProgramsApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(uriApp, false, 200));
    Assert.assertEquals(propMap, getPreferences(uriApp, true, 200));
    getPreferences(getPreferenceUri(namespace1, "InvalidAppName"), false, 404);

    // Application created but no preferences created yet. API call still succeeds but result is empty.
    detail = getPreferencesInternal(uriApp, false, HttpResponseStatus.OK);
    Assert.assertEquals(Collections.emptyMap(), detail.getProperties());
    // For entity without any references, seqId is set to default 0, otherwise it should be always > 0.
    Assert.assertEquals(0, detail.getSeqId());

    // Set the preference
    Map<String, String> instanceProperties =
      ImmutableMap.of(
        "key0", "instance-val0",
        "instance-key1", "instance-val1"
      );
    Map<String, String> namespace1Properties = ImmutableMap.of(
      "key0", "namespace-val0",
      "namespace1-key1", "namespace1-val1"
    );
    Map<String, String> appProperties = ImmutableMap.of(
      "key0", "app-val0",
      "app-key1", "app-val1"
    );

    setPreferences(uriInstance, instanceProperties, 200);
    setPreferences(uriNamespace1, namespace1Properties, 200);
    setPreferences(uriApp, appProperties, 200);

    // Get and verify preferences on the application
    detail = getPreferencesInternal(uriApp, false, HttpResponseStatus.OK);
    Assert.assertEquals(appProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertFalse(detail.getResolved());

    // Get and verify resolved preferences on the application
    detail = getPreferencesInternal(uriApp, true, HttpResponseStatus.OK);
    combinedProperties.clear();
    combinedProperties.putAll(instanceProperties);
    combinedProperties.putAll(namespace1Properties);
    combinedProperties.putAll(appProperties);
    Assert.assertEquals(combinedProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());

    // Delete preferences on the application and verify resolved
    deletePreferences(uriApp, 200);
    detail = getPreferencesInternal(uriApp, true, HttpResponseStatus.OK);
    combinedProperties.clear();
    combinedProperties.putAll(instanceProperties);
    combinedProperties.putAll(namespace1Properties);
    Assert.assertEquals(combinedProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());


    // Delete preferences on the namespace and verify.
    deletePreferences(uriNamespace1, 200);
    detail = getPreferencesInternal(uriApp, true, HttpResponseStatus.OK);
    combinedProperties.clear();
    combinedProperties.putAll(instanceProperties);
    Assert.assertEquals(combinedProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());

    // Delete preferences on the instance and verify.
    deletePreferences(uriInstance, 200);
    detail = getPreferencesInternal(uriApp, true, HttpResponseStatus.OK);
    combinedProperties.clear();
    Assert.assertEquals(combinedProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());
  }

  @Test
  public void testProgram() throws Exception {
    String uriInstance = getPreferenceUri();
    String namespace2 = TEST_NAMESPACE2;
    String appName = AllProgramsApp.NAME;
    String uriNamespace2Service = getPreferenceUri(namespace2, appName, "services", AllProgramsApp.NoOpService.NAME);
    PreferencesDetail detail;
    Map<String, String> programProperties = Maps.newHashMap();

    // Create application.
    addApplication(namespace2, new AllProgramsApp());

    // Get preferences on invalid program type
    getPreferencesInternal(getPreferenceUri(
      namespace2, appName, "invalidType", "somename"), false, HttpResponseStatus.BAD_REQUEST);

    // Get preferences on non-existing program id. Should succeed and get back a PreferencesDetail with empty properites
    detail = getPreferencesInternal(
        getPreferenceUri(namespace2, appName, "services", "somename"),
                                    false,
                                    HttpResponseStatus.OK);
    Assert.assertTrue(detail.getProperties().isEmpty());
    Assert.assertFalse(detail.getResolved());
    Assert.assertEquals(0, detail.getSeqId());

    // Set preferences on the program
    programProperties.clear();
    programProperties.put("key0", "program-val0");
    programProperties.put("program-key1", "program-val1");
    setPreferences(uriNamespace2Service, programProperties, 200);

    // Get and verify preferences
    detail = getPreferencesInternal(uriNamespace2Service, false, HttpResponseStatus.OK);
    Assert.assertEquals(programProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertFalse(detail.getResolved());

    // Set preferences on the instance and verify.
    Map<String, String> instanceProperties = ImmutableMap.of(
      "key0", "instance-key0",
      "instance-key1", "instance-val1"
    );
    setPreferences(uriInstance, instanceProperties, 200);

    // Get resolved preferences on the program
    detail = getPreferencesInternal(uriNamespace2Service, true, HttpResponseStatus.OK);
    Map<String, String> combinedProperties = Maps.newHashMap();
    combinedProperties.putAll(instanceProperties);
    combinedProperties.putAll(programProperties);
    Assert.assertEquals(combinedProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());

    // Delete preferences on the program
    deletePreferences(uriNamespace2Service, 200);
    detail = getPreferencesInternal(uriNamespace2Service, true, HttpResponseStatus.OK);
    Assert.assertEquals(instanceProperties, detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());

    // Delete preferences on the instance
    deletePreferences(uriInstance, 200);
    detail = getPreferencesInternal(uriNamespace2Service, true, HttpResponseStatus.OK);
    Assert.assertEquals(Collections.emptyMap(), detail.getProperties());
    Assert.assertTrue(detail.getSeqId() > 0);
    Assert.assertTrue(detail.getResolved());
  }
}
