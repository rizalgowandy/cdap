/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
import io.cdap.cdap.gateway.handlers.PreferencesHttpHandler;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.profile.Profile;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link PreferencesHttpHandler}
 */
public class PreferencesHttpHandlerTest extends AppFabricTestBase {

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
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setPreferences(getPreferenceUri(), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), true, 200));
    propMap.clear();
    deletePreferences(getPreferenceUri(), 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), true, 200));
  }

  @Test
  public void testNamespace() throws Exception {
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE2), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE2), true, 200));
    propMap.put("k1", "3@#3");
    propMap.put("@#$#ljfds", "231@#$");
    setPreferences(getPreferenceUri(TEST_NAMESPACE1), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200));

    Map<String, String> instanceMap = Maps.newHashMap();
    instanceMap.put("k1", "432432*#######");
    setPreferences(getPreferenceUri(), instanceMap, 200);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(), true, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE2), true, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200));

    instanceMap.put("k2", "(93424");
    setPreferences(getPreferenceUri(), instanceMap, 200);
    instanceMap.putAll(propMap);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200));

    deletePreferences(getPreferenceUri(TEST_NAMESPACE1), 200);
    deletePreferences(getPreferenceUri(TEST_NAMESPACE2), 200);

    instanceMap.clear();
    instanceMap.put("*&$kjh", "*(&*1");
    setPreferences(getPreferenceUri(), instanceMap, 200);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE2), true, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200));
    instanceMap.clear();
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE2), false, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200));

    deletePreferences(getPreferenceUri(), 200);
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE2), true, 200));
    Assert.assertEquals(instanceMap, getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200));
    getPreferences(getPreferenceUri("invalidNamespace"), true, 404);
  }

  @Test
  public void testApplication() throws Exception {
    String appName = AllProgramsApp.NAME;

    addApplication(TEST_NAMESPACE1, new AllProgramsApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(
        getPreferenceUri(TEST_NAMESPACE1, appName), false, 200));
    Assert.assertEquals(propMap, getPreferences(
        getPreferenceUri(TEST_NAMESPACE1, appName), true, 200));
    getPreferences(getPreferenceUri(TEST_NAMESPACE1, "InvalidAppName"), false, 404);
    setPreferences(getPreferenceUri(), ImmutableMap.of("k1", "instance"), 200);
    setPreferences(
        getPreferenceUri(TEST_NAMESPACE1), ImmutableMap.of("k1", "namespace"), 200);
    setPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), ImmutableMap.of("k1", "application"), 200);
    Assert.assertEquals("application",
                        getPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), false, 200).get("k1"));
    Assert.assertEquals("application",
                        getPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), true, 200).get("k1"));
    Assert.assertEquals("namespace", getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200).get("k1"));
    Assert.assertEquals("namespace", getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200).get("k1"));
    Assert.assertEquals("instance", getPreferences(getPreferenceUri(), true, 200).get("k1"));
    Assert.assertEquals("instance", getPreferences(getPreferenceUri(), false, 200).get("k1"));
    deletePreferences(getPreferenceUri(TEST_NAMESPACE1, appName), 200);
    Assert.assertEquals("namespace",
                        getPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), false, 200).get("k1"));
    deletePreferences(getPreferenceUri(TEST_NAMESPACE1), 200);
    Assert.assertEquals("instance",
                        getPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), true, 200).get("k1"));
    Assert.assertEquals("instance",
                        getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200).get("k1"));
    deletePreferences(getPreferenceUri(), 200);
    Assert.assertNull(getPreferences(getPreferenceUri(), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceUri(TEST_NAMESPACE1), true, 200).get("k1"));
    Assert.assertNull(getPreferences(getPreferenceUri(TEST_NAMESPACE1, appName), true, 200).get("k1"));
  }

  @Test
  public void testProgram() throws Exception {
    String appName = AllProgramsApp.NAME;
    String serviceName = AllProgramsApp.NoOpService.NAME;

    addApplication(TEST_NAMESPACE2, new AllProgramsApp());
    Map<String, String> propMap = Maps.newHashMap();
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), false, 200));
    getPreferences(
        getPreferenceUri(TEST_NAMESPACE2, appName, "invalidType", "somename"), false, 400);
    getPreferences(
        getPreferenceUri(TEST_NAMESPACE2, appName, "services", "somename"), false, 404);
    propMap.put("k1", "k349*&#$");
    setPreferences(
        getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), false, 200));
    propMap.put("k1", "instance");
    setPreferences(getPreferenceUri(), propMap, 200);
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), true, 200));
    propMap.put("k1", "k349*&#$");
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), false, 200));
    deletePreferences(
        getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), 200);
    propMap.put("k1", "instance");
    Assert.assertEquals(0, getPreferences(
        getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName),
                                          false, 200).size());
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), true, 200));
    deletePreferences(getPreferenceUri(), 200);
    propMap.clear();
    Assert.assertEquals(propMap, getPreferences(
      getPreferenceUri(TEST_NAMESPACE2, appName, "services", serviceName), false, 200));
    Assert.assertEquals(propMap, getPreferences(getPreferenceUri(), false, 200));
  }

  @Test
  public void testSetPreferenceWithProfiles() throws Exception {
    // put my profile
    ProfileId myProfile = new ProfileId(TEST_NAMESPACE1, "MyProfile");
    putProfile(myProfile, Profile.NATIVE, 200);

    // put some properties with my profile, it should work fine
    Map<String, String> properties = new HashMap<>();
    properties.put("1st key", "1st value");
    properties.put("2nd key", "2nd value");
    properties.put(SystemArguments.PROFILE_NAME, "USER:MyProfile");
    Map<String, String> expected = ImmutableMap.copyOf(properties);
    setPreferences(getPreferenceUri(TEST_NAMESPACE1), properties, 200);
    Assert.assertEquals(expected, getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200));

    // put some property with non-existing profile, it should fail with 404
    properties.put(SystemArguments.PROFILE_NAME, "NonExisting");
    setPreferences(getPreferenceUri(TEST_NAMESPACE1), properties, 404);
    Assert.assertEquals(expected, getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200));

    // disable the profile and put again, it should fail with 409
    disableProfile(myProfile, 200);
    properties.put(SystemArguments.PROFILE_NAME, "USER:MyProfile");
    setPreferences(getPreferenceUri(TEST_NAMESPACE1), properties, 409);
    Assert.assertEquals(expected, getPreferences(getPreferenceUri(TEST_NAMESPACE1), false, 200));

    deletePreferences(getPreferenceUri(TEST_NAMESPACE1), 200);
  }
}
