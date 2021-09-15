/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.collect.ImmutableSet;
import com.google.crypto.tink.CleartextKeysetHandle;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.aead.AeadConfig;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.metrics.ProgramTypeMetricTag;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.CipherException;
import io.cdap.cdap.security.auth.TinkCipher;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Tests for {@link DefaultAccessEnforcer}.
 */
public class DefaultAccessEnforcerTest extends AuthorizationTestBase {

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId NS = new NamespaceId("ns");
  private static final ApplicationId APP = NS.app("app");

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setupClass() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, InMemoryAccessController.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(
      locationFactory, InMemoryAccessController.class, manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
  }

  @Test
  public void testAuthenticationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testAuthorizationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.Authorization.ENABLED, false);
    verifyDisabled(cConfCopy);
  }

  @Test
  public void testPropagationDisabled() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      DefaultAccessEnforcer accessEnforcer =
        new DefaultAccessEnforcer(cConfCopy, SCONF, accessControllerInstantiator, null,
                                  new NoOpMetricsCollectionService());
      accessControllerInstantiator.get().grant(Authorizable.fromEntityId(NS), ALICE,
                                               ImmutableSet.of(StandardPermission.UPDATE));
      accessEnforcer.enforce(NS, ALICE, StandardPermission.UPDATE);
      try {
        accessEnforcer.enforce(APP, ALICE, StandardPermission.UPDATE);
        Assert.fail("Alice should not have ADMIN privilege on the APP.");
      } catch (UnauthorizedException ignored) {
        // expected
      }
    }
  }

  @Test
  public void testAuthEnforce() throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      DefaultAccessEnforcer authEnforcementService =
        new DefaultAccessEnforcer(CCONF, SCONF, accessControllerInstantiator, null,
                                  new NoOpMetricsCollectionService());
      // update privileges for alice. Currently alice has not been granted any privileges.
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.UPDATE);

      // grant some test privileges
      DatasetId ds = NS.dataset("ds");
      accessController.grant(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET,
                                                                                   StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds), BOB, ImmutableSet.of(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(NS, EntityType.DATASET), ALICE,
                             ImmutableSet.of(StandardPermission.LIST));

      // auth enforcement for alice should succeed on ns for actions read, write and list datasets
      authEnforcementService.enforce(NS, ALICE, ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));
      authEnforcementService.enforceOnParent(EntityType.DATASET, NS, ALICE, StandardPermission.LIST);
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, EnumSet.allOf(StandardPermission.class));
      // alice do not have CREATE, READ or WRITE on the dataset, so authorization should fail
      assertAuthorizationFailure(authEnforcementService, ds, ALICE, StandardPermission.GET);
      assertAuthorizationFailure(authEnforcementService, ds, ALICE, StandardPermission.UPDATE);
      assertAuthorizationFailure(authEnforcementService, EntityType.DATASET, NS, ALICE, StandardPermission.CREATE);

      // Alice doesn't have Delete right on NS, hence should fail.
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.DELETE);
      // bob enforcement should succeed since we grant him admin privilege
      authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
      // revoke all of alice's privileges
      accessController.revoke(Authorizable.fromEntityId(NS), ALICE, ImmutableSet.of(StandardPermission.GET));
      try {
        authEnforcementService.enforce(NS, ALICE, StandardPermission.GET);
        Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                  ALICE, StandardPermission.GET, NS));
      } catch (UnauthorizedException ignored) {
        // expected
      }
      accessController.revoke(Authorizable.fromEntityId(NS));

      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.GET);
      assertAuthorizationFailure(authEnforcementService, NS, ALICE, StandardPermission.UPDATE);
      authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
    }
  }

  @Test
  public void testIsVisible() throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      NamespaceId ns1 = new NamespaceId("ns1");
      NamespaceId ns2 = new NamespaceId("ns2");
      DatasetId ds11 = ns1.dataset("ds11");
      DatasetId ds12 = ns1.dataset("ds12");
      DatasetId ds21 = ns2.dataset("ds21");
      DatasetId ds22 = ns2.dataset("ds22");
      DatasetId ds23 = ns2.dataset("ds33");
      Set<NamespaceId> namespaces = ImmutableSet.of(ns1, ns2);
      // Alice has access on ns1, ns2, ds11, ds21, ds23, Bob has access on ds11, ds12, ds22
      accessController.grant(Authorizable.fromEntityId(ns1), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ns2), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds11), ALICE, Collections.singleton(StandardPermission.GET));
      accessController.grant(Authorizable.fromEntityId(ds11), BOB, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds12), BOB, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds12), BOB, EnumSet.allOf(StandardPermission.class));
      accessController.grant(Authorizable.fromEntityId(ds21), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds23), ALICE, Collections.singleton(StandardPermission.UPDATE));
      accessController.grant(Authorizable.fromEntityId(ds22), BOB, Collections.singleton(StandardPermission.UPDATE));
      DefaultAccessEnforcer authEnforcementService = new DefaultAccessEnforcer(CCONF, SCONF,
                                                                               accessControllerInstantiator,
                                                                               null,
                                                                               new NoOpMetricsCollectionService());
      Assert.assertEquals(namespaces.size(), authEnforcementService.isVisible(namespaces, ALICE).size());
      // bob should also be able to list two namespaces since he has privileges on the dataset in both namespaces
      Assert.assertEquals(namespaces.size(), authEnforcementService.isVisible(namespaces, BOB).size());
      Set<DatasetId> expectedDatasetIds = ImmutableSet.of(ds11, ds21, ds23);
      Assert.assertEquals(expectedDatasetIds.size(), authEnforcementService.isVisible(expectedDatasetIds,
                                                                                      ALICE).size());
      expectedDatasetIds = ImmutableSet.of(ds12, ds22);
      // this will be empty since now isVisible will not check the hierarchy privilege for the parent of the entity
      Assert.assertEquals(Collections.EMPTY_SET, authEnforcementService.isVisible(expectedDatasetIds, ALICE));
      expectedDatasetIds = ImmutableSet.of(ds11, ds12, ds22);
      Assert.assertEquals(expectedDatasetIds.size(), authEnforcementService.isVisible(expectedDatasetIds, BOB).size());
      expectedDatasetIds = ImmutableSet.of(ds21, ds23);
      Assert.assertTrue(authEnforcementService.isVisible(expectedDatasetIds, BOB).isEmpty());
    }
  }

  @Test
  public void testAuthEnforceWithEncryptedCredential()
    throws IOException, AccessException, CipherException, GeneralSecurityException {
    SConfiguration sConfCopy = enableCredentialEncryption();
    TinkCipher cipher = new TinkCipher(sConfCopy);

    String cred = cipher.encryptToBase64("credential".getBytes(StandardCharsets.UTF_8), null);
    Principal userWithCredEncrypted = new Principal("userFoo", Principal.PrincipalType.USER, null,
                                                    new Credential(cred, Credential.CredentialType.EXTERNAL_ENCRYPTED));

    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      DefaultAccessEnforcer accessEnforcer = new DefaultAccessEnforcer(CCONF, sConfCopy, accessControllerInstantiator,
                                                                       null, new NoOpMetricsCollectionService());

      assertAuthorizationFailure(accessEnforcer, NS, userWithCredEncrypted, StandardPermission.UPDATE);

      accessController.grant(Authorizable.fromEntityId(NS), userWithCredEncrypted,
                             ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));

      accessEnforcer.enforce(NS, userWithCredEncrypted, StandardPermission.GET);
      accessEnforcer.enforce(NS, userWithCredEncrypted, StandardPermission.UPDATE);
    }
  }

  @Test
  public void testAuthEnforceWithBadEncryptedCredential()
    throws IOException, AccessException, CipherException, GeneralSecurityException {
    thrown.expect(Exception.class);
    thrown.expectMessage("Failed to decrypt credential in principle:");

    SConfiguration sConfCopy = enableCredentialEncryption();
    TinkCipher cipher = new TinkCipher(sConfCopy);

    String badCipherCred = Base64.getEncoder().encodeToString("invalid encrypted credential".getBytes());

    Principal userWithCredEncrypted = new Principal("userFoo", Principal.PrincipalType.USER, null,
                                                    new Credential(badCipherCred,
                                                                   Credential.CredentialType.EXTERNAL_ENCRYPTED));

    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();

      accessController.grant(Authorizable.fromEntityId(NS), userWithCredEncrypted,
                             ImmutableSet.of(StandardPermission.GET, StandardPermission.GET));

      DefaultAccessEnforcer accessEnforcer = new DefaultAccessEnforcer(CCONF, sConfCopy, accessControllerInstantiator,
                                                                       null, new NoOpMetricsCollectionService());

      accessEnforcer.enforce(NS, userWithCredEncrypted, StandardPermission.GET);
    }
  }

  @Test
  public void testIsVisibleWithEncryptedCredential()
    throws IOException, AccessException, CipherException, GeneralSecurityException {
    SConfiguration sConfCopy = enableCredentialEncryption();
    TinkCipher cipher = new TinkCipher(sConfCopy);

    String cred = cipher.encryptToBase64("credential".getBytes(StandardCharsets.UTF_8), null);
    Principal userWithCredEncrypted = new Principal("userFoo", Principal.PrincipalType.USER, null,
                                                    new Credential(cred, Credential.CredentialType.EXTERNAL_ENCRYPTED));

    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      DefaultAccessEnforcer accessEnforcer = new DefaultAccessEnforcer(CCONF, sConfCopy, accessControllerInstantiator,
                                                                       null, new NoOpMetricsCollectionService());

      Set<NamespaceId> namespaces = ImmutableSet.of(NS);

      Assert.assertEquals(0, accessEnforcer.isVisible(namespaces, userWithCredEncrypted).size());

      accessController.grant(Authorizable.fromEntityId(NS), userWithCredEncrypted,
                             ImmutableSet.of(StandardPermission.GET, StandardPermission.UPDATE));

      Assert.assertEquals(1, accessEnforcer.isVisible(namespaces, userWithCredEncrypted).size());
    }
  }

  @Test
  public void testSystemUser() throws IOException, AccessException {
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    Principal systemUser =
      new Principal(UserGroupInformation.getCurrentUser().getShortUserName(), Principal.PrincipalType.USER);
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      DefaultAccessEnforcer accessEnforcer = new DefaultAccessEnforcer(cConfCopy, SCONF, accessControllerInstantiator,
                                                                       null, new NoOpMetricsCollectionService());
      NamespaceId ns1 = new NamespaceId("ns1");
      accessEnforcer.enforce(NamespaceId.SYSTEM, systemUser, EnumSet.allOf(StandardPermission.class));
      accessEnforcer.enforce(NamespaceId.SYSTEM, systemUser, StandardPermission.GET);
      Assert.assertEquals(ImmutableSet.of(NamespaceId.SYSTEM),
                          accessEnforcer.isVisible(ImmutableSet.of(ns1, NamespaceId.SYSTEM),
                                                   systemUser));
    }
  }

  @Test
  public void testInternalAuthEnforce() throws IOException, AccessException {
    Principal userWithInternalCred = new Principal("system", Principal.PrincipalType.USER, null,
                                                   new Credential("credential",
                                                                  Credential.CredentialType.INTERNAL));
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      DefaultAccessEnforcer accessEnforcer = new DefaultAccessEnforcer(cConfCopy, SCONF, accessControllerInstantiator,
                                                                       new NoOpAccessController(),
                                                                       new NoOpMetricsCollectionService());
      // Make sure that the actual access controller does not have access.
      assertAuthorizationFailure(accessController, NS, userWithInternalCred, StandardPermission.GET);
      assertAuthorizationFailure(accessController, NS, userWithInternalCred, StandardPermission.UPDATE);
      // The no-op access enforcer allows all requests through, so this should succeed if it is using the right
      // access controller.
      accessEnforcer.enforce(NS, userWithInternalCred, StandardPermission.GET);
      accessEnforcer.enforce(NS, userWithInternalCred, StandardPermission.UPDATE);
    }
  }

  @Test
  public void testInternalIsVisible() throws IOException, AccessException {
    Principal userWithInternalCred = new Principal("system", Principal.PrincipalType.USER, null,
                                                   new Credential("credential",
                                                                  Credential.CredentialType.INTERNAL));
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = accessControllerInstantiator.get();
      DefaultAccessEnforcer accessEnforcer = new DefaultAccessEnforcer(cConfCopy, SCONF, accessControllerInstantiator,
                                                                       new NoOpAccessController(),
                                                                       new NoOpMetricsCollectionService());

      Set<EntityId> namespaces = ImmutableSet.of(NS);
      // Make sure that the actual access controller does not have access.
      Assert.assertEquals(Collections.emptySet(), accessController.isVisible(namespaces, userWithInternalCred));
      // The no-op access enforcer allows all requests through, so this should succeed if it is using the right
      // access controller.
      Assert.assertEquals(namespaces, accessEnforcer.isVisible(namespaces, userWithInternalCred));
    }
  }

  private void verifyDisabled(CConfiguration cConf) throws IOException, AccessException {
    try (AccessControllerInstantiator accessControllerInstantiator =
           new AccessControllerInstantiator(cConf, AUTH_CONTEXT_FACTORY)) {
      DefaultAccessEnforcer authEnforcementService =
        new DefaultAccessEnforcer(cConf, SCONF, accessControllerInstantiator, null,
                                  new NoOpMetricsCollectionService());
      DatasetId ds = NS.dataset("ds");
      // All enforcement operations should succeed, since authorization is disabled
      accessControllerInstantiator.get().grant(Authorizable.fromEntityId(ds), BOB,
                                               ImmutableSet.of(StandardPermission.UPDATE));
      authEnforcementService.enforce(NS, ALICE, StandardPermission.UPDATE);
      authEnforcementService.enforce(ds, BOB, StandardPermission.UPDATE);
      authEnforcementService.enforce(NS, BOB, StandardPermission.GET);
      authEnforcementService.enforce(ds, BOB, StandardPermission.GET);
      Assert.assertEquals(2, authEnforcementService.isVisible(ImmutableSet.<EntityId>of(NS, ds), BOB).size());
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal,
                                          Permission permission) throws AccessException {
    try {
      authEnforcementService.enforce(entityId, principal, permission);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s but it does.",
                                principal, permission, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService, EntityType entityType,
                                          EntityId parentId, Principal principal,
                                          Permission permission) throws AccessException {
    try {
      authEnforcementService.enforceOnParent(entityType, parentId, principal, permission);
      Assert.fail(String.format("Expected %s to not have '%s' privilege on %s in %s but it does.",
                                principal, permission, entityType, parentId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private void assertAuthorizationFailure(AccessEnforcer authEnforcementService,
                                          EntityId entityId, Principal principal,
                                          Set<? extends Permission> permissions) throws AccessException {
    try {
      authEnforcementService.enforce(entityId, principal, permissions);
      Assert.fail(String.format("Expected %s to not have '%s' privileges on %s but it does.",
                                principal, permissions, entityId));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private SConfiguration enableCredentialEncryption() throws IOException, GeneralSecurityException {
    SConfiguration sConfCopy = SConfiguration.copy(SCONF);
    sConfCopy.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_ENABLED, "true");
    sConfCopy.set(Constants.Security.Authentication.USER_CREDENTIAL_ENCRYPTION_KEYSET,
                  generateEncryptionKeyset());
    return sConfCopy;
  }

  private String generateEncryptionKeyset() throws IOException, GeneralSecurityException {
    AeadConfig.register();
    KeysetHandle keysetHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"));
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    CleartextKeysetHandle.write(keysetHandle, JsonKeysetWriter.withOutputStream(outputStream));
    return outputStream.toString();
  }

  @Test
  public void testExpectedMetricsTagsForEntityId() {
    String namespaceName = "namespace";
    NamespaceId namespaceId = new NamespaceId(namespaceName);
    Map<String, String> expectedTags = new HashMap<>();
    expectedTags.put(Constants.Metrics.Tag.NAMESPACE, namespaceName);
    Map<String, String> tags = DefaultAccessEnforcer.createEntityIdMetricsTags(namespaceId);
    Assert.assertEquals(expectedTags, tags);
  }

  @Test
  public void testExpectedMetricsTagsForChildEntityId() {
    String namespaceName = "namespace";
    NamespaceId namespaceId = new NamespaceId(namespaceName);
    String appName = "app";
    ApplicationId applicationId = namespaceId.app(appName);
    String programName = "program";
    ProgramId programId = applicationId.program(ProgramType.SPARK, programName);
    Map<String, String> expectedTags = new HashMap<>();
    expectedTags.put(Constants.Metrics.Tag.NAMESPACE, namespaceName);
    expectedTags.put(Constants.Metrics.Tag.APP, appName);
    expectedTags.put(Constants.Metrics.Tag.PROGRAM, programName);
    expectedTags.put(Constants.Metrics.Tag.PROGRAM_TYPE, ProgramTypeMetricTag.getTagName(programId.getType()));
    Map<String, String> tags = DefaultAccessEnforcer.createEntityIdMetricsTags(programId);
    Assert.assertEquals(expectedTags, tags);
  }
}
