/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.store;

import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableSchemaIncompatibleException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Fields;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class which contains all the store definition, the table name the store will use, the schema of
 * the table should all be specified here.
 * TODO: CDAP-14674 Make sure all the store definition goes here.
 */
public final class StoreDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(StoreDefinition.class);

  private static List<StructuredTableSpecification> tableSpecifications;

  private StoreDefinition() {
    // prevent instantiation
  }

  /**
   * Create all system tables. A boolean flag can be used to skip creating tables that already
   * exist.
   *
   * @param tableAdmin the table admin to create the table
   */
  public static void createAllTables(StructuredTableAdmin tableAdmin) throws IOException {
    // Please use register() in case of multiple table creation calls.
    // Some structured table admin implementations batch these table creation calls for better performance.
    ArtifactStore.register();
    OwnerStore.register();
    NamespaceStore.register();
    SecretStore.register();
    WorkflowStore.register();
    ConfigStore.register();
    PreferencesStore.register();
    ProvisionerStore.register();
    AppMetadataStore.register();
    ProfileStore.register();
    ProgramScheduleStore.register();
    DatasetInstanceStore.register();
    DatasetTypeStore.register();
    LineageStore.register();
    JobQueueStore.register();
    TimeScheduleStore.register();
    RemoteRuntimeStore.register();
    ProgramHeartbeatStore.register();
    LogCheckpointStore.register();
    UsageStore.register();
    FieldLineageStore.register();
    LogFileMetaStore.register();
    CapabilitiesStore.register();
    TetheringStore.register();
    AppStateStore.register();
    CredentialProviderStore.register();
    OperationRunsStore.register();

    // Please ensure createRegisteredTables() is not followed by any other register calls.
    createRegisteredTables(tableAdmin);
  }

  /**
   * Performs store operations required for table creation.
   */
  private static void createRegisteredTables(StructuredTableAdmin admin) throws IOException {
    if (!tableSpecifications.isEmpty()) {
      try {
        admin.createOrUpdate(tableSpecifications);
      } catch (TableSchemaIncompatibleException e) {
        throw new IllegalStateException(
            "Table already exists with an incompatible schema", e);
      }
      tableSpecifications.clear();
    }
  }

  /**
   * Adds table to list of tables to be created.
   */
  private static void registerTable(StructuredTableSpecification spec) {
    if (tableSpecifications == null) {
      tableSpecifications = new ArrayList<>();
    }
    tableSpecifications.add(spec);
  }

  /**
   * Namespace store schema.
   */
  public static final class NamespaceStore {

    public static final StructuredTableId NAMESPACES = new StructuredTableId("namespaces");
    public static final StructuredTableId REPOSITORIES = new StructuredTableId("repositories");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String NAMESPACE_METADATA_FIELD = "namespace_metadata";
    public static final String REPOSITORY_CONFIGURATION_FIELD = "config";
    public static final String UPDATE_TIME = "updatedtimemillis";

    public static final StructuredTableSpecification NAMESPACE_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(NAMESPACES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(NAMESPACE_METADATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD)
            .build();

    public static final StructuredTableSpecification REPOSITORY_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(REPOSITORIES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(REPOSITORY_CONFIGURATION_FIELD),
                Fields.longType(UPDATE_TIME))
            .withPrimaryKeys(NAMESPACE_FIELD)
            .build();

    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }

    public static void register() {
      registerTable(NAMESPACE_TABLE_SPEC);
      registerTable(REPOSITORY_TABLE_SPEC);
    }
  }

  /**
   * Schema for ConfigStore.
   */
  public static final class ConfigStore {

    public static final StructuredTableId CONFIGS = new StructuredTableId("configs");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String PROPERTIES_FIELD = "properties";

    public static final StructuredTableSpecification CONFIG_TABLE_SPEC = new StructuredTableSpecification.Builder()
        .withId(CONFIGS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(TYPE_FIELD),
            Fields.stringType(NAME_FIELD),
            Fields.stringType(PROPERTIES_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, TYPE_FIELD, NAME_FIELD)
        .build();

    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }

    public static void register() {
      registerTable(CONFIG_TABLE_SPEC);
    }
  }

  /**
   * Schema for PreferencesStore.
   */
  public static final class PreferencesStore {

    public static final StructuredTableId PREFERENCES = new StructuredTableId("preferences");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String PROPERTIES_FIELD = "properties";
    public static final String SEQUENCE_ID_FIELD = "seq";

    public static final StructuredTableSpecification PREFERENCES_TABLE_SPEC = new StructuredTableSpecification.Builder()
        .withId(PREFERENCES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(TYPE_FIELD),
            Fields.stringType(NAME_FIELD),
            Fields.stringType(PROPERTIES_FIELD),
            Fields.longType(SEQUENCE_ID_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, TYPE_FIELD, NAME_FIELD)
        .build();

    public static void register() {
      registerTable(PREFERENCES_TABLE_SPEC);
    }

    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for workflow table.
   */
  public static final class WorkflowStore {

    public static final StructuredTableId WORKFLOW_STATISTICS = new StructuredTableId(
        "workflow_statistics");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_FIELD = "program";
    public static final String START_TIME_FIELD = "start_time";
    public static final String RUN_ID_FIELD = "run_id";
    public static final String TIME_TAKEN_FIELD = "time_taken";
    public static final String PROGRAM_RUN_DATA = "program_run_data";

    public static final StructuredTableSpecification WORKFLOW_TABLE_SPEC = new StructuredTableSpecification.Builder()
        .withId(WORKFLOW_STATISTICS)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(APPLICATION_FIELD),
            Fields.stringType(VERSION_FIELD),
            Fields.stringType(PROGRAM_FIELD),
            Fields.longType(START_TIME_FIELD),
            Fields.stringType(RUN_ID_FIELD),
            Fields.longType(TIME_TAKEN_FIELD),
            Fields.stringType(PROGRAM_RUN_DATA))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_FIELD,
            START_TIME_FIELD)
        .build();

    public static void register() {
      registerTable(WORKFLOW_TABLE_SPEC);
    }

    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for ArtifactStore.
   */
  public static final class ArtifactStore {

    public static final StructuredTableId ARTIFACT_DATA_TABLE = new StructuredTableId(
        "artifact_data");
    public static final StructuredTableId APP_DATA_TABLE = new StructuredTableId("app_data");
    public static final StructuredTableId PLUGIN_DATA_TABLE = new StructuredTableId("plugin_data");
    public static final StructuredTableId UNIV_PLUGIN_DATA_TABLE = new StructuredTableId(
        "universal_plugin_data");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String ARTIFACT_NAMESPACE_FIELD = "artifact_namespace";
    public static final String ARTIFACT_NAME_FIELD = "artifact_name";
    public static final String ARTIFACT_VER_FIELD = "artifiact_version";
    public static final String ARTIFACT_DATA_FIELD = "artifact_data";
    public static final String CLASS_NAME_FIELD = "class_name";
    public static final String APP_DATA_FIELD = "app_data";
    public static final String PARENT_NAMESPACE_FIELD = "parent_namespace";
    public static final String PARENT_NAME_FIELD = "parent_name";
    public static final String PLUGIN_TYPE_FIELD = "plugin_type";
    public static final String PLUGIN_NAME_FIELD = "plugin_name";
    public static final String PLUGIN_DATA_FIELD = "plugin_data";

    // Artifact Data table
    public static final StructuredTableSpecification ARTIFACT_DATA_SPEC = new StructuredTableSpecification.Builder()
        .withId(ARTIFACT_DATA_TABLE)
        .withFields(Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
            Fields.stringType(ARTIFACT_NAME_FIELD),
            Fields.stringType(ARTIFACT_VER_FIELD),
            Fields.stringType(ARTIFACT_DATA_FIELD))
        .withPrimaryKeys(ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
        .build();

    // App Data table
    public static final StructuredTableSpecification APP_DATA_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(APP_DATA_TABLE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(CLASS_NAME_FIELD),
                Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                Fields.stringType(ARTIFACT_NAME_FIELD),
                Fields.stringType(ARTIFACT_VER_FIELD),
                Fields.stringType(APP_DATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, CLASS_NAME_FIELD, ARTIFACT_NAMESPACE_FIELD,
                ARTIFACT_NAME_FIELD,
                ARTIFACT_VER_FIELD)
            .build();

    // Plugin Data table
    public static final StructuredTableSpecification PLUGIN_DATA_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PLUGIN_DATA_TABLE)
            .withFields(Fields.stringType(PARENT_NAMESPACE_FIELD),
                Fields.stringType(PARENT_NAME_FIELD),
                Fields.stringType(PLUGIN_TYPE_FIELD),
                Fields.stringType(PLUGIN_NAME_FIELD),
                Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                Fields.stringType(ARTIFACT_NAME_FIELD),
                Fields.stringType(ARTIFACT_VER_FIELD),
                Fields.stringType(PLUGIN_DATA_FIELD))
            .withPrimaryKeys(PARENT_NAMESPACE_FIELD, PARENT_NAME_FIELD, PLUGIN_TYPE_FIELD,
                PLUGIN_NAME_FIELD,
                ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
            .build();

    // Universal Plugin Data table
    public static final StructuredTableSpecification UNIV_PLUGIN_DATA_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(UNIV_PLUGIN_DATA_TABLE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(PLUGIN_TYPE_FIELD),
                Fields.stringType(PLUGIN_NAME_FIELD),
                Fields.stringType(ARTIFACT_NAMESPACE_FIELD),
                Fields.stringType(ARTIFACT_NAME_FIELD),
                Fields.stringType(ARTIFACT_VER_FIELD),
                Fields.stringType(PLUGIN_DATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, PLUGIN_TYPE_FIELD, PLUGIN_NAME_FIELD,
                ARTIFACT_NAMESPACE_FIELD, ARTIFACT_NAME_FIELD, ARTIFACT_VER_FIELD)
            .build();

    public static void register() {
      registerTable(ARTIFACT_DATA_SPEC);
      registerTable(APP_DATA_SPEC);
      registerTable(PLUGIN_DATA_SPEC);
      registerTable(UNIV_PLUGIN_DATA_SPEC);
    }

    /**
     * Creates artifact store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Table specification and create table definitions for owner store.
   */
  public static final class OwnerStore {

    public static final StructuredTableId OWNER_TABLE = new StructuredTableId("owner_data");
    public static final String PRINCIPAL_FIELD = "principal";
    public static final String KEYTAB_FIELD = "keytab";

    public static final StructuredTableSpecification OWNER_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(OWNER_TABLE)
            .withFields(Fields.stringType(PRINCIPAL_FIELD),
                Fields.bytesType(KEYTAB_FIELD))
            .withPrimaryKeys(PRINCIPAL_FIELD).build();

    public static void register() {
      registerTable(OWNER_TABLE_SPEC);
    }

    /**
     * Creates owner store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for {@link SecretStore}.
   */
  public static final class SecretStore {

    public static final StructuredTableId SECRET_STORE_TABLE = new StructuredTableId(
        "secret_store");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String SECRET_NAME_FIELD = "secret_name";
    public static final String SECRET_DATA_FIELD = "secret_data";

    public static final StructuredTableSpecification SECRET_STORE_SPEC = new StructuredTableSpecification.Builder()
        .withId(SECRET_STORE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(SECRET_NAME_FIELD),
            Fields.bytesType(SECRET_DATA_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, SECRET_NAME_FIELD)
        .build();

    public static void register() {
      registerTable(SECRET_STORE_SPEC);
    }

    /**
     * Creates secret store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Table specification and create table definitions for provisioner store.
   */
  public static final class ProvisionerStore {

    public static final StructuredTableId PROVISIONER_TABLE = new StructuredTableId(
        "provisioner_data");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String KEY_TYPE = "type";
    public static final String PROVISIONER_TASK_INFO_FIELD = "provisioner_task_info";

    public static final StructuredTableSpecification PROVISIONER_STORE_SPEC = new StructuredTableSpecification.Builder()
        .withId(PROVISIONER_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(APPLICATION_FIELD),
            Fields.stringType(VERSION_FIELD),
            Fields.stringType(PROGRAM_TYPE_FIELD),
            Fields.stringType(PROGRAM_FIELD),
            Fields.stringType(RUN_FIELD),
            Fields.stringType(KEY_TYPE),
            Fields.stringType(PROVISIONER_TASK_INFO_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD,
            PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD, KEY_TYPE)
        .build();

    public static void register() {
      registerTable(PROVISIONER_STORE_SPEC);
    }

    /**
     * Creates provisioner store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Defines schema for AppMetadata tables.
   */
  public static final class AppMetadataStore {

    public static final StructuredTableId APPLICATION_SPECIFICATIONS = new StructuredTableId(
        "application_specs");
    public static final StructuredTableId APPLICATION_EDIT = new StructuredTableId(
        "application_edit");
    public static final StructuredTableId WORKFLOW_NODE_STATES = new StructuredTableId(
        "workflow_node_states");
    public static final StructuredTableId RUN_RECORDS = new StructuredTableId("run_records");
    public static final StructuredTableId WORKFLOWS = new StructuredTableId("workflows");
    public static final StructuredTableId PROGRAM_COUNTS = new StructuredTableId("program_counts");
    // TODO: CDAP-14876 Move this table into it's own store, along with associated methods
    public static final StructuredTableId SUBSCRIBER_STATES = new StructuredTableId(
        "subscriber_state");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String EDIT_NUM_FIELD = "edit_num";
    public static final String APPLICATION_DATA_FIELD = "application_data";
    public static final String CREATION_TIME_FIELD = "created";
    public static final String CHANGE_SUMMARY_FIELD = "change_summary";
    public static final String AUTHOR_FIELD = "author";
    public static final String LATEST_FIELD = "latest";
    public static final String SOURCE_CONTROL_META = "source_control_metadata";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String NODE_ID = "node_id";
    public static final String NODE_STATE_DATA = "node_state_data";
    public static final String RUN_STATUS = "run_status";
    public static final String RUN_START_TIME = "run_start_time";
    public static final String RUN_RECORD_DATA = "run_record_data";
    public static final String FLOW_CONTROL_STATUS = "flow_control_status";
    public static final String WORKFLOW_DATA = "workflow_data";
    public static final String COUNT_TYPE = "count_type";
    public static final String COUNTS = "counts";
    public static final String SUBSCRIBER_TOPIC = "subscriber_topic";
    public static final String SUBSCRIBER_MESSAGE = "subscriber_message";
    public static final String SUBSCRIBER = "subscriber";


    public static final StructuredTableSpecification APPLICATION_SPECIFICATIONS_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(APPLICATION_SPECIFICATIONS)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(APPLICATION_DATA_FIELD),
                Fields.longType(CREATION_TIME_FIELD),
                Fields.stringType(AUTHOR_FIELD),
                Fields.stringType(CHANGE_SUMMARY_FIELD),
                Fields.booleanType(LATEST_FIELD),
                Fields.stringType(SOURCE_CONTROL_META))
            .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD)
            .withIndexes(LATEST_FIELD, CREATION_TIME_FIELD)
            .build();

    // The table that stores the edit# of an application. It provides:
    // 1. Keys on namespace + application name to prevent the race of multiple first application versions
    // 2. Store the number of edits for an application
    public static final StructuredTableSpecification APPLICATION_EDIT_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(APPLICATION_EDIT)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.intType(EDIT_NUM_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD)
            .build();

    public static final StructuredTableSpecification WORKFLOW_NODE_STATES_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(WORKFLOW_NODE_STATES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(PROGRAM_TYPE_FIELD),
                Fields.stringType(PROGRAM_FIELD),
                Fields.stringType(RUN_FIELD),
                Fields.stringType(NODE_ID),
                Fields.stringType(NODE_STATE_DATA))
            .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD,
                PROGRAM_FIELD,
                RUN_FIELD, NODE_ID)
            .build();

    public static final StructuredTableSpecification RUN_RECORDS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(RUN_RECORDS)
            .withFields(Fields.stringType(RUN_STATUS),
                Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(PROGRAM_TYPE_FIELD),
                Fields.stringType(PROGRAM_FIELD),
                Fields.longType(RUN_START_TIME),
                Fields.stringType(RUN_FIELD),
                Fields.stringType(FLOW_CONTROL_STATUS),
                Fields.stringType(RUN_RECORD_DATA))
            .withPrimaryKeys(RUN_STATUS, NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD,
                PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_START_TIME, RUN_FIELD)
            .withIndexes(FLOW_CONTROL_STATUS)
            .build();

    public static final StructuredTableSpecification WORKFLOWS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(WORKFLOWS)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(PROGRAM_TYPE_FIELD),
                Fields.stringType(PROGRAM_FIELD),
                Fields.stringType(RUN_FIELD),
                Fields.stringType(WORKFLOW_DATA))
            .withPrimaryKeys(
                NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD,
                PROGRAM_FIELD, RUN_FIELD)
            .build();

    public static final StructuredTableSpecification PROGRAM_COUNTS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PROGRAM_COUNTS)
            .withFields(Fields.stringType(COUNT_TYPE),
                Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(PROGRAM_TYPE_FIELD),
                Fields.stringType(PROGRAM_FIELD),
                Fields.longType(COUNTS))
            .withPrimaryKeys(
                COUNT_TYPE, NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD,
                PROGRAM_FIELD)
            .build();

    public static final StructuredTableSpecification SUBSCRIBER_STATE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(SUBSCRIBER_STATES)
            .withFields(Fields.stringType(SUBSCRIBER_TOPIC),
                Fields.stringType(SUBSCRIBER),
                Fields.stringType(SUBSCRIBER_MESSAGE))
            .withPrimaryKeys(SUBSCRIBER_TOPIC, SUBSCRIBER)
            .build();

    public static void register() {
      registerTable(APPLICATION_SPECIFICATIONS_TABLE_SPEC);
      registerTable(APPLICATION_EDIT_TABLE_SPEC);
      registerTable(WORKFLOW_NODE_STATES_SPEC);
      registerTable(RUN_RECORDS_SPEC);
      registerTable(WORKFLOWS_SPEC);
      registerTable(PROGRAM_COUNTS_SPEC);
      registerTable(SUBSCRIBER_STATE_SPEC);
    }

    /**
     * Creates app metadata store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Dataset instance store schema.
   */
  public static final class DatasetInstanceStore {

    public static final StructuredTableId DATASET_INSTANCES =
        new StructuredTableId("dataset_instances");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String DATASET_FIELD = "dataset";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_INSTANCES_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(DATASET_INSTANCES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(DATASET_FIELD),
                Fields.stringType(DATASET_METADATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, DATASET_FIELD)
            .build();

    public static void register() {
      registerTable(DATASET_INSTANCES_SPEC);
    }

    /**
     * Creates dataset instance store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Table schema for profile store.
   */
  public static final class ProfileStore {

    public static final StructuredTableId PROFILE_STORE_TABLE =
        new StructuredTableId("profile_store");
    public static final StructuredTableId PROFILE_ENTITY_STORE_TABLE =
        new StructuredTableId("profile_entity_store");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String PROFILE_ID_FIELD = "profile_id";
    public static final String ENTITY_ID_FIELD = "entity_id";
    public static final String PROFILE_DATA_FIELD = "profile_data";
    public static final String ENTITY_DATA_FIELD = "entity_data";

    public static final StructuredTableSpecification PROFILE_STORE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PROFILE_STORE_TABLE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(PROFILE_ID_FIELD),
                Fields.stringType(PROFILE_DATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_ID_FIELD)
            .build();

    public static final StructuredTableSpecification PROFILE_ENTITY_STORE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PROFILE_ENTITY_STORE_TABLE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(PROFILE_ID_FIELD),
                Fields.stringType(ENTITY_ID_FIELD),
                Fields.stringType(ENTITY_DATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_ID_FIELD, ENTITY_ID_FIELD)
            .build();

    public static void register() {
      registerTable(PROFILE_STORE_SPEC);
      registerTable(PROFILE_ENTITY_STORE_SPEC);
    }

    /**
     * Creates profile store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Table schema for program schedule store.
   */
  public static final class ProgramScheduleStore {

    public static final StructuredTableId PROGRAM_SCHEDULE_TABLE =
        new StructuredTableId("program_schedule_store");
    public static final StructuredTableId PROGRAM_TRIGGER_TABLE =
        new StructuredTableId("program_trigger_store");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String SCHEDULE_NAME = "schedule_name";
    public static final String SEQUENCE_ID = "sequence_id";
    public static final String SCHEDULE = "schedule";
    public static final String UPDATE_TIME = "update_time";
    public static final String STATUS = "status";
    public static final String TRIGGER_KEY = "trigger_key";


    public static final StructuredTableSpecification PROGRAM_SCHEDULE_STORE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PROGRAM_SCHEDULE_TABLE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(SCHEDULE_NAME),
                Fields.stringType(SCHEDULE),
                Fields.longType(UPDATE_TIME),
                Fields.stringType(STATUS))
            .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, SCHEDULE_NAME)
            .build();

    public static final StructuredTableSpecification PROGRAM_TRIGGER_STORE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PROGRAM_TRIGGER_TABLE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(VERSION_FIELD),
                Fields.stringType(SCHEDULE_NAME),
                Fields.intType(SEQUENCE_ID),
                Fields.stringType(TRIGGER_KEY))
            .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, SCHEDULE_NAME,
                SEQUENCE_ID)
            .withIndexes(TRIGGER_KEY)
            .build();

    public static void register() {
      registerTable(PROGRAM_SCHEDULE_STORE_SPEC);
      registerTable(PROGRAM_TRIGGER_STORE_SPEC);
    }

    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Dataset type store schema.
   */
  public static final class DatasetTypeStore {

    public static final StructuredTableId DATASET_TYPES = new StructuredTableId("dataset_types");
    public static final StructuredTableId MODULE_TYPES = new StructuredTableId("module_types");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String MODULE_NAME_FIELD = "module_name";
    public static final String TYPE_NAME_FIELD = "type_name";
    public static final String DATASET_METADATA_FIELD = "dataset_metadata";

    public static final StructuredTableSpecification DATASET_TYPES_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(DATASET_TYPES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(TYPE_NAME_FIELD),
                Fields.stringType(DATASET_METADATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, TYPE_NAME_FIELD)
            .build();
    public static final StructuredTableSpecification MODULE_TYPES_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(MODULE_TYPES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(MODULE_NAME_FIELD),
                Fields.stringType(DATASET_METADATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, MODULE_NAME_FIELD)
            .build();

    public static void register() {
      registerTable(DATASET_TYPES_SPEC);
      registerTable(MODULE_TYPES_SPEC);
    }

    /**
     * Creates dataset type store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for lineage table.
   */
  public static final class LineageStore {

    public static final StructuredTableId DATASET_LINEAGE_TABLE = new StructuredTableId(
        "dataset_lineage");
    public static final StructuredTableId PROGRAM_LINEAGE_TABLE = new StructuredTableId(
        "program_lineage");
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String DATASET_FIELD = "dataset";
    public static final String START_TIME_FIELD = "start_time";
    public static final String PROGRAM_NAMESPACE_FIELD = "program_namespace";
    public static final String PROGRAM_APPLICATION_FIELD = "program_application";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String ACCESS_TYPE_FIELD = "access_type";
    public static final String ACCESS_TIME_FIELD = "access_time";

    public static final StructuredTableSpecification DATASET_LINEAGE_SPEC = new StructuredTableSpecification.Builder()
        .withId(DATASET_LINEAGE_TABLE)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(DATASET_FIELD),
            Fields.longType(START_TIME_FIELD),
            Fields.stringType(PROGRAM_NAMESPACE_FIELD),
            Fields.stringType(PROGRAM_APPLICATION_FIELD),
            Fields.stringType(PROGRAM_TYPE_FIELD),
            Fields.stringType(PROGRAM_FIELD),
            Fields.stringType(RUN_FIELD),
            Fields.stringType(ACCESS_TYPE_FIELD),
            Fields.longType(ACCESS_TIME_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, DATASET_FIELD, START_TIME_FIELD, PROGRAM_NAMESPACE_FIELD,
            PROGRAM_APPLICATION_FIELD,
            PROGRAM_TYPE_FIELD, PROGRAM_FIELD, RUN_FIELD, ACCESS_TYPE_FIELD)
        .build();

    public static final StructuredTableSpecification PROGRAM_LINEAGE_SPEC = new StructuredTableSpecification.Builder()
        .withId(PROGRAM_LINEAGE_TABLE)
        .withFields(Fields.stringType(PROGRAM_NAMESPACE_FIELD),
            Fields.stringType(PROGRAM_APPLICATION_FIELD),
            Fields.stringType(PROGRAM_TYPE_FIELD),
            Fields.stringType(PROGRAM_FIELD),
            Fields.longType(START_TIME_FIELD),
            Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(DATASET_FIELD),
            Fields.stringType(RUN_FIELD),
            Fields.stringType(ACCESS_TYPE_FIELD),
            Fields.longType(ACCESS_TIME_FIELD))
        .withPrimaryKeys(PROGRAM_NAMESPACE_FIELD, PROGRAM_APPLICATION_FIELD, PROGRAM_TYPE_FIELD,
            PROGRAM_FIELD,
            START_TIME_FIELD, NAMESPACE_FIELD, DATASET_FIELD, RUN_FIELD, ACCESS_TYPE_FIELD)
        .build();

    public static void register() {
      registerTable(DATASET_LINEAGE_SPEC);
      registerTable(PROGRAM_LINEAGE_SPEC);
    }

    /**
     * Creates lineage tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Table schema for job queue.
   */
  public static final class JobQueueStore {

    public static final StructuredTableId JOB_QUEUE_TABLE =
        new StructuredTableId("job_queue_store");

    public static final String PARTITION_ID = "partition_id";
    public static final String SCHEDULE_ID = "schedule_id";
    public static final String GENERATION_ID = "generation_id";
    public static final String ROW_TYPE = "row_type";
    public static final String JOB = "job";
    public static final String DELETE_TIME = "delete_time";
    public static final String OBSOLETE_TIME = "obsolete_time";

    /**
     * Specifies the type of the data in a row. This is used as part of the primary key
     */
    public enum RowType {
      JOB, // row contains the serialized job
      DELETE, // if the job is marked for deletion, the row contains the time when the job was marked for deletion
      OBSOLETE // if the job has timed out, the row contains the time when the job was marked as obsolete
    }

    public static final StructuredTableSpecification JOB_QUEUE_STORE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(JOB_QUEUE_TABLE)
            .withFields(Fields.intType(PARTITION_ID),
                Fields.stringType(SCHEDULE_ID),
                Fields.intType(GENERATION_ID),
                Fields.stringType(ROW_TYPE),
                Fields.stringType(JOB),
                Fields.longType(DELETE_TIME),
                Fields.longType(OBSOLETE_TIME))
            .withPrimaryKeys(PARTITION_ID, SCHEDULE_ID, GENERATION_ID, ROW_TYPE)
            .build();

    public static void register() {
      registerTable(JOB_QUEUE_STORE_SPEC);
    }

    /**
     * Creates job queue store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for time schedules.
   */
  public static final class TimeScheduleStore {

    public static final StructuredTableId SCHEDULES = new StructuredTableId("schedules");

    public static final String TYPE_FIELD = "type";
    public static final String NAME_FIELD = "name";
    public static final String VALUE_FIELD = "value";

    public static final StructuredTableSpecification SCHEDULES_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(SCHEDULES)
            .withFields(Fields.stringType(TYPE_FIELD),
                Fields.stringType(NAME_FIELD),
                Fields.bytesType(VALUE_FIELD))
            .withPrimaryKeys(TYPE_FIELD, NAME_FIELD)
            .build();

    public static void register() {
      registerTable(SCHEDULES_SPEC);
    }

    /**
     * Creates time schedule store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for remote runtime.
   */
  public static final class RemoteRuntimeStore {

    public static final StructuredTableId RUNTIMES = new StructuredTableId("runtimes");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String VERSION_FIELD = "version";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String PROGRAM_OPTIONS_FIELD = "program_options";

    public static final StructuredTableSpecification RUNTIMES_SPEC = new StructuredTableSpecification.Builder()
        .withId(RUNTIMES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(APPLICATION_FIELD),
            Fields.stringType(VERSION_FIELD),
            Fields.stringType(PROGRAM_TYPE_FIELD),
            Fields.stringType(PROGRAM_FIELD),
            Fields.stringType(RUN_FIELD),
            Fields.stringType(PROGRAM_OPTIONS_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, VERSION_FIELD, PROGRAM_TYPE_FIELD,
            PROGRAM_FIELD, RUN_FIELD)
        .build();

    public static void register() {
      registerTable(RUNTIMES_SPEC);
    }

    /**
     * Creates remote runtime store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for program heartbeat.
   */
  public static final class ProgramHeartbeatStore {

    public static final StructuredTableId PROGRAM_HEARTBEATS = new StructuredTableId(
        "program_heartbeats");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TIMESTAMP_SECONDS_FIELD = "timestamp";
    public static final String APPLICATION_FIELD = "application";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String RUN_FIELD = "run";
    public static final String RUN_RECORD = "run_record";

    public static final StructuredTableSpecification PROGRAM_HEARTBEATS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(PROGRAM_HEARTBEATS)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.longType(TIMESTAMP_SECONDS_FIELD),
                Fields.stringType(APPLICATION_FIELD),
                Fields.stringType(PROGRAM_TYPE_FIELD),
                Fields.stringType(PROGRAM_FIELD),
                Fields.stringType(RUN_FIELD),
                Fields.stringType(RUN_RECORD))
            .withPrimaryKeys(
                NAMESPACE_FIELD, TIMESTAMP_SECONDS_FIELD, APPLICATION_FIELD, PROGRAM_TYPE_FIELD,
                PROGRAM_FIELD, RUN_FIELD)
            .build();

    public static void register() {
      registerTable(PROGRAM_HEARTBEATS_SPEC);
    }

    /**
     * Creates program heartbeat store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for log checkpoint store.
   */
  public static final class LogCheckpointStore {

    public static final StructuredTableId LOG_CHECKPOINT_TABLE = new StructuredTableId(
        "log_checkpoints");
    public static final String ROW_PREFIX_FIELD = "prefix";
    public static final String PARTITION_FIELD = "partition";
    public static final String CHECKPOINT_FIELD = "checkpoint";

    public static final StructuredTableSpecification LOG_CHECKPOINT_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(LOG_CHECKPOINT_TABLE)
            .withFields(Fields.stringType(ROW_PREFIX_FIELD),
                Fields.intType(PARTITION_FIELD),
                Fields.bytesType(CHECKPOINT_FIELD))
            .withPrimaryKeys(ROW_PREFIX_FIELD, PARTITION_FIELD)
            .build();

    public static void register() {
      registerTable(LOG_CHECKPOINT_TABLE_SPEC);
    }

    /**
     * Creates log checkpoint store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for usage table.
   */
  public static final class UsageStore {

    public static final StructuredTableId USAGES = new StructuredTableId("usages");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APPLICATION_FIELD = "application";
    public static final String PROGRAM_TYPE_FIELD = "program_type";
    public static final String PROGRAM_FIELD = "program";
    public static final String DATASET_FIELD = "dataset";
    public static final String INDEX_FIELD = "index";

    public static final StructuredTableSpecification USAGES_SPEC = new StructuredTableSpecification.Builder()
        .withId(USAGES)
        .withFields(Fields.stringType(NAMESPACE_FIELD),
            Fields.stringType(APPLICATION_FIELD),
            Fields.stringType(PROGRAM_TYPE_FIELD),
            Fields.stringType(PROGRAM_FIELD),
            Fields.stringType(DATASET_FIELD),
            Fields.stringType(INDEX_FIELD))
        .withPrimaryKeys(NAMESPACE_FIELD, APPLICATION_FIELD, PROGRAM_TYPE_FIELD, PROGRAM_FIELD,
            DATASET_FIELD)
        .withIndexes(INDEX_FIELD)
        .build();

    public static void register() {
      registerTable(USAGES_SPEC);
    }

    /**
     * Creates usage store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for field lineage.
   *
   * <p>Endpoint checksum table is used to store endpoints/properties of endpoints to a checksum.
   * Checksum can then be used the query the other tables. Also contains the program run info for
   * that checksum.
   *
   * <p>The remaining tables store various endpoint data keyed by checksum.
   */
  public static final class FieldLineageStore {

    public static final StructuredTableId ENDPOINT_CHECKSUM_TABLE = new StructuredTableId(
        "fields_table");
    public static final StructuredTableId OPERATIONS_TABLE = new StructuredTableId(
        "operations_table");
    public static final StructuredTableId DESTINATION_FIELDS_TABLE = new StructuredTableId(
        "destination_fields_table");
    public static final StructuredTableId SUMMARY_FIELDS_TABLE = new StructuredTableId(
        "summary_fields_table");

    public static final String DIRECTION_FIELD = "direction";
    public static final String ENDPOINT_NAMESPACE_FIELD = "endpoint_namespace";
    public static final String ENDPOINT_NAME_FIELD = "endpoint";
    public static final String ENDPOINT_PROPERTIES_FIELD = "endpoint_properties";
    public static final String START_TIME_FIELD = "start_time";
    public static final String CHECKSUM_FIELD = "checksum";
    public static final String PROGRAM_RUN_FIELD = "program_run";
    public static final String OPERATIONS_FIELD = "operations";
    public static final String DESTINATION_DATA_FIELD = "destination_data";
    public static final String ENDPOINT_FIELD = "endpoint_field";

    public static final StructuredTableSpecification ENDPOINT_CHECKSUM_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(ENDPOINT_CHECKSUM_TABLE)
            .withFields(Fields.stringType(DIRECTION_FIELD),
                Fields.stringType(ENDPOINT_NAMESPACE_FIELD),
                Fields.stringType(ENDPOINT_NAME_FIELD),
                Fields.stringType(ENDPOINT_PROPERTIES_FIELD),
                Fields.longType(START_TIME_FIELD),
                Fields.longType(CHECKSUM_FIELD),
                Fields.stringType(PROGRAM_RUN_FIELD))
            .withPrimaryKeys(DIRECTION_FIELD, ENDPOINT_NAMESPACE_FIELD, ENDPOINT_NAME_FIELD,
                START_TIME_FIELD)
            .build();
    public static final StructuredTableSpecification OPERATIONS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(OPERATIONS_TABLE)
            .withFields(Fields.longType(CHECKSUM_FIELD),
                Fields.stringType(OPERATIONS_FIELD))
            .withPrimaryKeys(CHECKSUM_FIELD)
            .build();
    public static final StructuredTableSpecification DESTINATION_FIELDS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(DESTINATION_FIELDS_TABLE)
            .withFields(Fields.longType(CHECKSUM_FIELD),
                Fields.stringType(ENDPOINT_NAMESPACE_FIELD),
                Fields.stringType(ENDPOINT_NAME_FIELD),
                Fields.stringType(DESTINATION_DATA_FIELD))
            .withPrimaryKeys(CHECKSUM_FIELD, ENDPOINT_NAMESPACE_FIELD, ENDPOINT_NAME_FIELD)
            .build();
    public static final StructuredTableSpecification SUMMARY_FIELDS_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(SUMMARY_FIELDS_TABLE)
            .withFields(Fields.longType(CHECKSUM_FIELD),
                Fields.stringType(DIRECTION_FIELD),
                Fields.stringType(ENDPOINT_NAMESPACE_FIELD),
                Fields.stringType(ENDPOINT_NAME_FIELD),
                Fields.stringType(ENDPOINT_FIELD),
                Fields.stringType(DESTINATION_DATA_FIELD))
            .withPrimaryKeys(CHECKSUM_FIELD, DIRECTION_FIELD, ENDPOINT_NAMESPACE_FIELD,
                ENDPOINT_NAME_FIELD,
                ENDPOINT_FIELD)
            .build();

    public static void register() {
      registerTable(ENDPOINT_CHECKSUM_SPEC);
      registerTable(OPERATIONS_SPEC);
      registerTable(DESTINATION_FIELDS_SPEC);
      registerTable(SUMMARY_FIELDS_SPEC);
    }

    /**
     * Creates field lineage store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for log file meta.
   */
  public static final class LogFileMetaStore {

    public static final StructuredTableId LOG_FILE_META = new StructuredTableId("logfile_meta");

    public static final String LOGGING_CONTEXT_FIELD = "logging_context";
    public static final String EVENT_TIME_FIELD = "event_time";
    public static final String CREATION_TIME_FIELD = "creation_time";
    public static final String FILE_FIELD = "file";

    public static final StructuredTableSpecification LOG_FILE_META_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(LOG_FILE_META)
            .withFields(Fields.stringType(LOGGING_CONTEXT_FIELD),
                Fields.longType(EVENT_TIME_FIELD),
                Fields.longType(CREATION_TIME_FIELD),
                Fields.stringType(FILE_FIELD))
            .withPrimaryKeys(LOGGING_CONTEXT_FIELD, EVENT_TIME_FIELD, CREATION_TIME_FIELD).build();

    public static void register() {
      registerTable(LOG_FILE_META_SPEC);
    }

    /**
     * Creates logfile meta store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for Capabilities.
   */
  public static final class CapabilitiesStore {

    public static final StructuredTableId CAPABILITIES = new StructuredTableId("capabilities");
    public static final StructuredTableId CAPABILITY_OPERATIONS = new StructuredTableId(
        "capability_operations");

    public static final String NAME_FIELD = "name";
    public static final String STATUS_FIELD = "status";
    public static final String ACTION_FIELD = "action";
    public static final String CONFIG_FIELD = "config";
    public static final String UPDATED_TIME_FIELD = "updated_time";

    public static final StructuredTableSpecification CAPABILITIES_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(CAPABILITIES)
            .withFields(Fields.stringType(NAME_FIELD),
                Fields.stringType(STATUS_FIELD),
                Fields.stringType(CONFIG_FIELD),
                Fields.longType(UPDATED_TIME_FIELD))
            .withPrimaryKeys(NAME_FIELD)
            .build();

    public static final StructuredTableSpecification CAPABILITY_OPERATIONS_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(CAPABILITY_OPERATIONS)
            .withFields(Fields.stringType(NAME_FIELD),
                Fields.stringType(ACTION_FIELD),
                Fields.stringType(CONFIG_FIELD))
            .withPrimaryKeys(NAME_FIELD)
            .build();

    public static void register() {
      registerTable(CAPABILITIES_TABLE_SPEC);
      registerTable(CAPABILITY_OPERATIONS_TABLE_SPEC);
    }

    /**
     * Creates capabilities store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for tethering.
   */
  public static final class TetheringStore {

    public static final StructuredTableId TETHERING = new StructuredTableId("tethering");

    public static final String PEER_NAME_FIELD = "name";
    public static final String PEER_URI_FIELD = "uri";
    public static final String TETHERING_STATE_FIELD = "state";
    public static final String REQUEST_TIME_FIELD = "request_time";
    public static final String LAST_CONNECTION_TIME_FIELD = "last_connection_time";
    public static final String PEER_METADATA_FIELD = "metadata";

    public static final StructuredTableSpecification TETHERING_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(TETHERING)
            .withFields(Fields.stringType(PEER_NAME_FIELD),
                Fields.stringType(PEER_URI_FIELD),
                Fields.stringType(TETHERING_STATE_FIELD),
                Fields.longType(REQUEST_TIME_FIELD),
                Fields.longType(LAST_CONNECTION_TIME_FIELD),
                Fields.stringType(PEER_METADATA_FIELD))
            .withPrimaryKeys(PEER_NAME_FIELD)
            .build();

    public static void register() {
      registerTable(TETHERING_TABLE_SPEC);
    }

    /**
     * Creates tethering store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schema for app state.
   */
  public static final class AppStateStore {

    public static final StructuredTableId APP_STATE = new StructuredTableId("state");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String APP_NAME_FIELD = "app_name";
    public static final String STATE_KEY_FIELD = "state_key";
    public static final String STATE_VALUE_FIELD = "state_value";

    public static final StructuredTableSpecification STATE_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(APP_STATE)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(APP_NAME_FIELD),
                Fields.stringType(STATE_KEY_FIELD),
                Fields.bytesType(STATE_VALUE_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, APP_NAME_FIELD, STATE_KEY_FIELD)
            .build();

    public static void register() {
      registerTable(STATE_TABLE_SPEC);
    }

    /**
     * Creates app state store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schemas for credential providers.
   */
  public static final class CredentialProviderStore {

    public static final StructuredTableId CREDENTIAL_PROFILES =
        new StructuredTableId("credential_profiles");
    public static final StructuredTableId CREDENTIAL_IDENTITIES =
        new StructuredTableId("credential_identities");

    public static final String NAMESPACE_FIELD = "namespace";
    public static final String PROFILE_NAME_FIELD = "profile_name";
    public static final String PROFILE_DATA_FIELD = "profile_data";
    public static final String IDENTITY_NAME_FIELD = "identity_name";
    public static final String IDENTITY_PROFILE_INDEX_FIELD = "profile_index";
    public static final String IDENTITY_DATA_FIELD = "identity_data";

    public static final StructuredTableSpecification PROFILE_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(CREDENTIAL_PROFILES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(PROFILE_NAME_FIELD),
                Fields.bytesType(PROFILE_DATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, PROFILE_NAME_FIELD)
            .build();

    public static final StructuredTableSpecification IDENTITY_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(CREDENTIAL_IDENTITIES)
            .withFields(Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(IDENTITY_NAME_FIELD),
                Fields.stringType(IDENTITY_PROFILE_INDEX_FIELD),
                Fields.bytesType(IDENTITY_DATA_FIELD))
            .withPrimaryKeys(NAMESPACE_FIELD, IDENTITY_NAME_FIELD)
            .withIndexes(IDENTITY_PROFILE_INDEX_FIELD)
            .build();

    public static void register() {
      registerTable(PROFILE_TABLE_SPEC);
      registerTable(IDENTITY_TABLE_SPEC);
    }

    /**
     * Creates credential provider store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }

  /**
   * Schemas for operation runs.
   */
  public static final class OperationRunsStore {

    public static final StructuredTableId OPERATION_RUNS =
        new StructuredTableId("operation_runs");

    public static final String ID_FIELD = "id";
    public static final String NAMESPACE_FIELD = "namespace";
    public static final String TYPE_FIELD = "type";
    public static final String STATUS_FIELD = "status";
    public static final String START_TIME_FIELD = "start_time";
    public static final String UPDATE_TIME_FIELD = "update_time";
    // contains serialized OperationRunDetail
    public static final String DETAILS_FIELD = "details";
    public static final StructuredTableSpecification OPERATION_RUNS_TABLE_SPEC =
        new StructuredTableSpecification.Builder()
            .withId(OPERATION_RUNS)
            .withFields(
                Fields.stringType(NAMESPACE_FIELD),
                Fields.stringType(ID_FIELD),
                Fields.stringType(TYPE_FIELD),
                Fields.stringType(STATUS_FIELD),
                Fields.longType(START_TIME_FIELD),
                Fields.longType(UPDATE_TIME_FIELD),
                Fields.stringType(DETAILS_FIELD)
            )
            .withPrimaryKeys(NAMESPACE_FIELD, ID_FIELD)
            .withIndexes(TYPE_FIELD, STATUS_FIELD, START_TIME_FIELD)
            .build();

    public static void register() {
      registerTable(OPERATION_RUNS_TABLE_SPEC);
    }

    /**
     * Creates operation store tables.
     *
     * @param tableAdmin The table admin to use.
     * @throws IOException If table creation fails.
     */
    public static void create(StructuredTableAdmin tableAdmin) throws IOException {
      register();
      createRegisteredTables(tableAdmin);
    }
  }
}
