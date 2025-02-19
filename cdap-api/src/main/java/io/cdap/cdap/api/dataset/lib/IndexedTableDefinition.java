/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package io.cdap.cdap.api.dataset.lib;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.IncompatibleUpdateException;
import io.cdap.cdap.api.dataset.table.Table;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * DatasetDefinition for {@link IndexedTable}.
 */
@Beta
public class IndexedTableDefinition
    extends CompositeDatasetDefinition<IndexedTable> {

  public IndexedTableDefinition(String name, DatasetDefinition<? extends Table, ?> tableDef) {
    super(name, "d", tableDef, "i", tableDef);
  }

  @Override
  public DatasetSpecification configure(String instanceName, DatasetProperties properties) {
    // Dynamic indexing allows indexes to be specified at runtime
    boolean dynamicIndexing = Boolean.parseBoolean(
        properties.getProperties().get(IndexedTable.DYNAMIC_INDEXING));
    if (!dynamicIndexing) {
      String columnNamesToIndex = properties.getProperties()
          .get(IndexedTable.INDEX_COLUMNS_CONF_KEY);
      if (columnNamesToIndex == null) {
        throw new IllegalArgumentException("columnsToIndex must be specified");
      }
    }
    return super.configure(instanceName, properties);
  }

  @Override
  public DatasetSpecification reconfigure(String instanceName,
      DatasetProperties newProperties,
      DatasetSpecification currentSpec) throws IncompatibleUpdateException {
    boolean dynamicIndexing = Boolean.parseBoolean(
        newProperties.getProperties().get(IndexedTable.DYNAMIC_INDEXING));
    boolean oldDynamicIndexing = Boolean.parseBoolean(
        currentSpec.getProperty(IndexedTable.DYNAMIC_INDEXING));
    if (dynamicIndexing != oldDynamicIndexing) {
      // dynamic indexing property cannot change
      throw new IncompatibleUpdateException(
          String.format("Attempt to change dynamic indexing from '%s' to '%s'",
              oldDynamicIndexing, dynamicIndexing));
    }
    if (!dynamicIndexing) {
      // validate that the columns to index property is not null and the same as before
      String columnNamesToIndex = newProperties.getProperties()
          .get(IndexedTable.INDEX_COLUMNS_CONF_KEY);
      if (columnNamesToIndex == null) {
        throw new IllegalArgumentException("columnsToIndex must be specified");
      }
      String oldColumnsToIndex = currentSpec.getProperty(IndexedTable.INDEX_COLUMNS_CONF_KEY);
      if (!columnNamesToIndex.equals(oldColumnsToIndex)) {
        Set<byte[]> newColumns = parseColumns(columnNamesToIndex);
        Set<byte[]> oldColumns = parseColumns(oldColumnsToIndex);
        if (!newColumns.equals(oldColumns)) {
          throw new IncompatibleUpdateException(
              String.format("Attempt to change columns to index from '%s' to '%s'",
                  oldColumnsToIndex, columnNamesToIndex));
        }
      }
    }
    return super.reconfigure(instanceName, newProperties, currentSpec);
  }

  @Override
  public IndexedTable getDataset(DatasetContext datasetContext, DatasetSpecification spec,
      Map<String, String> arguments, ClassLoader classLoader) throws IOException {

    SortedSet<byte[]> columnsToIndex;
    String keyPrefix = null;
    boolean dynamicIndexing = Boolean.parseBoolean(spec.getProperty(IndexedTable.DYNAMIC_INDEXING));
    if (dynamicIndexing) {
      // If dynamic indexing is enabled, get the index columns from the runtime arguments
      String columnsToIndexProp = arguments.get(IndexedTable.INDEX_COLUMNS_CONF_KEY);
      if (columnsToIndexProp == null) {
        throw new IllegalArgumentException(
            "columnsToIndex must be specified in runtime arguments when dynamic indexing is enabled");
      }
      columnsToIndex = parseColumns(columnsToIndexProp);

      // Dynamic indexing also needs a key prefix
      keyPrefix = arguments.get(IndexedTable.DYNAMIC_INDEXING_PREFIX);
      if (keyPrefix == null) {
        throw new IllegalArgumentException(
            "When dynamic indexing is used, the indexing prefix has to be specified");
      }
    } else {
      columnsToIndex = parseColumns(spec.getProperty(IndexedTable.INDEX_COLUMNS_CONF_KEY));
    }

    Table table = getDataset(datasetContext, "d", spec, arguments, classLoader);
    Table index = getDataset(datasetContext, "i", spec, arguments, classLoader);

    return new IndexedTable(spec.getName(), table, index, columnsToIndex,
        keyPrefix == null ? Bytes.EMPTY_BYTE_ARRAY : Bytes.toBytes(keyPrefix));
  }

  /**
   * Helper method to parse a list of column names, comma-separated.
   */
  private SortedSet<byte[]> parseColumns(String value) {
    // TODO: add support for setting index key delimiter
    SortedSet<byte[]> columnsToIndex = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    for (String column : value.split(",")) {
      columnsToIndex.add(Bytes.toBytes(column));
    }
    return columnsToIndex;
  }
}
