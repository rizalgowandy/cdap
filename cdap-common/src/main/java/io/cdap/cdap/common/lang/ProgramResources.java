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

package io.cdap.cdap.common.lang;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.service.SystemServiceConfigurer;
import io.cdap.cdap.common.internal.guava.ClassPath;
import io.cdap.cdap.common.internal.guava.ClassPath.ResourceInfo;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import javax.ws.rs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to maintain list of resources that are visible to user programs.
 */
public final class ProgramResources {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramResources.class);

  private static final List<String> HADOOP_PACKAGES = ImmutableList.of("org.apache.hadoop.");
  private static final List<String> EXCLUDE_PACKAGES = ImmutableList.of("org.apache.hadoop.hbase.",
      "org.apache.hadoop.hive.");

  // Contains set of resources that are always visible to all program type.
  private static Set<String> baseResources;

  /**
   * Returns a Set of resource names that are visible through to user program.
   */
  public static synchronized Set<String> getVisibleResources() {
    if (baseResources != null) {
      return baseResources;
    }
    try {
      baseResources = createBaseResources();
    } catch (IOException e) {
      LOG.error("Failed to determine base visible resources to user program", e);
      baseResources = Collections.emptySet();
    }
    return baseResources;
  }

  /**
   * Returns a Set of resources name that are visible through the cdap-api module as well as Hadoop
   * classes. This includes all classes+resources in cdap-api plus all classes+resources that
   * cdap-api depends on (for example, sl4j, gson, etc).
   */
  private static Set<String> createBaseResources() throws IOException {
    // Everything should be traceable in the same ClassLoader of this class, which is the CDAP system ClassLoader
    ClassLoader classLoader = ProgramResources.class.getClassLoader();

    // Gather resources information for cdap-api classes
    // Add everything in cdap-api as visible resources
    // Trace dependencies for cdap-api classes
    Set<String> result = ClassPathResources.getResourcesWithDependencies(classLoader,
        Application.class);
    result.addAll(ClassPathResources.getResourcesWithDependencies(classLoader,
        SystemServiceConfigurer.class));

    // Gather resources for javax.ws.rs classes. They are not traceable from the api classes.
    ClassPathResources.getClassPathResources(classLoader, Path.class).stream()
        .map(ResourceInfo::getResourceName)
        .forEach(result::add);

    // Gather Hadoop classes and resources
    getResources(ClassPath.from(classLoader, uri -> uri.getPath().endsWith(".jar")),
        HADOOP_PACKAGES, EXCLUDE_PACKAGES, ResourceInfo::getResourceName,
        result);

    return Collections.unmodifiableSet(result);
  }

  /**
   * Finds all resources that are accessible in a given {@link ClassPath} that starts with certain
   * package prefixes. Also includes all non .class file resources in the same base URLs of those
   * classes that are accepted through the package prefixes filtering.
   *
   * Resources information presented in the result collection is transformed by the given result
   * transformation function.
   */
  private static <V, T extends Collection<V>> T getResources(ClassPath classPath,
      Iterable<String> includePackagePrefixes,
      Iterable<String> excludePackagePrefixes,
      Function<ResourceInfo, V> resultTransform,
      T result) {
    Set<URL> resourcesBaseURLs = new HashSet<>();
    // Adds all .class resources that should be included
    // Also record the base URL of those resources
    for (ClassPath.ClassInfo classInfo : classPath.getAllClasses()) {
      boolean include = false;
      for (String prefix : includePackagePrefixes) {
        if (classInfo.getName().startsWith(prefix)) {
          include = true;
          break;
        }
      }
      for (String prefix : excludePackagePrefixes) {
        if (classInfo.getName().startsWith(prefix)) {
          include = false;
          break;
        }
      }

      if (include) {
        result.add(resultTransform.apply(classInfo));
        resourcesBaseURLs.add(classInfo.baseURL());
      }
    }

    // Adds non .class resources that are in the resourceBaseURLs
    for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
      if (resourceInfo instanceof ClassPath.ClassInfo) {
        // We already processed all classes in the loop above
        continue;
      }
      // See if the resource base URL is already accepted through class filtering.
      // If it does, adds the resource name to the collection as well.
      if (resourcesBaseURLs.contains(resourceInfo.baseURL())) {
        result.add(resultTransform.apply(resourceInfo));
      }
    }

    return result;
  }

  private ProgramResources() {
  }
}
