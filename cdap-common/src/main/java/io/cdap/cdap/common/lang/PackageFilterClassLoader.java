/*
 * Copyright © 2015 Cask Data, Inc.
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

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A {@link ClassLoader} that filter class based on package name. Classes in the bootstrap
 * ClassLoader is always loadable from this ClassLoader.
 */
public class PackageFilterClassLoader extends ClassLoader implements Closeable {

  private final Predicate<String> predicate;
  private final URLClassLoader bootstrapClassLoader;

  /**
   * Constructs a new instance that only allow class's package name passes the given {@link
   * Predicate}.
   */
  public PackageFilterClassLoader(ClassLoader parent, Predicate<String> predicate) {
    super(parent);
    this.predicate = predicate;
    // There is no reliable way to get bootstrap ClassLoader from Java (System.class.getClassLoader() may return null).
    // A URLClassLoader with no URLs and with a null parent will load class from bootstrap ClassLoader only.
    this.bootstrapClassLoader = new URLClassLoader(new URL[0], null);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    try {
      return bootstrapClassLoader.loadClass(name);
    } catch (ClassNotFoundException e) {
      if (!predicate.test(getClassPackage(name))) {
        throw new ClassNotFoundException("Loading of class " + name + " not allowed");
      }

      return super.loadClass(name, resolve);
    }
  }

  @Override
  public URL getResource(String name) {
    URL resource = bootstrapClassLoader.getResource(name);
    if (resource != null) {
      return resource;
    }

    if (name.endsWith(".class") && !predicate.test(getResourcePackage(name))) {
      return null;
    }
    return super.getResource(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> resources = bootstrapClassLoader.getResources(name);
    if (resources.hasMoreElements()) {
      return resources;
    }
    if (name.endsWith(".class") && !predicate.test(getResourcePackage(name))) {
      return Collections.emptyEnumeration();
    }
    return super.getResources(name);
  }

  @Override
  protected Package[] getPackages() {
    List<Package> packages = new ArrayList<>();
    for (Package pkg : super.getPackages()) {
      if (predicate.test(pkg.getName())) {
        packages.add(pkg);
      }
    }
    return packages.toArray(new Package[0]);
  }

  @Override
  protected Package getPackage(String name) {
    if (!predicate.test(name)) {
      return null;
    }
    return super.getPackage(name);
  }

  /**
   * Returns the package of the given class or {@code null} if the class is in default package.
   *
   * @param className fully qualified name of the class
   */
  @Nullable
  private String getClassPackage(String className) {
    int idx = className.lastIndexOf('.');
    return idx < 0 ? null : className.substring(0, idx);
  }

  /**
   * Returns the package name of the given resource name representing a class.
   *
   * @param classResource Resource name of the class.
   */
  private String getResourcePackage(String classResource) {
    String packageName = classResource.substring(0, classResource.length() - ".class".length())
        .replace('/', '.');
    if (packageName.startsWith("/")) {
      return packageName.substring(1);
    }
    return packageName;
  }

  @Override
  public void close() throws IOException {
    this.bootstrapClassLoader.close();
  }
}
