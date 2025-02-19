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

package io.cdap.cdap.api.macro;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Class representing a macro function.
 */
public class MacroFunction implements Serializable {
  private static final long serialVersionUID = 5883638727963250169L;
  private final String functionName;
  private final List<String> arguments;

  public MacroFunction(String functionName, List<String> arguments) {
    this.functionName = functionName;
    this.arguments = arguments;
  }

  /**
   * return the function name
   *
   * @return function name
   */
  public String getFunctionName() {
    return functionName;
  }

  /**
   * return the list of arguments to the function
   *
   * @return the list of arguments
   */
  public List<String> getArguments() {
    return arguments;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MacroFunction that = (MacroFunction) o;

    return Objects.equals(functionName, that.functionName) && Objects.equals(arguments,
        that.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionName, arguments);
  }

  @Override
  public String toString() {
    return "MacroFunction{"
        + "functionName='" + functionName + '\''
        + ", arguments=" + arguments
        + '}';
  }
}
