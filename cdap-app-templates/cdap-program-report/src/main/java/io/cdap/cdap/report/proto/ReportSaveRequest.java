/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.report.proto;

/**
 * Represents a request to save a program run report in an HTTP request.
 */
public class ReportSaveRequest {

  private final String name;
  private final String description;

  public ReportSaveRequest(String name, String description) {
    this.name = name;
    this.description = description;
  }

  /**
   * @return the name of the report
   */
  public String getName() {
    return name;
  }

  /**
   * @return the description of the report
   */
  public String getDescription() {
    return description;
  }
}
