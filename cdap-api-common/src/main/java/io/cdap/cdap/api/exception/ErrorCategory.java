/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.api.exception;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Class representing the category of an error.
 *
 * <p>This class is used to classify errors into different categories,
 * such as plugin errors, provisioning errors, etc.
 * </p>
 */
public class ErrorCategory {
  private final ErrorCategoryEnum errorCategory;
  @Nullable
  private final String subCategory;

  /**
   * Constructor for ErrorCategory.
   *
   * @param errorCategory The category of the error.
   */
  public ErrorCategory(ErrorCategoryEnum errorCategory) {
    this(errorCategory, null);
  }

  /**
   * Constructor for ErrorCategory.
   *
   * @param errorCategory The category of the error.
   * @param subCategory The sub-category of the error.
   */
  public ErrorCategory(ErrorCategoryEnum errorCategory, @Nullable String subCategory) {
    this.errorCategory = errorCategory;
    this.subCategory = subCategory;
  }

  /*
   * Returns the category of the error.
   */
  public String getErrorCategory() {
    return errorCategory == null ? ErrorCategoryEnum.OTHERS.toString() : subCategory == null
        ? errorCategory.toString() : String.format("%s-'%s'", errorCategory, subCategory);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ErrorCategory)) {
      return false;
    }

    ErrorCategory that = (ErrorCategory) o;
    return Objects.equals(this.errorCategory, that.errorCategory)
        && Objects.equals(this.subCategory, that.subCategory);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.errorCategory, this.subCategory);
  }

  /*
   * Returns a string representation of the error category.
   */
  @Override
  public String toString() {
    return getErrorCategory();
  }

  /**
   * Returns the parent category of the error.
   */
  public ErrorCategoryEnum getParentCategory() {
    return errorCategory == null ? ErrorCategoryEnum.OTHERS : errorCategory;
  }

  /**
   * Enum representing the different categories of errors.
   */
  public enum ErrorCategoryEnum {
    ACCESS("Access"),
    DEPROVISIONING("Deprovisioning"),
    NETWORKING("Networking"),
    MACROS("Macros"),
    OTHERS("Others"),
    PLUGIN("Plugin"),
    PROVISIONING("Provisioning"),
    SCHEDULES_AND_TRIGGERS("Schedules and Triggers"),
    STARTING("Starting");

    private final String displayName;

    ErrorCategoryEnum(String name) {
      displayName = name;
    }

    /**
     * Returns a string representation of the error category enum.
     */
    @Override
    public String toString() {
      return displayName;
    }
  }
}
