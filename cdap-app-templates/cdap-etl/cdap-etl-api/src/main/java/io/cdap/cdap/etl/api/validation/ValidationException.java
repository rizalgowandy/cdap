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

package io.cdap.cdap.etl.api.validation;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.FailureDetailsProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Validation exception that carries multiple validation failures.
 */
@Beta
public class ValidationException extends RuntimeException implements FailureDetailsProvider {

  private final List<ValidationFailure> failures;

  /**
   * Creates a validation exception with list of failures.
   *
   * @param failures list of validation failures
   */
  public ValidationException(List<ValidationFailure> failures) {
    super(generateMessage(failures));
    this.failures = Collections.unmodifiableList(new ArrayList<>(failures));
  }

  /**
   * Returns a list of validation failures.
   */
  public List<ValidationFailure> getFailures() {
    return failures;
  }

  private static String generateMessage(List<ValidationFailure> failures) {
    return String.format("Errors were encountered during validation. %s",
        failures.isEmpty() ? "" : failures.iterator().next().getMessage());
  }

  @Override
  public String getErrorReason() {
    return String.format("Stage '%s' encountered %s validation failures.", getFailureStage(),
        failures.size());
  }

  @Nullable
  @Override
  public String getFailureStage() {
    return !failures.isEmpty() ? failures.get(0).getStageName() : null;
  }

  @Override
  public ErrorCategory getErrorCategory() {
    return new ErrorCategory(ErrorCategoryEnum.PLUGIN, "Validation");
  }

  @Override
  public ErrorType getErrorType() {
    return ErrorType.USER;
  }
}
