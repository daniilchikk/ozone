/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.client.validator;

import org.apache.hadoop.hdds.scm.XceiverClientSpi.Validator;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

public final class ResponseValidatorFactory {
  private ResponseValidatorFactory() {}

  public static List<Validator> getDefault() {
    return singletonList((request, response) -> new DefaultResponseValidator().validate(response));
  }

  public static List<Validator> createValidators(Validator validator) {
    final List<Validator> defaults = getDefault();
    final List<Validator> validators = new ArrayList<>(defaults.size() + 1);
    validators.addAll(defaults);
    validators.add(validator);
    return unmodifiableList(validators);
  }
}
