/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.proto;

import java.util.Map;

/**
 * POJO that carries dataset type and properties information for create dataset request
 */
public final class DatasetInstanceConfiguration {
  private final String typeName;
  private final Map<String, String> properties;
  private final boolean update;

  public DatasetInstanceConfiguration(String typeName, Map<String, String> properties, boolean update) {
    this.typeName = typeName;
    this.properties = properties;
    this.update = update;
  }

  public DatasetInstanceConfiguration(String typeName, Map<String, String> properties) {
    this(typeName, properties, false);
  }

  public String getTypeName() {
    return typeName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public boolean isUpdate() {
    return update;
  }
}