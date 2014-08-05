/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.datafabric;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;

import javax.annotation.Nullable;

/**
 * Reactor's dataset namespace.
 */
public class ReactorDatasetNamespace implements DatasetNamespace {
  public static final String DEFAULT_TABLE_PREFIX = "continuuity";
  public static final String CFG_TABLE_PREFIX = "data.table.prefix";
  private final String namespacePrefix;
  private final Namespace namespace;

  public ReactorDatasetNamespace(CConfiguration conf, Namespace namespace) {
    String reactorNameSpace = conf.get(CFG_TABLE_PREFIX, DEFAULT_TABLE_PREFIX);
    this.namespacePrefix = reactorNameSpace + ".";
    this.namespace = namespace;
  }

  @Override
  public String namespace(String name) {
    return namespacePrefix + namespace.namespace(name);
  }

  @Override
  @Nullable
  public String fromNamespaced(String name) {
    if (!name.startsWith(namespacePrefix)) {
      return null;
    }
    // will return null if doesn't belong to namespace
    return namespace.fromNamespaced(name.substring(namespacePrefix.length()));
  }
}