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

package co.cask.cdap.data2.dataset2.module.lib.leveldb;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.data2.dataset.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBMetricsTableDefinition;

/**
 * Registers LevelDB-based implementations of the metrics system datasets
 */
public class LevelDBMetricsTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new LevelDBMetricsTableDefinition(LevelDBMetricsTable.class.getName()));
    registry.add(new LevelDBMetricsTableDefinition(MetricsTable.class.getName()));
  }
}