/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream.leveldb;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data.runtime.DataFabricLevelDBModule;
import co.cask.cdap.data.runtime.TransactionMetricsModule;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStore;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateTestBase;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class LevelDBStreamConsumerStateTest extends StreamConsumerStateTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static StreamConsumerStateStoreFactory stateStoreFactory;
  private static StreamAdmin streamAdmin;

  @BeforeClass
  public static void init() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataFabricLevelDBModule(),
      new TransactionMetricsModule()
    );
    streamAdmin = injector.getInstance(StreamAdmin.class);
    stateStoreFactory = injector.getInstance(StreamConsumerStateStoreFactory.class);
  }


  @Override
  protected StreamConsumerStateStore createStateStore(StreamConfig streamConfig) throws IOException {
    return stateStoreFactory.create(streamConfig);
  }

  @Override
  protected StreamAdmin getStreamAdmin() {
    return streamAdmin;
  }
}
