package com.continuuity.explore.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.hive.client.guice.HiveClientModule;
import com.continuuity.hive.guice.HiveRuntimeModule;
import com.continuuity.internal.data.dataset.DatasetAdmin;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.continuuity.explore.service.KeyStructValueTableDefinition.KeyValue;

/**
 *
 */
public class HiveExploreServiceTest {
  private static InMemoryTransactionManager transactionManager;
  private static DatasetFramework datasetFramework;
  private static DatasetService datasetService;
  private static HiveExploreService hiveExploreService;

  @BeforeClass
  public static void start() throws Exception {
    Injector injector = Guice.createInjector(createInMemoryModules(CConfiguration.create(), new Configuration()));
    transactionManager = injector.getInstance(InMemoryTransactionManager.class);
    transactionManager.startAndWait();

    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();

    hiveExploreService = injector.getInstance(HiveExploreService.class);
    hiveExploreService.start();

    datasetFramework = injector.getInstance(DatasetFramework.class);
    String moduleName = "inMemory";
    datasetFramework.register(moduleName, InMemoryTableModule.class);
    datasetFramework.register("keyValue", KeyStructValueTableDefinition.KeyStructValueTableModule.class);

    // Performing admin operations to create dataset instance
    datasetFramework.addInstance("keyValueTable", "my_table", DatasetInstanceProperties.EMPTY);
    DatasetAdmin admin = datasetFramework.getAdmin("my_table", null);
    Assert.assertNotNull(admin);
    admin.create();

    Transaction tx1 = transactionManager.startShort(100);

    // Accessing dataset instance to perform data operations
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    table.startTx(tx1);

    KeyValue.Value value1 = new KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5));
    KeyValue.Value value2 = new KeyValue.Value("two", Lists.newArrayList(10, 11, 12, 13, 14));
    table.put("1", value1);
    table.put("2", value2);
    Assert.assertEquals(value1, table.get("1"));

    Assert.assertTrue(table.commitTx());

    transactionManager.canCommit(tx1, table.getTxChanges());
    transactionManager.commit(tx1);

    table.postTxCommit();

    Transaction tx2 = transactionManager.startShort(100);
    table.startTx(tx2);

    Assert.assertEquals(value1, table.get("1"));
  }

  @AfterClass
  public static void stop() throws Exception {
    hiveExploreService.stop();
    transactionManager.stopAndWait();
    datasetService.startAndWait();
  }

  @Test
  public void testTable() throws Exception {
    KeyStructValueTableDefinition.KeyStructValueTable table = datasetFramework.getDataset("my_table", null);
    Assert.assertNotNull(table);
    Transaction tx = transactionManager.startShort(100);
    table.startTx(tx);
    Assert.assertEquals(new KeyValue.Value("first", Lists.newArrayList(1, 2, 3, 4, 5)), table.get("1"));
    transactionManager.abort(tx);
  }

  @Test
  public void testHiveIntegration() throws Exception {
    runCommand("drop table if exists kv_table",
               false,
               ImmutableList.<ColumnDesc>of(),
               ImmutableList.<Row>of());

    runCommand("create external table kv_table (key STRING, value struct<name:string,ints:array<int>>) " +
                 "stored by 'com.continuuity.hive.datasets.DatasetStorageHandler' " +
                 "with serdeproperties (\"reactor.dataset.name\"=\"my_table\")",
               false,
               ImmutableList.<ColumnDesc>of(),
               ImmutableList.<Row>of());

    runCommand("show tables",
               true,
               Lists.newArrayList(new ColumnDesc("tab_name", "STRING", 1, "from deserializer")),
               Lists.newArrayList(new Row(Lists.<Object>newArrayList("kv_table"))));

    runCommand("describe kv_table",
               true,
               Lists.newArrayList(
                 new ColumnDesc("col_name", "STRING", 1, "from deserializer"),
                 new ColumnDesc("data_type", "STRING", 2, "from deserializer"),
                 new ColumnDesc("comment", "STRING", 3, "from deserializer")
               ),
               Lists.newArrayList(
                 new Row(Lists.<Object>newArrayList("key", "string", "from deserializer")),
                 new Row(Lists.<Object>newArrayList("value", "struct<name:string,ints:array<int>>",
                                                    "from deserializer"))
               ));

    runCommand("select key, value from kv_table",
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new Row(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new Row(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}"))));

    runCommand("select * from kv_table",
               true,
               Lists.newArrayList(new ColumnDesc("key", "STRING", 1, null),
                                  new ColumnDesc("value", "STRING", 2, null)),
               Lists.newArrayList(
                 new Row(Lists.<Object>newArrayList("1", "{\"name\":\"first\",\"ints\":[1,2,3,4,5]}")),
                 new Row(Lists.<Object>newArrayList("2", "{\"name\":\"two\",\"ints\":[10,11,12,13,14]}"))));

    runCommand("drop table if exists kv_table",
               false,
               ImmutableList.<ColumnDesc>of(),
               ImmutableList.<Row>of());
  }

  @Test
  public void testCancel() throws Exception {
    Handle handle = hiveExploreService.execute("select key, value from kv_table");
    Status status = hiveExploreService.cancel(handle);
    Assert.assertEquals(Status.State.CANCELED, status.getState());
  }

  private static void runCommand(String command, boolean expectedHasResult,
                                 List<ColumnDesc> expectedColumnDescs, List<Row> expectedRows) throws Exception {
    Handle handle = hiveExploreService.execute(command);

    Status status;
    do {
      TimeUnit.MILLISECONDS.sleep(200);
      status = hiveExploreService.getStatus(handle);
    } while (status.getState() == Status.State.RUNNING || status.getState() == Status.State.PENDING);

    Assert.assertEquals(Status.State.FINISHED, status.getState());
    Assert.assertEquals(expectedHasResult, status.hasResults());

    Assert.assertEquals(expectedColumnDescs, hiveExploreService.getResultSchema(handle));
    Assert.assertEquals(expectedRows, trimColumnValues(hiveExploreService.nextResults(handle, 100)));

    hiveExploreService.close(handle);
  }

  private static List<Row> trimColumnValues(List<Row> rows) {
    List<Row> newRows = Lists.newArrayList();
    for (Row row : rows) {
      List<Object> newCols = Lists.newArrayList();
      for (Object obj : row.getColumns()) {
        if (obj instanceof String) {
          newCols.add(((String) obj).trim());
        } else {
          newCols.add(obj);
        }
      }
      newRows.add(new Row(newCols));
    }
    return newRows;
  }

  private static List<Module> createInMemoryModules(CConfiguration configuration, Configuration hConf) {
    configuration.set(Constants.CFG_DATA_INMEMORY_PERSISTENCE, Constants.InMemoryPersistenceType.MEMORY.name());

    return ImmutableList.of(
      new ConfigModule(configuration, hConf),
      new IOModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModule(),
      new DataFabricModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new AuthModule(),
      new HiveRuntimeModule().getInMemoryModules(),
      new HiveClientModule()
    );
  }
}