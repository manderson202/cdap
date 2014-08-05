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

package co.cask.cdap.data2.dataset2.lib.table.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.hbase.AbstractHBaseDataSetAdmin;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import com.continuuity.tephra.TxConstants;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 *
 */
public class HBaseOrderedTableAdmin extends AbstractHBaseDataSetAdmin {
  public static final String PROPERTY_SPLITS = "hbase.splits";
  static final byte[] DATA_COLUMN_FAMILY = Bytes.toBytes("d");
  private static final Gson GSON = new Gson();

  private final DatasetSpecification spec;
  // todo: datasets should not depend on continuuity configuration!
  private final CConfiguration conf;

  private final LocationFactory locationFactory;

  public HBaseOrderedTableAdmin(DatasetSpecification spec,
                                Configuration hConf,
                                HBaseTableUtil tableUtil,
                                CConfiguration conf,
                                LocationFactory locationFactory) throws IOException {
    super(spec.getName(), new HBaseAdmin(hConf), tableUtil);
    this.spec = spec;
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  public void create() throws IOException {
    final byte[] name = Bytes.toBytes(HBaseTableUtil.getHBaseTableName(tableName));

    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(DATA_COLUMN_FAMILY);
    // todo: make stuff configurable
    // NOTE: we cannot limit number of versions as there's no hard limit on # of excluded from read txs
    columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);

    String ttlProp = spec.getProperties().get(TxConstants.PROPERTY_TTL);
    if (ttlProp != null) {
      int ttl = Integer.parseInt(ttlProp);
      if (ttl > 0) {
        columnDescriptor.setValue(TxConstants.PROPERTY_TTL, String.valueOf(ttl));
      }
    }

    final HTableDescriptor tableDescriptor = new HTableDescriptor(name);
    tableDescriptor.addFamily(columnDescriptor);
    CoprocessorJar coprocessorJar = createCoprocessorJar();

    for (Class<? extends Coprocessor> coprocessor : coprocessorJar.getCoprocessors()) {
      addCoprocessor(tableDescriptor, coprocessor, coprocessorJar.getJarLocation(),
                     coprocessorJar.getPriority(coprocessor));
    }

    byte[][] splits = null;
    String splitsProperty = spec.getProperty(PROPERTY_SPLITS);
    if (splitsProperty != null) {
      splits = GSON.fromJson(splitsProperty, byte[][].class);
    }

    tableUtil.createTableIfNotExists(admin, name, tableDescriptor, splits);
  }

  @Override
  protected boolean upgradeTable(HTableDescriptor tableDescriptor) {
    HColumnDescriptor columnDescriptor = tableDescriptor.getFamily(DATA_COLUMN_FAMILY);

    boolean needUpgrade = false;
    if (columnDescriptor.getMaxVersions() < Integer.MAX_VALUE) {
      columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
      needUpgrade = true;
    }
    if (tableUtil.getBloomFilter(columnDescriptor) != HBaseTableUtil.BloomType.ROW) {
      tableUtil.setBloomFilter(columnDescriptor, HBaseTableUtil.BloomType.ROW);
      needUpgrade = true;
    }
    if (spec.getProperty(TxConstants.PROPERTY_TTL) == null &&
        columnDescriptor.getValue(TxConstants.PROPERTY_TTL) != null) {
      columnDescriptor.remove(TxConstants.PROPERTY_TTL.getBytes());
      needUpgrade = true;
    } else if (spec.getProperty(TxConstants.PROPERTY_TTL) != null &&
               !spec.getProperty(TxConstants.PROPERTY_TTL).equals
                  (columnDescriptor.getValue(TxConstants.PROPERTY_TTL))) {
      columnDescriptor.setValue(TxConstants.PROPERTY_TTL, spec.getProperty(TxConstants.PROPERTY_TTL));
      needUpgrade = true;
    }

    return needUpgrade;
  }

  @Override
  protected CoprocessorJar createCoprocessorJar() throws IOException {
    if (!conf.getBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE,
                         TxConstants.DataJanitor.DEFAULT_TX_JANITOR_ENABLE)) {
      return CoprocessorJar.EMPTY;
    }

    // create the jar for the data janitor coprocessor.
    Location jarDir = locationFactory.create(conf.get(Constants.CFG_HDFS_LIB_DIR));
    Class<? extends Coprocessor> dataJanitorClass = tableUtil.getTransactionDataJanitorClassForVersion();
    Class<? extends Coprocessor> incrementClass = tableUtil.getIncrementHandlerClassForVersion();
    ImmutableList<Class<? extends Coprocessor>> coprocessors =
      ImmutableList.<Class<? extends Coprocessor>>of(dataJanitorClass, incrementClass);
    Location jarFile = HBaseTableUtil.createCoProcessorJar("table", jarDir, coprocessors);
    CoprocessorJar cpJar = new CoprocessorJar(coprocessors, jarFile);
    // TODO: this is hacky, the priority should come from the CP implementation
    cpJar.setPriority(dataJanitorClass, 1);
    cpJar.setPriority(incrementClass, 2);
    return cpJar;
  }
}