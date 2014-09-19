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
package co.cask.cdap.examples.ticker.order;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.examples.ticker.data.MultiIndexedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Saves order data in a multi indexed Table.
 */
public class OrderDataSaver extends AbstractFlowlet {
  public static final byte[] TIMESTAMP_COL = Bytes.toBytes("ts");
  public static final byte[] PAYLOAD_COL = Bytes.toBytes("p");

  private static final Logger LOG = LoggerFactory.getLogger(OrderDataSaver.class);

  @UseDataSet("orderIndex")
  private MultiIndexedTable orderTable;

  @ProcessInput
  public void process(OrderRecord order) {
    Put put = new Put(order.getId());
    put.add(TIMESTAMP_COL, order.getTimestamp());
    put.add(PAYLOAD_COL, order.getPayload());
    for (Map.Entry<String, String> field : order.getFields().entrySet()) {
      put.add(Bytes.toBytes(field.getKey()), Bytes.toBytes(field.getValue()));
    }
    orderTable.put(put);
  }
}