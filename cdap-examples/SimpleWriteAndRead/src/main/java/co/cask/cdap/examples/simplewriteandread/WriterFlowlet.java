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
package co.cask.cdap.examples.simplewriteandread;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.examples.simplewriteandread.SimpleWriteAndReadFlow.KeyAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer Flowlet.
 */
public class WriterFlowlet extends AbstractFlowlet {
  private static Logger logger = LoggerFactory.getLogger(WriterFlowlet.class);
  @UseDataSet(SimpleWriteAndRead.TABLE_NAME)
  KeyValueTable kvTable;
  private OutputEmitter<byte[]> output;

  @ProcessInput
  public void process(KeyAndValue kv) {
    logger.debug(this.getContext().getName() + ": Received KeyValue " + kv);

    this.kvTable.write(
      Bytes.toBytes(kv.getKey()), Bytes.toBytes(kv.getValue()));

    output.emit(Bytes.toBytes(kv.getKey()));
  }
}