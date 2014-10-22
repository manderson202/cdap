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

package co.cask.cdap.client.app;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.data.batch.RecordScannable;
import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Scannables;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.List;

/**
 *
 */
public class FakeDataset extends AbstractDataset
  implements BatchReadable<byte[], Integer>, RecordScannable<KeyValue<byte[], Integer>> {

  public static final String TYPE_NAME = "fakeType";

  private KeyValueTable table;

  public FakeDataset(String instanceName, KeyValueTable table) {
    super(instanceName, table);
    this.table = table;
  }

  public int get(byte[] key) {
    return Bytes.toInt(table.read(key));
  }

  public void put(byte[] key, int value) {
    table.write(key, Bytes.toBytes(value));
  }

  @Override
  public Type getRecordType() {
    return new TypeToken<KeyValue<byte[], Integer>>() { }.getType();
  }

  @Override
  public List<Split> getSplits() {
    return table.getSplits();
  }

  @Override
  public RecordScanner<KeyValue<byte[], Integer>> createSplitRecordScanner(Split split) {
    return Scannables.splitRecordScanner(createSplitReader(split), new RecordMaker());
  }

  @Override
  public SplitReader<byte[], Integer> createSplitReader(Split split) {
    return new Scanner(table.createSplitReader(split));
  }

  private static final class Scanner extends SplitReader<byte[], Integer> {

    private SplitReader<byte[], byte[]> reader;

    public Scanner(SplitReader<byte[], byte[]> reader) {
      this.reader = reader;
    }

    @Override
    public void initialize(Split split) throws InterruptedException {
      this.reader.initialize(split);
    }

    @Override
    public boolean nextKeyValue() throws InterruptedException {
      return this.reader.nextKeyValue();
    }

    @Override
    public byte[] getCurrentKey() throws InterruptedException {
      return this.reader.getCurrentKey();
    }

    @Override
    public void close() {
      this.reader.close();
    }

    @Override
    public Integer getCurrentValue() throws InterruptedException {
      return Bytes.toInt(this.reader.getCurrentValue());
    }
  }

  private static final class RecordMaker implements Scannables.RecordMaker<byte[], Integer, KeyValue<byte[], Integer>> {
    @Override
    public KeyValue<byte[], Integer> makeRecord(byte[] bytes, Integer integer) {
      return new KeyValue<byte[], Integer>(bytes, integer);
    }
  }
}
