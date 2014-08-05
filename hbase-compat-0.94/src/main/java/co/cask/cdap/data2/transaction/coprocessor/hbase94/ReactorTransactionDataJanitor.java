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

package co.cask.cdap.data2.transaction.coprocessor.hbase94;

import co.cask.cdap.data2.increment.hbase94.IncrementFilter;
import co.cask.cdap.data2.transaction.coprocessor.ReactorTransactionStateCacheSupplier;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.coprocessor.TransactionStateCache;
import com.continuuity.tephra.hbase94.coprocessor.TransactionProcessor;
import com.continuuity.tephra.hbase94.coprocessor.TransactionVisibilityFilter;
import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Implementation of the {@link com.continuuity.tephra.hbase94.coprocessor.TransactionProcessor}
 * coprocessor that uses {@link co.cask.cdap.data2.transaction.coprocessor.ReactorTransactionStateCache}
 * to automatically refresh transaction state.
 */
public class ReactorTransactionDataJanitor extends TransactionProcessor {

  @Override
  protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
    String tableName = env.getRegion().getTableDesc().getNameAsString();
    String[] parts = tableName.split("\\.", 2);
    String tableNamespace = "";
    if (parts.length > 0) {
      tableNamespace = parts[0];
    }
    return new ReactorTransactionStateCacheSupplier(tableNamespace, env.getConfiguration());
  }

  @Override
  protected Filter getTransactionFilter(Transaction tx) {
    return new TransactionVisibilityFilter(tx, ttlByFamily, allowEmptyValues, new IncrementFilter());
  }
}