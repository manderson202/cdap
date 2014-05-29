package com.continuuity.data2.dataset2.executor;

import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Starts {@link DatasetOpExecutorServer} in YARN.
 *
 * TODO: Currently the DatasetOpExecutorServer (which this communicates with) is started by ReactorTwillApplication.
 * We want to start the DatasetOpExecutorServer in this class startUp(), but it's not possible currently
 * since the service relies on MetricsClientRuntimeModules which is in watchdog module.
 */
public class YarnDatasetOpExecutor extends RemoteDatasetOpExecutor {

  @Inject
  public YarnDatasetOpExecutor(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient);
  }

  @Override
  protected void startUp() throws Exception {
    // TODO: start {@link DatasetOpExecutorServer} in YARN here
  }

  @Override
  protected void shutDown() throws Exception {

  }
}
