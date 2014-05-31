package com.continuuity.hive.client;

import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * Hive client using beeline with thrift.
 */
public class HiveCommandExecutor extends AbstractBeelineCommandExecutor {

  @Inject
  public HiveCommandExecutor(DiscoveryServiceClient discoveryClient) {
    super(discoveryClient);
  }

  @Override
  protected String getConnectionPostfix() {
    return "/default;auth=noSasl?" +
           "hive.exec.pre.hooks=com.continuuity.hive.hooks.TransactionPreHook;" +
           "hive.exec.post.hooks=com.continuuity.hive.hooks.TransactionPostHook";
  }
}