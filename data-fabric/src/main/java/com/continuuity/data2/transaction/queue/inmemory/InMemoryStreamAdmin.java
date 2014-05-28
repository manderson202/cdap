package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.Collection;

/**
 * admin for queues in memory.
 */
@Singleton
public class InMemoryStreamAdmin extends InMemoryQueueAdmin implements StreamAdmin {

  @Inject
  public InMemoryStreamAdmin(InMemoryQueueService queueService) {
    super(queueService);
  }

  @Override
  public void dropAll() throws Exception {
    queueService.resetStreams();
  }

  @Override
  public Collection<StreamConfig> getAll(String accountId) {
    return null;
  }

  @Override
  public StreamConfig getConfig(String accountId, String streamName) {
    // TODO: implement accountId
    return new StreamConfig(streamName, Long.MAX_VALUE, Long.MAX_VALUE, null, Long.MAX_VALUE);
  }
}
