/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.common.queue.QueueName;

import java.io.IOException;

/**
 * Interface for queue consumer.
 */
public interface QueueConsumer {

  /**
   * Returns the queue name that this consumer is working on.
   */
  QueueName getQueueName();

  /**
   * Returns the configuration of this consumer.
   * @return
   */
  ConsumerConfig getConfig();

  /**
   * Dequeue an entry from the queue.
   * @return A {@link DequeueResult}.
   */
  DequeueResult<byte[]> dequeue() throws IOException;

  /**
   * Dequeue multiple entries from the queue. The dequeue result may have less entries than the given
   * maxBatchSize, depending on how many entries in the queue.
   * @param maxBatchSize Maximum number of entries to queue.
   * @return A {@link DequeueResult}.
   */
  DequeueResult<byte[]> dequeue(int maxBatchSize) throws IOException;
}