/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file.filter;

import com.continuuity.data.file.ReadFilter;

/**
 * {@link com.continuuity.data.file.ReadFilter} for filtering expired stream events according to TTL and current time.
 */
public class TTLReadFilter extends ReadFilter {

  /**
   * Time to live. A value of 0 indicates indefinite TTL.
   */
  private long ttl;

  public TTLReadFilter(long ttl) {
    this.ttl = ttl;
  }

  @Override
  public boolean acceptOffset(long offset, long eventTimestamp) {
    if (ttl == 0) {
      return true;
    }

    return getCurrentTime() - eventTimestamp <= ttl;
  }

  protected long getCurrentTime() {
    return System.currentTimeMillis();
  }
}
