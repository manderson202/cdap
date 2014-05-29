/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file.filter;

import com.continuuity.data.file.ReadFilter;

/**
 * AND multiple @{link ReadFilter}s.
 */
public final class AndReadFilter extends ReadFilter {
  private ReadFilter[] filters;

  public AndReadFilter(ReadFilter...filters) {
    this.filters = filters;
  }

  @Override
  public boolean acceptOffset(long offset, long eventTimestamp) {
    for (ReadFilter filter : filters) {
      if (!filter.acceptOffset(offset, eventTimestamp)) {
        return false;
      }
    }
    return true;
  }
}
