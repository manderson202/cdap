package com.continuuity.api.data.stream;

import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Preconditions;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;

/**
 * Specification for {@link Stream}.
 */
public final class StreamSpecification {
  private final String name;
  private final long ttl;

  private StreamSpecification(final String name, final long ttl) {
    this.name = name;
    this.ttl = ttl;
  }

  /**
   * Returns the name of the Stream.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the time to live for all events in the Stream.
   */
  public long getTtl() {
    return ttl;
  }

  /**
   * @return A {@link StreamSpecification} from a {@link StreamConfig}.
   */
  public static StreamSpecification from(StreamConfig config) {
    return new StreamSpecification(config.getName(), config.getTtl());
  }

  /**
  * {@code StreamSpecification} builder used to build specification of stream.
  */
  public static final class Builder {
    private String name;
    private long ttl;

    /**
     * Adds name parameter to Streams.
     * @param name stream name
     * @return Builder instance
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    /**
     * Adds ttl parameter to Streams.
     * @param ttl stream ttl
     * @return Builder instance
     */
    public Builder setTtl(long ttl) {
      this.ttl = ttl;
      return this;
    }

    /**
     * Create {@code StreamSpecification}.
     * @return Instance of {@code StreamSpecification}
     */
    public StreamSpecification create() {
      StreamSpecification specification = new StreamSpecification(name, ttl);
      return specification;
    }
  }
}
