package com.continuuity.api.data.stream;

/**
 *  Streams are the primary means for pushing data from external systems
 *  into the AppFabric. Each individual event or signal sent to a Stream
 *  is stored as an Event, which is comprised of a body (blob of arbitrary
 *  binary data) and headers (map of strings for metadata).Within the system,
 *  Streams are identified by a Unique ID string and must be explicitly created
 *  before being used.
 */
public final class Stream {
  private final String name;
  private final long ttl;

  public Stream(final String name) {
    // TODO: remove this hardcoded default
    this(name, Long.MAX_VALUE);
  }

  public Stream(final String name, final long ttl) {
    this.name = name;
    this.ttl = ttl;
  }

 /**
  * Configures {@code Stream} by returning a {@link StreamSpecification}.
  *
  * @return Instance of {@link StreamSpecification}
  *
  */
  public StreamSpecification configure() {
    return new StreamSpecification.Builder()
      .setName(this.name)
      .setTtl(this.ttl)
      .create();
  }
}
