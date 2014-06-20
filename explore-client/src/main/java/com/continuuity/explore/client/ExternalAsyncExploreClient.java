package com.continuuity.explore.client;

import java.net.InetSocketAddress;

/**
 * An Explore Client that uses the provided host and port to talk to a server
 * implementing {@link com.continuuity.explore.service.Explore} over HTTP.
 */
public class ExternalAsyncExploreClient extends AbstractAsyncExploreClient {

  private final InetSocketAddress addr;

  public ExternalAsyncExploreClient(String host, int port) {
    addr = InetSocketAddress.createUnresolved(host, port);
  }

  @Override
  protected InetSocketAddress getExploreServiceAddress() {
    return addr;
  }
}