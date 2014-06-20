package com.continuuity.explore.client;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.HttpRequests;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.explore.service.ColumnDesc;
import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Row;
import com.continuuity.explore.service.Status;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A base for an Explore Client that talks to a server implementing {@link Explore} over HTTP.
 */
public abstract class AbstractAsyncExploreClient implements Explore {
  private static final Logger LOG = LoggerFactory.getLogger(InternalAsyncExploreClient.class);
  private static final Gson GSON = new Gson();

  private static final Type MAP_TYPE_TOKEN = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type COL_DESC_LIST_TYPE = new TypeToken<List<ColumnDesc>>() { }.getType();
  private static final Type ROW_LIST_TYPE = new TypeToken<List<Row>>() { }.getType();

  @Override
  public Handle execute(String statement) throws ExploreException {
    HttpResponse response = doPost("data/queries", GSON.toJson(ImmutableMap.of("query", statement)), null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return Handle.fromId(parseResponseAsMap(response, "handle"));
    }
    throw new ExploreException("Cannot execute query. Reason: " + getDetails(response));
  }

  @Override
  public Status getStatus(Handle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/queries/%s/%s", handle.getHandle(), "status"));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, Status.class);
    }
    throw new ExploreException("Cannot get status. Reason: " + getDetails(response));
  }

  @Override
  public List<ColumnDesc> getResultSchema(Handle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doGet(String.format("data/queries/%s/%s", handle.getHandle(), "schema"));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, COL_DESC_LIST_TYPE);
    }
    throw new ExploreException("Cannot get result schema. Reason: " + getDetails(response));
  }

  @Override
  public List<Row> nextResults(Handle handle, int size) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doPost(String.format("data/queries/%s/%s", handle.getHandle(), "next"),
                                   GSON.toJson(ImmutableMap.of("size", size)), null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return parseJson(response, ROW_LIST_TYPE);
    }
    throw new ExploreException("Cannot get next results. Reason: " + getDetails(response));
  }

  @Override
  public void cancel(Handle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doPost(String.format("data/queries/%s/%s", handle.getHandle(), "cancel"), null, null);
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return;
    }
    throw new ExploreException("Cannot cancel operation. Reason: " + getDetails(response));
  }

  @Override
  public void close(Handle handle) throws ExploreException, HandleNotFoundException {
    HttpResponse response = doDelete(String.format("data/queries/%s", handle.getHandle()));
    if (HttpResponseStatus.OK.getCode() == response.getResponseCode()) {
      return;
    }
    throw new ExploreException("Cannot close operation. Reason: " + getDetails(response));
  }

  protected String parseResponseAsMap(HttpResponse response, String key) throws ExploreException {
    Map<String, String> responseMap = parseJson(response, MAP_TYPE_TOKEN);
    if (responseMap.containsKey(key)) {
      return responseMap.get(key);
    }

    String message = String.format("Cannot find key %s in server response: %s", key,
        new String(response.getResponseBody(), Charsets.UTF_8));
    LOG.error(message);
    throw new ExploreException(message);
  }

  protected <T> T parseJson(HttpResponse response, Type type) throws ExploreException {
    String responseString = new String(response.getResponseBody(), Charsets.UTF_8);
    try {
      return GSON.fromJson(responseString, type);
    } catch (JsonSyntaxException e) {
      String message = String.format("Cannot parse server response: %s", responseString);
      LOG.error(message, e);
      throw new ExploreException(message, e);
    } catch (JsonParseException e) {
      String message = String.format("Cannot parse server response as map: %s", responseString);
      LOG.error(message, e);
      throw new ExploreException(message, e);
    }
  }

  protected HttpResponse doGet(String resource) throws ExploreException {
    return doRequest(resource, "GET", null, null, null);
  }

  protected HttpResponse doPost(String resource, String body, Map<String, String> headers) throws ExploreException {
    return doRequest(resource, "POST", headers, body, null);
  }

  protected HttpResponse doDelete(String resource) throws ExploreException {
    return doRequest(resource, "DELETE", null, null, null);
  }

  private HttpResponse doRequest(String resource, String requestMethod,
                                 @Nullable Map<String, String> headers,
                                 @Nullable String body,
                                 @Nullable InputStream bodySrc) throws ExploreException {
    String resolvedUrl = resolve(resource);
    try {
      URL url = new URL(resolvedUrl);
      return HttpRequests.doRequest(requestMethod, url, headers, body, bodySrc);
    } catch (IOException e) {
      throw new ExploreException(
          String.format("Error connecting to Explore Service at %s while doing %s with headers %s and body %s",
              resolvedUrl, requestMethod,
              headers == null ? "null" : Joiner.on(",").withKeyValueSeparator("=").join(headers),
              body == null ? bodySrc : body), e);
    }
  }

  protected String getDetails(HttpResponse response) {
    return String.format("Response code: %s, message:'%s', body: '%s'",
        response.getResponseCode(), response.getResponseMessage(),
        response.getResponseBody() == null ?
            "null" : new String(response.getResponseBody(), Charsets.UTF_8));

  }

  private String resolve(String resource) {
    InetSocketAddress addr = getExploreServiceAddress();
    String url = String.format("http://%s:%s%s/%s", addr.getHostName(), addr.getPort(),
        Constants.Gateway.GATEWAY_VERSION, resource);
    LOG.trace("Explore URL = {}", url);
    return url;
  }

  protected abstract InetSocketAddress getExploreServiceAddress();
}