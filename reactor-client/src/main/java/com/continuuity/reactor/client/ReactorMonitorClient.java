/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.reactor.client;

import com.continuuity.common.http.HttpMethod;
import com.continuuity.common.http.HttpRequest;
import com.continuuity.common.http.HttpResponse;
import com.continuuity.common.http.ObjectResponse;
import com.continuuity.reactor.client.config.ReactorClientConfig;
import com.continuuity.reactor.client.exception.BadRequestException;
import com.continuuity.reactor.client.exception.NotFoundException;
import com.continuuity.reactor.client.exception.ServiceNotEnabledException;
import com.continuuity.reactor.client.util.RestClient;
import com.continuuity.reactor.metadata.Instances;
import com.continuuity.reactor.metadata.SystemServiceMeta;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

/**
 * Provides ways to monitor Reactor.
 */
public class ReactorMonitorClient {

  private static final Gson GSON = new Gson();

  private final RestClient restClient;
  private final ReactorClientConfig config;

  @Inject
  public ReactorMonitorClient(ReactorClientConfig config) {
    this.config = config;
    this.restClient = RestClient.create(config);
  }

  /**
   * Lists all system services.
   *
   * @return list of {@link SystemServiceMeta}s.
   * @throws IOException if a network error occurred
   */
  public List<SystemServiceMeta> listSystemServices() throws IOException {
    URL url = config.resolveURL("system/services");
    HttpResponse response = restClient.execute(HttpMethod.GET, url);
    return ObjectResponse.fromJsonBody(response, new TypeToken<List<SystemServiceMeta>>() { }).getResponseObject();
  }

  /**
   * Gets the status of a system service.
   *
   * @param serviceName Name of the system service
   * @return status of the system service
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the system service with the specified name could not be found
   * @throws BadRequestException if the operation was not valid for the system service
   */
  public String getSystemServiceStatus(String serviceName) throws IOException, NotFoundException, BadRequestException {
    URL url = config.resolveURL(String.format("system/services/%s/status", serviceName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    String responseBody = new String(response.getResponseBody());
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("system service", serviceName);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(responseBody);
    }
    return responseBody;
  }

  /**
   * Gets the status of all system services.
   *
   * @return map from service name to service status
   * @throws IOException if a network error occurred
   */
  public Map<String, String> getAllSystemServiceStatus() throws IOException {
    URL url = config.resolveURL("system/services/status");
    HttpResponse response = restClient.execute(HttpMethod.GET, url);
    return ObjectResponse.fromJsonBody(response, new TypeToken<Map<String, String>>() { }).getResponseObject();
  }

  public void setSystemServiceInstances(String serviceName, int instances)
    throws IOException, NotFoundException, BadRequestException {

    URL url = config.resolveURL(String.format("system/services/%s/instances", serviceName));
    HttpRequest request = HttpRequest.put(url).withBody(GSON.toJson(new Instances(instances))).build();
    HttpResponse response = restClient.execute(request, HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_BAD_REQUEST);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("system service", serviceName);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
      throw new BadRequestException(new String(response.getResponseBody()));
    }
  }

  /**
   * Gets the number of instances the system service is running on.
   *
   * @param serviceName name of the system service
   * @return number of instances the system service is running on
   * @throws IOException if a network error occurred
   * @throws NotFoundException if the system service with the specified name was not found
   * @throws ServiceNotEnabledException if the system service is not currently enabled
   */
  public int getSystemServiceInstances(String serviceName)
    throws IOException, NotFoundException, ServiceNotEnabledException {

    URL url = config.resolveURL(String.format("system/services/%s/instances", serviceName));
    HttpResponse response = restClient.execute(HttpMethod.GET, url, HttpURLConnection.HTTP_NOT_FOUND,
                                               HttpURLConnection.HTTP_FORBIDDEN);
    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new NotFoundException("system service", serviceName);
    } else if (response.getResponseCode() == HttpURLConnection.HTTP_FORBIDDEN) {
      throw new ServiceNotEnabledException(serviceName);
    }

    return ObjectResponse.fromJsonBody(response, Instances.class).getResponseObject().getInstances();
  }

}
