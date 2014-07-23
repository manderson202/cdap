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

package com.continuuity.gateway.handlers;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramRuntimeService;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.UserErrors;
import com.continuuity.internal.UserMessages;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.distributed.Containers;
import com.continuuity.internal.app.runtime.service.LiveInfo;
import com.continuuity.internal.app.runtime.service.NotRunningLiveInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.inject.Inject;
import org.apache.twill.api.RuntimeSpecification;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 *  Handler class for User services.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ServiceHttpHandler extends AbstractAppFabricHttpHandler {

  private final Store store;
  private final ProgramRuntimeService runtimeService;

  private static final Logger LOG = LoggerFactory.getLogger(ServiceHttpHandler.class);

  @Inject
  public ServiceHttpHandler(Authenticator authenticator, StoreFactory storeFactory,
                            ProgramRuntimeService runtimeService) {
    super(authenticator);
    this.store = storeFactory.create();
    this.runtimeService = runtimeService;
  }

  /**
   * Return the list of user twill apps for an application.
   */
  @Path("/apps/{app-id}/services")
  @GET
  public void listServices(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      ApplicationSpecification spec = store.getApplication(Id.Application.from(accountId, appId));
      if (spec != null) {
        JsonArray services = new JsonArray();
        for (Map.Entry<String, ServiceSpecification> entry : spec.getServices().entrySet()) {
          JsonObject service = new JsonObject();
          service.addProperty("type", Type.SERVICE.prettyName());
          service.addProperty("app", appId);
          service.addProperty("id", entry.getValue().getName());
          service.addProperty("name", entry.getValue().getName());
          service.addProperty("description", entry.getValue().getDescription());
          services.add(service);
        }
        responder.sendJson(HttpResponseStatus.OK, services);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return information for all user twill apps for an application.
   */
  @GET
  @Path("/apps/{app-id}/services-info")
  public void infoServices(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      ApplicationSpecification spec = store.getApplication(Id.Application.from(accountId, appId));
      if (spec != null) {
        JsonArray services = new JsonArray();
        for (Map.Entry<String, ServiceSpecification> entry : spec.getServices().entrySet()) {
          JsonObject service = new JsonObject();
          service.addProperty("type", Type.SERVICE.prettyName());
          service.addProperty("app", appId);
          String serviceId = entry.getValue().getName();
          service.addProperty("id", serviceId);
          // service id is the same as service name
          service.addProperty("name", serviceId);
          service.addProperty("description", entry.getValue().getDescription());
          // add status of service
          final Id.Program id = Id.Program.from(accountId, appId, serviceId);
          ProgramRuntimeService.RuntimeInfo runtimeInfo =
            findRuntimeInfo(accountId, appId, serviceId, Type.SERVICE);
          // program is stopped
          if (runtimeInfo == null) {
            service.addProperty("status", ProgramController.State.STOPPED.toString());
          } else {
            service.addProperty("status", controllerStateToString(runtimeInfo.getController().getState()));
          }
          // Runnables
          JsonArray runnables = new JsonArray();
          ServiceSpecification serviceSpec = getServiceSpecification(accountId, appId, serviceId);
          if (serviceSpec != null) {
            for (Map.Entry<String, RuntimeSpecification> runnableEntry : serviceSpec.getRunnables().entrySet()) {
              JsonObject runnable = new JsonObject();
              String runnableName = runnableEntry.getKey();
              runnable.addProperty("id", runnableName);
              runnable.addProperty("requested", runnableEntry.getValue().getResourceSpecification().getInstances());
              runnable.addProperty("provisioned", getRunnableCount(accountId, appId, serviceId, runnableName));
              runnables.add(runnable);
            }
          }
          service.add("runnables", runnables);
          services.add(service);
        }
        responder.sendJson(HttpResponseStatus.OK, services);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return the service details of a given service.
   */
  @Path("/apps/{app-id}/services/{service-id}")
  @GET
  public void getService(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") String appId,
                         @PathParam("service-id") String serviceId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      ServiceSpecification spec = getServiceSpecification(accountId, appId, serviceId);
      if (spec != null) {
        JsonObject service = new JsonObject();
        service.addProperty("id", spec.getName());
        service.addProperty("name", spec.getName());
        service.addProperty("description", spec.getDescription());
        JsonArray runnables = new JsonArray();
        for (Map.Entry<String, RuntimeSpecification> entry : spec.getRunnables().entrySet()) {
          runnables.add(new JsonPrimitive(entry.getKey()));
        }
        service.add("runnables", runnables);
        responder.sendJson(HttpResponseStatus.OK, service);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Return the number of instances for the given runnable of a service.
   */
  @GET
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void getInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-id") String serviceId,
                           @PathParam("runnable-name") String runnableName) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      RuntimeSpecification specification = getRuntimeSpecification(accountId, appId, serviceId, runnableName);
      if (specification == null) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        JsonObject reply = new JsonObject();
        reply.addProperty("requested", specification.getResourceSpecification().getInstances());
        reply.addProperty("provisioned", getRunnableCount(accountId, appId, serviceId, runnableName));
        responder.sendJson(HttpResponseStatus.OK, reply);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Set instances.
   */
  @PUT
  @Path("/apps/{app-id}/services/{service-id}/runnables/{runnable-name}/instances")
  public void setInstances(HttpRequest request, HttpResponder responder,
                           @PathParam("app-id") String appId,
                           @PathParam("service-id") String serviceId,
                           @PathParam("runnable-name") String runnableName) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      Id.Program programId = Id.Program.from(accountId, appId, serviceId);

      int instances = getInstances(request);
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }

      int oldInstances = store.getServiceRunnableInstances(programId, runnableName);
      store.setServiceRunnableInstances(programId, runnableName, instances);

      ProgramRuntimeService.RuntimeInfo runtimeInfo = findRuntimeInfo(programId.getAccountId(),
                                                                      programId.getApplicationId(),
                                                                      programId.getId(),
                                                                      Type.SERVICE);
      if (runtimeInfo != null) {
        runtimeInfo.getController().command(ProgramOptionConstants.RUNNABLE_INSTANCES,
                                            ImmutableMap.of("runnable", runnableName,
                                                            "newInstances", String.valueOf(instances),
                                                            "oldInstances", String.valueOf(oldInstances))).get();
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable throwable) {
      LOG.error("Got exception : ", throwable);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/services/{service-id}/live-info")
  public void liveInfo(HttpRequest request, HttpResponder responder,
                       @PathParam("app-id") String appId,
                       @PathParam("service-id") String serviceId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      responder.sendJson(HttpResponseStatus.OK,
                         runtimeService.getLiveInfo(Id.Program.from(accountId,
                                                                    appId,
                                                                    serviceId),
                                                    Type.SERVICE));
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @Nullable
  private ServiceSpecification getServiceSpecification(String accountId, String id,
                                                       String serviceName) throws OperationException {
    ApplicationSpecification applicationSpecification = store.getApplication(Id.Application.from(accountId, id));
    Map<String, ServiceSpecification> serviceSpecs = applicationSpecification.getServices();
    if (serviceSpecs.containsKey(serviceName)) {
      return serviceSpecs.get(serviceName);
    } else {
      return null;
    }
  }

  @Nullable
  private RuntimeSpecification getRuntimeSpecification(String accountId, String appId, String serviceName,
                                                        String runnableName) throws OperationException {
    ServiceSpecification specification = getServiceSpecification(accountId, appId, serviceName);
    if (specification != null) {
      Map<String, RuntimeSpecification> runtimeSpecs =  specification.getRunnables();
      return runtimeSpecs.containsKey(runnableName) ? runtimeSpecs.get(runnableName) : null;
    } else {
      return null;
    }
  }

  private int getRunnableCount(String accountId, String appId, String serviceName, String runnable) {
    LiveInfo info = runtimeService.getLiveInfo(Id.Program.from(accountId, appId, serviceName), Type.SERVICE);
    int count = 0;
    if (info instanceof NotRunningLiveInfo) {
      return count;
    } else if (info instanceof Containers) {
      Containers containers = (Containers) info;
      for (Containers.ContainerInfo container : containers.getContainers()) {
        if (container.getName().equals(runnable)) {
          count++;
        }
      }
      return count;
    } else {
      //Not running on YARN default 1
      return 1;
    }
  }

  private ProgramRuntimeService.RuntimeInfo findRuntimeInfo(String accountId, String appId,
                                                            String flowId, Type typeId) {
    Type type = Type.valueOf(typeId.name());
    Collection<ProgramRuntimeService.RuntimeInfo> runtimeInfos = runtimeService.list(type).values();
    Preconditions.checkNotNull(runtimeInfos, UserMessages.getMessage(UserErrors.RUNTIME_INFO_NOT_FOUND),
                               accountId, flowId);

    Id.Program programId = Id.Program.from(accountId, appId, flowId);

    for (ProgramRuntimeService.RuntimeInfo info : runtimeInfos) {
      if (programId.equals(info.getProgramId())) {
        return info;
      }
    }
    return null;
  }

  /**
   * Convert controller state to appropriate string
   */
  private String controllerStateToString(ProgramController.State state) {
    if (state == ProgramController.State.ALIVE) {
      return "RUNNING";
    }
    if (state == ProgramController.State.ERROR) {
      return "FAILED";
    }
    return state.toString();
  }
}
