/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.workflow.WorkflowStatus;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.spark.SparkProgramRunner;
import co.cask.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * Core of Workflow engine that drives the execution of Workflow.
 */
final class WorkflowDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);

  private final Program program;
  private final RunId runId;
  private final InetAddress hostname;
  private final Map<String, String> runtimeArgs;
  private final WorkflowSpecification workflowSpec;
  private final long logicalStartTime;
  private final MapReduceRunnerFactory mrRunnerFactory;
  private final SparkRunnerFactory sparkRunnerFactory;
  private NettyHttpService httpService;
  private volatile boolean running;
  private volatile WorkflowStatus workflowStatus;

  WorkflowDriver(Program program, RunId runId, ProgramOptions options, InetAddress hostname,
                 WorkflowSpecification workflowSpec, MapReduceProgramRunner mrProgramRunner,
                 SparkProgramRunner sparkProgramRunner) {
    this.program = program;
    this.runId = runId;
    this.hostname = hostname;
    this.runtimeArgs = createRuntimeArgs(options.getUserArguments());
    this.workflowSpec = workflowSpec;
    this.logicalStartTime = options.getArguments().hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
                                ? Long.parseLong(options.getArguments()
                                                         .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
                                : System.currentTimeMillis();

    this.mrRunnerFactory = new WorkflowMapReduceRunnerFactory(workflowSpec, mrProgramRunner, program,
                                                            runId, options.getUserArguments(), logicalStartTime);
    this.sparkRunnerFactory = new WorkflowSparkRunnerFactory(workflowSpec, sparkProgramRunner, program, runId,
                                                             options.getUserArguments(), logicalStartTime);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Workflow {}", workflowSpec);

    // Using small size thread pool is enough, as the API we supported are just simple lookup.
    httpService = NettyHttpService.builder()
      .setWorkerThreadPoolSize(2)
      .setExecThreadPoolSize(4)
      .setHost(hostname.getHostName())
      .addHttpHandlers(ImmutableList.of(new WorkflowServiceHandler(createStatusSupplier())))
      .build();

    httpService.startAndWait();
    running = true;
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Start workflow execution for {}", workflowSpec);
    InstantiatorFactory instantiator = new InstantiatorFactory(false);
    ClassLoader classLoader = program.getClassLoader();

    // Executes actions step by step. Individually invoke the init()->run()->destroy() sequence.
    Iterator<WorkflowActionSpecification> iterator = workflowSpec.getActions().iterator();
    int step = 0;
    while (running && iterator.hasNext()) {
      WorkflowActionSpecification actionSpec = iterator.next();
      workflowStatus = new WorkflowStatus(state(), actionSpec, step++);

      WorkflowAction action = initialize(actionSpec, classLoader, instantiator);
      try {
        action.run();
      } catch (Throwable t) {
        LOG.warn("Exception on WorkflowAction.run(), aborting Workflow. {}", actionSpec);
        // this will always rethrow
        Throwables.propagateIfPossible(t, Exception.class);
      } finally {
        // Destroy the action.
        destroy(actionSpec, action);
      }
    }

    // If there is some task left when the loop exited, it must be called by explicit stop of this driver.
    if (iterator.hasNext()) {
      LOG.warn("Workflow explicitly stopped. Treated as abort on error. {} {}", workflowSpec);
      throw new IllegalStateException("Workflow stopped without executing all tasks: " + workflowSpec);
    }

    LOG.info("Workflow execution succeeded for {}", workflowSpec);

    running = false;
  }

  @Override
  protected void triggerShutdown() {
    running = false;
  }

  /**
   * Returns the endpoint that the http service is bind to.
   *
   * @throws IllegalStateException if the service is not started.
   */
  InetSocketAddress getServiceEndpoint() {
    Preconditions.checkState(httpService != null && httpService.isRunning(), "Workflow service is not started.");
    return httpService.getBindAddress();
  }

  /**
   * Instantiates and initialize a WorkflowAction.
   */
  @SuppressWarnings("unchecked")
  private WorkflowAction initialize(WorkflowActionSpecification actionSpec,
                                    ClassLoader classLoader, InstantiatorFactory instantiator) throws Exception {
    Class<?> clz = Class.forName(actionSpec.getClassName(), true, classLoader);
    Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
    WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();

    try {
      action.initialize(new BasicWorkflowContext(workflowSpec, actionSpec,
                                                 logicalStartTime, mrRunnerFactory, sparkRunnerFactory, runtimeArgs));
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.initialize(), abort Workflow. {}", actionSpec, t);
      // this will always rethrow
      Throwables.propagateIfPossible(t, Exception.class);
    }

    return action;
  }

  /**
   * Calls the destroy method on the given WorkflowAction.
   */
  private void destroy(WorkflowActionSpecification actionSpec, WorkflowAction action) {
    try {
      action.destroy();
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.destroy(): {}", actionSpec, t);
      // Just log, but not propagate
    }
  }

  private Map<String, String> createRuntimeArgs(Arguments args) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : args) {
      builder.put(entry);
    }
    return builder.build();
  }

  private Supplier<WorkflowStatus> createStatusSupplier() {
    return new Supplier<WorkflowStatus>() {
      @Override
      public WorkflowStatus get() {
        return workflowStatus;
      }
    };
  }
}
