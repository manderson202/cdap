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
package co.cask.cdap.internal.workflow;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Represents action to executed in a {@link Workflow} for {@link Spark}
 */
public final class SparkWorkflowAction implements WorkflowAction {

  private static final Logger LOG = LoggerFactory.getLogger(SparkWorkflowAction.class);
  private static final String SPARK_NAME = "sparkName";

  private final String name;
  private String sparkName;
  private Callable<SparkContext> sparkRunner;
  private WorkflowContext context;

  public SparkWorkflowAction(String name, String sparkName) {
    this.name = name;
    this.sparkName = sparkName;
  }

  @Override
  public WorkflowActionSpecification configure() {
    return WorkflowActionSpecification.Builder.with()
      .setName(name)
      .setDescription("Workflow action for " + sparkName)
      .withOptions(ImmutableMap.of(SPARK_NAME, sparkName))
      .build();
  }

  @Override
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;

    sparkName = context.getSpecification().getProperties().get(SPARK_NAME);
    Preconditions.checkNotNull(sparkName, "No spark name provided.");

    sparkRunner = context.getSparkRunner(sparkName);

    LOG.info("Initialized for Spark workflow action: {}", sparkName);
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting Spark workflow action: {}", sparkName);
      SparkContext sparkContext = sparkRunner.call();

      // TODO (terence) : Put something back to context.

      LOG.info("Spark workflow action completed: {}", sparkName);
    } catch (Exception e) {
      LOG.info("Failed to execute Spark workflow: {}", sparkName, e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void destroy() {
    // No-op
  }
}
