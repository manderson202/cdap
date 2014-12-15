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

import co.cask.cdap.api.spark.SparkContext;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.internal.app.runtime.AbstractListener;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.spark.SparkProgramController;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Threads;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A {@link SparkRunnerFactory} that creates {@link Callable} for executing Spark programs from Workflow.
 */
final class WorkflowSparkRunnerFactory implements SparkRunnerFactory {

  private final WorkflowSpecification workflowSpec;
  private final ProgramRunner programRunner;
  private final Program workflowProgram;
  private final RunId runId;
  private final Arguments userArguments;
  private final long logicalStartTime;

  WorkflowSparkRunnerFactory(WorkflowSpecification workflowSpec, ProgramRunner programRunner,
                             Program workflowProgram, RunId runId,
                             Arguments userArguments, long logicalStartTime) {
    this.workflowSpec = workflowSpec;
    this.programRunner = programRunner;
    this.workflowProgram = workflowProgram;
    this.runId = runId;
    this.logicalStartTime = logicalStartTime;
    this.userArguments = userArguments;
  }

  @Override
  public Callable<SparkContext> create(String name) {

    final SparkSpecification sparkSpec = workflowSpec.getSpark().get(name);
    Preconditions.checkArgument(sparkSpec != null,
                                "No Spark with name %s found in Workflow %s", name, workflowSpec.getName());

    final Program sparkProgram = new WorkflowSparkProgram(workflowProgram, sparkSpec);
    final ProgramOptions options = new SimpleProgramOptions(
      sparkProgram.getName(),
      new BasicArguments(ImmutableMap.of(
        ProgramOptionConstants.RUN_ID, runId.getId(),
        ProgramOptionConstants.LOGICAL_START_TIME, Long.toString(logicalStartTime),
        ProgramOptionConstants.WORKFLOW_BATCH, name
      )),
      userArguments
    );

    return new Callable<SparkContext>() {
      @Override
      public SparkContext call() throws Exception {
        return runAndWait(sparkProgram, options);
      }
    };
  }

  /**
   * Executes given Spark Program and block until it completed. On completion, return the SparkContext.
   *
   * @throws Exception if execution failed.
   */
  private SparkContext runAndWait(Program program, ProgramOptions options) throws Exception {
    ProgramController controller = programRunner.run(program, options);
    final SparkContext context = (controller instanceof SparkProgramController)
                                        ? ((SparkProgramController) controller).getContext()
                                        : null;
    // Execute the program.
    final SettableFuture<SparkContext> completion = SettableFuture.create();
    controller.addListener(new AbstractListener() {
      @Override
      public void stopped() {
        completion.set(context);
      }

      @Override
      public void error(Throwable cause) {
        completion.setException(cause);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Block for completion.
    try {
      return completion.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw Throwables.propagate(cause);
    }
  }
}
