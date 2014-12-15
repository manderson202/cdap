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

import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.internal.app.ForwardingApplicationSpecification;
import co.cask.cdap.internal.app.program.ForwardingProgram;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A Forwarding Program that turns a Workflow Program into a Spark program.
 */
public final class WorkflowSparkProgram extends ForwardingProgram {

  private final SparkSpecification sparkSpec;

  public WorkflowSparkProgram(Program delegate, SparkSpecification sparkSpec) {
    super(delegate);
    this.sparkSpec = sparkSpec;
  }

  @Override
  public String getMainClassName() {
    return sparkSpec.getClassName();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) Class.forName(getMainClassName(), true, getClassLoader());
  }

  @Override
  public ProgramType getType() {
    return ProgramType.SPARK;
  }

  @Override
  public Id.Program getId() {
    return Id.Program.from(getAccountId(), getApplicationId(), getName());
  }

  @Override
  public String getName() {
    return sparkSpec.getName();
  }

  @Override
  public ApplicationSpecification getSpecification() {
    return new ForwardingApplicationSpecification(super.getSpecification()) {
      @Override
      public Map<String, SparkSpecification> getSpark() {
        return ImmutableMap.of(sparkSpec.getName(), sparkSpec);
      }
    };
  }
}
