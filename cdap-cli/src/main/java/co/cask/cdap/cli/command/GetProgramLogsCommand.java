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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.client.ProgramClient;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;

import java.io.PrintStream;

/**
 * Gets the logs of a program.
 */
public class GetProgramLogsCommand implements Command {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetProgramLogsCommand(ElementType elementType, ProgramClient programClient) {
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void execute(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    String appId = programIdParts[0];
    String programId = programIdParts[1];
    long start = arguments.getLong(ArgumentName.START_TIME.toString(), 0);
    long stop = arguments.getLong(ArgumentName.START_TIME.toString(), Long.MAX_VALUE);

    String logs = programClient.getProgramLogs(appId, elementType.getProgramType(), programId, start, stop);
    output.println(logs);
  }

  @Override
  public String getPattern() {
    return String.format("get %s logs <%s> [<%s>] [<%s>]", elementType.getName(), elementType.getArgumentName(),
                         ArgumentName.START_TIME, ArgumentName.END_TIME);
  }

  @Override
  public String getDescription() {
    return "Gets the logs of a " + elementType.getPrettyName();
  }
}
