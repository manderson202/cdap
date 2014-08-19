/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.exception.BadRequestException;
import co.cask.cdap.client.exception.StreamNotFoundException;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 *
 */
@Category(XSlowTests.class)
public class StreamClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(StreamClientTestRun.class);

  private StreamClient streamClient;

  @Before
  public void setUp() throws Throwable {
    ClientConfig config = new ClientConfig("localhost");
    streamClient = new StreamClient(config);
  }

  @Test
  public void testAll() throws Exception {
    String testStreamId = "teststream";
    String testStreamEvent = "blargh_data";

    LOG.info("Getting stream list");
    int baseStreamCount = streamClient.list().size();
    Assert.assertEquals(baseStreamCount, streamClient.list().size());
    LOG.info("Creating stream");
    streamClient.create(testStreamId);
    LOG.info("Checking stream list");
    Assert.assertEquals(baseStreamCount + 1, streamClient.list().size());
    // TODO: getting and setting config for stream is not supported with in-memory
//    streamClient.setTTL(testStreamId, 123);
//    streamClient.sendEvent(testStreamId, testStreamEvent);
//    streamClient.truncate(testStreamId);
//    streamClient.sendEvent(testStreamId, testStreamEvent);
//    String consumerId = streamClient.getConsumerId(testStreamId);
//    Assert.assertEquals(testStreamEvent, streamClient.dequeueEvent(testStreamId, consumerId));
//    Assert.assertEquals(null, streamClient.dequeueEvent(testStreamId, consumerId));
  }

  /**
   * Tests for the get events call
   */
  @Test
  public void testStreamEvents() throws IOException, BadRequestException, StreamNotFoundException {
    String streamId = "testEvents";

    streamClient.create(streamId);
    for (int i = 0; i < 10; i++) {
      streamClient.sendEvent(streamId, "Testing " + i);
    }

    // Read all events
    List<StreamEvent> events = streamClient.getEvents(streamId, 0, Long.MAX_VALUE,
                                                      Integer.MAX_VALUE, Lists.<StreamEvent>newArrayList());
    Assert.assertEquals(10, events.size());

    // Read first 5 only
    events.clear();
    streamClient.getEvents(streamId, 0, Long.MAX_VALUE, 5, events);
    Assert.assertEquals(5, events.size());

    // Read 2nd and 3rd only
    long startTime = events.get(1).getTimestamp();
    long endTime = events.get(2).getTimestamp() + 1;
    events.clear();
    streamClient.getEvents(streamId, startTime, endTime, Integer.MAX_VALUE, events);

    Assert.assertEquals(2, events.size());

    for (int i = 1; i < 3; i++) {
      Assert.assertEquals("Testing " + i, Charsets.UTF_8.decode(events.get(i - 1).getBody()).toString());
    }
  }
}