/*
 * Copyright © 2020  camunda services GmbH (info@camunda.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package io.zeebe.engine.processor.workflow.perf;

import io.zeebe.engine.Loggers;
import io.zeebe.engine.util.TimeAggregation;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.util.sched.clock.DefaultActorClock;
import io.zeebe.util.sched.testing.ActorSchedulerRule;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class PerfDeploymentTest {

  @ClassRule
  public static ActorSchedulerRule schedulerRule =
      new ActorSchedulerRule(1, 1, new DefaultActorClock());

  public static final int WARM_UP_ITERATION = 1_000;
  public static final int ITER_COUNT = 1_000;
  public static final List<TimeAggregation> TIME_AGGREGATIONS = new CopyOnWriteArrayList<>();

  private static final String PROCESS_ID = "process";
  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess(PROCESS_ID).startEvent("startEvent").endEvent().done();

  @Parameter(0)
  public String testName;

  @Rule
  @Parameter(1)
  public EngineRule warmUpRule;

  @Rule
  @Parameter(2)
  public EngineRule engineRule;

  private TimeAggregation timeAggregation;

  @Parameters(name = "{0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "Default CFG",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 128 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 128 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "Entry size 64 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 128 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 128 * 1024 * 1024)
      },
      {
        "Entry size 128 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 128 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 128 * 1024 * 1024)
      },
      {
        "Entry size 256 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 128 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 128 * 1024 * 1024)
      },
      {
        "Entry size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 128 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 128 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 512 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 512 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      {
        "LogSegment size 512 KB - Entry size 64 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 512 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 512 * 1024)
      },
      {
        "LogSegment size 512 KB - Entry size 128 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 512 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 512 * 1024)
      },
      {
        "LogSegment size 512 KB - Entry size 256 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 512 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 512 * 1024)
      },
      {
        "LogSegment size 512 KB - Entry size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 512 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 512 * 1024)
      },
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 1 MB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 1 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 1 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 1 MB - Entry size 64 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 1 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 1 * 1024 * 1024)
      },
      {
        "LogSegment size 1 MB - Entry size 128 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 1 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 1 * 1024 * 1024)
      },
      {
        "LogSegment size 1 MB - Entry size 256 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 1 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 1 * 1024 * 1024)
      },
      {
        "LogSegment size 1 MB - Entry size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 1 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 1 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 16 MB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 16 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 16 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 16 MB - Entry size 64 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 16 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 16 * 1024 * 1024)
      },
      {
        "LogSegment size 16 MB - Entry size 128 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 16 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 16 * 1024 * 1024)
      },
      {
        "LogSegment size 16 MB - Entry size 256 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 16 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 16 * 1024 * 1024)
      },
      {
        "LogSegment size 16 MB - Entry size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 16 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 16 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 64 MB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 64 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 64 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 64 MB - Entry size 64 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 64 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 64 * 1024 * 1024)
      },
      {
        "LogSegment size 64 MB - Entry size 128 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 64 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 64 * 1024 * 1024)
      },
      {
        "LogSegment size 64 MB - Entry size 256 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 64 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 64 * 1024 * 1024)
      },
      {
        "LogSegment size 64 MB - Entry size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 64 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 64 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 512 MB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 512 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 4 * 1024 * 1024, 512 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      {
        "LogSegment size 512 MB - Entry size 64 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 512 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 64 * 1024, 512 * 1024 * 1024)
      },
      {
        "LogSegment size 512 MB - Entry size 128 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 512 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 128 * 1024, 512 * 1024 * 1024)
      },
      {
        "LogSegment size 512 MB - Entry size 256 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 512 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 256 * 1024, 512 * 1024 * 1024)
      },
      {
        "LogSegment size 512 MB - Entry size 512 KB",
        // warm up
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 512 * 1024 * 1024),
        // run
        EngineRule.singlePartition(() -> schedulerRule.get(), 512 * 1024, 512 * 1024 * 1024)
      },

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

    };
  }

  @Before
  public void setup() {
    Loggers.WORKFLOW_PROCESSOR_LOGGER.warn("Running test {}", testName);
    warmUpRule.deployment().withXmlResource(WORKFLOW).deploy();
    engineRule.deployment().withXmlResource(WORKFLOW).deploy();

    warmup();

    timeAggregation =
        new TimeAggregation("START_EVENT:ELEMENT_ACTIVATING", "START_EVENT:ELEMENT_ACTIVATED");
  }

  @After
  public void tearDown() {
    TIME_AGGREGATIONS.add(timeAggregation);
  }

  @AfterClass
  public static void printResults() {
    Loggers.WORKFLOW_PROCESSOR_LOGGER.info(
        "Run {} tests. Print results...", TIME_AGGREGATIONS.size());
    for (int i = 0; i < TIME_AGGREGATIONS.size(); i++) {
      final var timeAggregation = TIME_AGGREGATIONS.get(i);
      Loggers.WORKFLOW_PROCESSOR_LOGGER.info("Test: {} result: {}", i, timeAggregation.toString());
    }
  }

  @Test
  public void shouldCreateDeploymentWithBpmnXml() {
    for (int i = 0; i < ITER_COUNT; i++) {
      final var process = engineRule.workflowInstance().ofBpmnProcessId("process").create();

      timeAggregation.addNanoTime(getStartTime(process), getEndtime(process));
      // TODO timeAggregation.add(getStartTime(process), getEndtime(process));

      if ((i + 1) % 50 == 0) {
        Loggers.STREAM_PROCESSING.warn(timeAggregation.toString());
      }

      // to not collect all records we wrote
      RecordingExporter.reset();
    }

    Loggers.STREAM_PROCESSING.warn(timeAggregation.toString());
  }

  private void warmup() {
    Loggers.STREAM_PROCESSING.warn("Will do warm up with {} iterations", WARM_UP_ITERATION);
    final var start = System.nanoTime();
    for (int i = 0; i < WARM_UP_ITERATION; i++) {
      warmUpRule.workflowInstance().ofBpmnProcessId("process").create();
    }
    final var end = System.nanoTime();
    Loggers.STREAM_PROCESSING.warn("Warm up done, took {}", end - start);
  }

  private long getStartTime(final long process) {
    return RecordingExporter.workflowInstanceRecords()
        .withWorkflowInstanceKey(process)
        .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
        .withElementId("startEvent")
        .findFirst()
        .orElseThrow()
        .getTimestamp();
  }

  private long getEndtime(final long process) {
    return RecordingExporter.workflowInstanceRecords()
        .withWorkflowInstanceKey(process)
        .withIntent(WorkflowInstanceIntent.ELEMENT_ACTIVATED)
        .withElementId("startEvent")
        .findFirst()
        .orElseThrow()
        .getTimestamp();
  }
}