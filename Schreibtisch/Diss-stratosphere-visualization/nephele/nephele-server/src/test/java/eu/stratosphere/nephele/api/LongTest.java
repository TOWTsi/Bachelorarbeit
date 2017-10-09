/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.api;

import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.nephele.util.StringUtils;

import eu.stratosphere.nephele.api.FaultToleranceAPI;
import eu.stratosphere.nephele.api.test.LongRuntimeTest;
import eu.stratosphere.nephele.api.test.LongRuntimeTest.JobRecord;

public class LongTest {

	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";

	/**
	 * The number of records to generate by each producer.
	 */
	private static final int RECORDS_TO_GENERATE = 256 * 512;

	/**
	 * The degree of parallelism for the job.
	 */
	private static final int DEGREE_OF_PARALLELISM = 4;

	/**
	 * The thread running the job manager.
	 */
	private static JobManagerThread JOB_MANAGER_THREAD = null;

	/**
	 * The configuration for the job client;
	 */
	private static Configuration CONFIGURATION;

	/**
	 * Global flag to indicate if a task has already failed once.
	 */
	private static final Set<String> FAILED_ONCE = new HashSet<String>();
	
	private static int countRecords = 0;

	/**
	 * This is an auxiliary class to run the job manager thread.
	 * 
	 * @author warneke
	 */
	private static final class JobManagerThread extends Thread {

		/**
		 * The job manager instance.
		 */
		private final JobManager jobManager;

		/**
		 * Constructs a new job manager thread.
		 * 
		 * @param jobManager
		 *        the job manager to run in this thread.
		 */
		private JobManagerThread(final JobManager jobManager) {

			this.jobManager = jobManager;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {

			// Run task loop
			this.jobManager.runTaskLoop();
		}

		/**
		 * Shuts down the job manager.
		 */
		public void shutDown() {
			this.jobManager.shutDown();
		}
	}

	/**
	 * Sets up Nephele in local mode.
	 */
	@BeforeClass
	public static void startNephele() {

		if (JOB_MANAGER_THREAD == null) {

			// Create the job manager
			JobManager jobManager = null;

			try {

				// Try to find the correct configuration directory
				final String userDir = System.getProperty("user.dir");
				String configDir = userDir + File.separator + CONFIGURATION_DIRECTORY;
				if (!new File(configDir).exists()) {
					configDir = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
				}

				final Constructor<JobManager> c = JobManager.class.getDeclaredConstructor(new Class[] { String.class,
					String.class });
				c.setAccessible(true);
				jobManager = c.newInstance(new Object[] { configDir,
					new String("local") });

			} catch (SecurityException e) {
				fail(e.getMessage());
			} catch (NoSuchMethodException e) {
				fail(e.getMessage());
			} catch (IllegalArgumentException e) {
				fail(e.getMessage());
			} catch (InstantiationException e) {
				fail(e.getMessage());
			} catch (IllegalAccessException e) {
				fail(e.getMessage());
			} catch (InvocationTargetException e) {
				fail(e.getMessage());
			}

			CONFIGURATION = GlobalConfiguration
				.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });

			// Start job manager thread
			if (jobManager != null) {
				JOB_MANAGER_THREAD = new JobManagerThread(jobManager);
				JOB_MANAGER_THREAD.start();
			}

			// Wait for the local task manager to arrive
			try {
				ServerTestUtils.waitForJobManagerToBecomeReady(jobManager);
			} catch (Exception e) {
				fail(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Shuts Nephele down.
	 */
	@AfterClass
	public static void stopNephele() {

		if (JOB_MANAGER_THREAD != null) {
			JOB_MANAGER_THREAD.shutDown();

			try {
				JOB_MANAGER_THREAD.join();
			} catch (InterruptedException ie) {
			}
		}
	}


	public final static class InputTask extends AbstractGenericInputTask {

		private RecordWriter<JobRecord> recordWriter;
		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			this.recordWriter = new RecordWriter<JobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			final JobRecord record = new JobRecord();
			for (int i = 0; i < RECORDS_TO_GENERATE; ++i) {
				this.recordWriter.emit(record);

				if (this.isCanceled) {
					break;
				}
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			this.isCanceled = true;
		}
	}
	

	public final static class InnerTask extends AbstractTask {

		private MutableRecordReader<JobRecord> recordReader;

		private RecordWriter<JobRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			this.recordWriter = new RecordWriter<JobRecord>(this);
			this.recordReader = new MutableRecordReader<LongRuntimeTest.JobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final JobRecord record = new JobRecord();

			int count = 0;

			while (this.recordReader.next(record)) {
				count++;
				
				countRecords++;
				if (countRecords % 10000 == 0) {
					System.out.println("LongInnerTask sleep " + countRecords);
				}
				
				this.recordWriter.emit(record);
				if (this.isCanceled) {
					break;
				}
				Thread.sleep(1);
			}
			if(this.getEnvironment().getTaskName().contains("Inner vertex 2")){
				System.out.println(this.getEnvironment().getTaskName() + " received " + count + " records");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			this.isCanceled = true;
		}
	}


	public static final class OutputTask extends AbstractOutputTask {

		private MutableRecordReader<JobRecord> recordReader;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordReader = new MutableRecordReader<LongRuntimeTest.JobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			final JobRecord record = new JobRecord();
			while (this.recordReader.next(record)) {
				//Do nothing
			}
		}

	}
	
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing inner vertex.
	 */
	@Test
	public void testNoFailingInternalVertex() {

		final JobGraph jobGraph = new JobGraph("Job inner vertex");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		// Reset the FAILED_ONCE flags
		synchronized (FAILED_ONCE) {
			FAILED_ONCE.clear();
		}
		
		FaultToleranceAPI api = new FaultToleranceAPI();
		api.createAndLaunchJob(jobGraph, CONFIGURATION);

	}
	
}
