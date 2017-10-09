package eu.stratosphere.nephele.api;

import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.api.FaultToleranceAPI;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
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
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;
//import eu.stratosphere.nephele.util.ServerTestUtils;
import eu.stratosphere.nephele.util.StringUtils;

//import eu.stratosphere.nephele.api.FaultToleranceAPI;

public class belsPlayground {

	/**
	 * The thread running the job manager.
	 */
	private static JobManagerThread JOB_MANAGER_THREAD = null;
	
	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";
	
	/**
	 * The configuration for the job client;
	 */
	private static Configuration CONFIGURATION;
	
	/**
	 * The degree of parallelism for the job.
	 */
	private static final int DEGREE_OF_PARALLELISM = 4;
	
	/**
	 * Global flag to indicate if a task has already failed once.
	 */
	private static final Set<String> FAILED_ONCE = new HashSet<String>();
	
	/**
	 * Configuration key to access the number of records after which the execution failure shall occur.
	 */
	private static final String FAILED_AFTER_RECORD_KEY = "failure.after.record";
	
	/**
	 * The key to access the index of the subtask which is supposed to fail.
	 */
	private static final String FAILURE_INDEX_KEY = "failure.index";
	
	/**
	 * The number of records to generate by each producer.
	 */
	private static final int RECORDS_TO_GENERATE = 512 * 1024;
	
	/**
	 * The size of an individual record in bytes.
	 */
	private static final int RECORD_SIZE = 256;
	
	
	public static void main(String[] args) {
		/*startNephele();
		
		testNoFailingInternalVertex();
		//testFailingInternalVertex();
		
		stopNephele();
		*/
		
		String configDir = null;
		try {

			// Try to find the correct configuration directory
			final String userDir = System.getProperty("user.dir");
			configDir = userDir + File.separator + CONFIGURATION_DIRECTORY;
			if (!new File(configDir).exists()) {
				configDir = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
			}

		} catch (SecurityException e) {
			fail(e.getMessage());
		} catch (IllegalArgumentException e) {
			fail(e.getMessage());
		}

		CONFIGURATION = GlobalConfiguration
			.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });
		
		
		
		FaultToleranceAPI api = new FaultToleranceAPI(CONFIGURATION, configDir);
		api.startNephele();
		
		JobGraph jobGraph = api.createJobGraph("my little test", 4, 1);
		
		api.createAndLaunchJob(jobGraph);
		
		api.stopNephele();
		
	}
	
	
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
	
	
	public final static class InputTask extends AbstractGenericInputTask {

		private RecordWriter<FailingJobRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			final FailingJobRecord record = new FailingJobRecord();
			for (int i = 0; i < RECORDS_TO_GENERATE; ++i) {
				this.recordWriter.emit(record);

				if (i == failAfterRecord && failing) {
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}

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

		private MutableRecordReader<FailingJobRecord> recordReader;

		private RecordWriter<FailingJobRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordWriter = new RecordWriter<FailingJobRecord>(this);
			this.recordReader = new MutableRecordReader<belsPlayground.FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final FailingJobRecord record = new FailingJobRecord();

			boolean failing = false;
		
			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
				
			}

			int count = 0;

			while (this.recordReader.next(record)) {

				this.recordWriter.emit(record);
				if (count++ == failAfterRecord && failing) {
					
					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskNameWithIndex());
				}
				if (this.isCanceled) {
					break;
				}
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

		private MutableRecordReader<FailingJobRecord> recordReader;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordReader = new MutableRecordReader<belsPlayground.FailingJobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			boolean failing = false;

			final int failAfterRecord = getTaskConfiguration().getInteger(FAILED_AFTER_RECORD_KEY, -1);
			synchronized (FAILED_ONCE) {
				failing = (getIndexInSubtaskGroup() == getTaskConfiguration().getInteger(
					FAILURE_INDEX_KEY, -1)) && FAILED_ONCE.add(getEnvironment().getTaskName());
			}

			int count = 0;

			final FailingJobRecord record = new FailingJobRecord();
			while (this.recordReader.next(record)) {

				if (count++ == failAfterRecord && failing) {

					throw new RuntimeException("Runtime exception in " + getEnvironment().getTaskName() + " "
						+ getIndexInSubtaskGroup());
				}
			}
		}

	}
	
	public static class FailingJobRecord implements Record {

		private final byte[] data = new byte[RECORD_SIZE];

		public FailingJobRecord() {
			for (int i = 0; i < this.data.length; ++i) {
				this.data[i] = (byte) (i % 31);
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final DataOutput out) throws IOException {

			out.write(this.data);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void read(final DataInput in) throws IOException {

			in.readFully(this.data);
		}
	}
	
	/**
	 * Waits until the job manager for the tests has become ready to accept jobs.
	 * 
	 * @param jobManager
	 *        the instance of the job manager to wait for
	 * @throws IOException
	 *         thrown if a connection to the job manager could not be established
	 * @throws InterruptedException
	 *         thrown if the thread was interrupted while waiting for the job manager to become ready
	 */
	//kopiert aus nephele-server, test, util, ServerTestUtils
	public static void waitForJobManagerToBecomeReady(final ExtendedManagementProtocol jobManager) throws IOException,
			InterruptedException {

		Map<InstanceType, InstanceTypeDescription> instanceMap = jobManager.getMapOfAvailableInstanceTypes();

		while (instanceMap.isEmpty()) {

			Thread.sleep(100);
			instanceMap = jobManager.getMapOfAvailableInstanceTypes();
		}
	}
	
	
	/**
	 * Sets up Nephele in local mode.
	 */
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
			//ServerTestUtils test = new ServerTestUtils();
			try {
				waitForJobManagerToBecomeReady(jobManager);
			} catch (Exception e) {
				//Führt zu fehler, wenn Programm hier reinspringt -> ändern!
				fail(StringUtils.stringifyException(e));
			}
		}
	}
	
	/**
	 * Shuts Nephele down.
	 */
	public static void stopNephele() {

		if (JOB_MANAGER_THREAD != null) {
			JOB_MANAGER_THREAD.shutDown();

			try {
				JOB_MANAGER_THREAD.join();
			} catch (InterruptedException ie) {
			}
		}
	}
	
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing inner vertex.
	 */
	//Hier static hinzugefügt, damit es aus der Main heraus aufrufbar ist. 
	//Später lieber ohne static nutzbar machen?!
	public static void testNoFailingInternalVertex() {

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
	
	
	/**
	 * This test checks Nephele's fault tolerance capabilities by simulating a failing inner vertex.
	 */
	public static void testFailingInternalVertex() {

		final JobGraph jobGraph = new JobGraph("Job with failing inner vertex");

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
		innerVertex2.getConfiguration().setInteger(FAILED_AFTER_RECORD_KEY, 95490);
		innerVertex2.getConfiguration().setInteger(FAILURE_INDEX_KEY, 2);

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
