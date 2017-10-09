package eu.stratosphere.nephele.api;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.util.StringUtils;

public class NepheleInLocalMode {
	
	/**
	 * The thread running the job manager.
	 */
	private static JobManagerThread JOB_MANAGER_THREAD = null;
	
	/**
	 * The JobManager
	 */
	private JobManager jobManager;
	
	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static final String CONFIGURATION_DIRECTORY = "correct-conf";
	
	/**
	 * The configuration for the job client;
	 */
	private static Configuration CONFIGURATION;
	
	/**
	 * The directory containing the Nephele configuration for this integration test.
	 */
	private static String CONF_DIR = "correct-conf";
	
	public NepheleInLocalMode() {
		//TODO Set the Configuration and delete Configuration Initilizing in startNephele
		// Try to find the correct configuration directory
		try {
			final String userDir = System.getProperty("user.dir");
			String CONF_DIR = userDir + File.separator + CONFIGURATION_DIRECTORY;
			if (!new File(CONF_DIR).exists()) {
				CONF_DIR = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
			}
		} catch (SecurityException e) {
			fail(e.getMessage());
		} catch (IllegalArgumentException e) {
			fail(e.getMessage());
		} 
				
		CONFIGURATION = GlobalConfiguration.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });
				
	}
	
	public NepheleInLocalMode(Configuration conf, String confDir) {
		CONFIGURATION = conf;
		CONF_DIR = confDir;
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
	public void startNephele() {

		if (JOB_MANAGER_THREAD == null) {

			// Create the job manager
			this.jobManager = null;

			try {
				final Constructor<JobManager> c = JobManager.class.getDeclaredConstructor(new Class[] { String.class,
					String.class });
				c.setAccessible(true);
				this.jobManager = c.newInstance(new Object[] { CONF_DIR,
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

			// Start job manager thread
			if (this.jobManager != null) {
				JOB_MANAGER_THREAD = new JobManagerThread(this.jobManager);
				JOB_MANAGER_THREAD.start();
			}

			// Wait for the local task manager to arrive
			//ServerTestUtils test = new ServerTestUtils();
			try {
				waitForJobManagerToBecomeReady(this.jobManager);
			} catch (Exception e) {
				//Führt zu fehler, wenn Programm hier reinspringt -> ändern!
				fail(StringUtils.stringifyException(e));
			}
			System.out.println("Started Nephele in local Mode.");
		}
	}
	
	/**
	 * Shuts Nephele down.
	 */
	public void stopNephele() {

		if (JOB_MANAGER_THREAD != null) {
			JOB_MANAGER_THREAD.shutDown();

			try {
				JOB_MANAGER_THREAD.join();
			} catch (InterruptedException ie) {
			}
			//Set to null, so we can restart nephele
			JOB_MANAGER_THREAD = null;
			this.jobManager = null;
			System.out.println("Stopped Nephele and shut down the Job Manager Thread.");
		}
	}
	
	public JobManager getJobManager() {
		return this.jobManager;
	}
	
}
