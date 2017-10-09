package eu.stratosphere.nephele.api;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.executiongraph.CheckpointState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.jobmanager.JobManager;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.nephele.rpc.ManagementTypeUtils;
import eu.stratosphere.nephele.rpc.RPCService;
import eu.stratosphere.nephele.taskmanager.Task;
import eu.stratosphere.nephele.taskmanager.TaskManager;
import eu.stratosphere.nephele.taskmanager.runtime.RuntimeTask;
import eu.stratosphere.nephele.util.StringUtils;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.api.NepheleInLocalMode;
import eu.stratosphere.nephele.api.belsPlayground.InnerTask;
import eu.stratosphere.nephele.api.belsPlayground.InputTask;
import eu.stratosphere.nephele.api.belsPlayground.OutputTask;
import eu.stratosphere.nephele.api.test.LongRuntimeTest;
import eu.stratosphere.nephele.checkpointing.EphemeralCheckpoint.CheckpointingDecisionState;

/**
 * 
 * @author bel
 *
 */
public class FaultToleranceAPI {
	
	private NepheleInLocalMode nepheleInLocalMode;
	
	//private Text pseodoConsoleTextField;
	
	private static final Log LOG = LogFactory.getLog(JobManager.class);
	
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
	private static String CONF_DIR;
	
	/**
	 * If there is a running JobClient, this jobClient is not null
	 */
	private JobClient jobClient = null;
	
	/**
	 * The running jobManager if it exist
	 */
	private JobManager jobManager = null;
	
	/**
	 * Constructor, which is used, when the Configuration is already initialized
	 * @param conf The Configuration Object
	 * @param confDir The directory of the Configuration
	 */
	public FaultToleranceAPI(Configuration conf, String confDir) {
		CONFIGURATION = conf; 
		CONF_DIR = confDir;
	}
	
	/**
	 * Default Constructor. Searches for the Configuration.
	 */
	public FaultToleranceAPI() {

		// Try to find the correct configuration directory
		try {
			final String userDir = System.getProperty("user.dir");
			//LOG.warn(userDir);
			CONF_DIR = userDir + File.separator + CONFIGURATION_DIRECTORY;
			//LOG.warn(CONF_DIR);
			if (!new File(CONF_DIR).exists()) {
				CONF_DIR = userDir + "/src/test/resources/" + CONFIGURATION_DIRECTORY;
				//LOG.warn(CONF_DIR);
			}
		} catch (SecurityException e) {
			fail(e.getMessage());
		} catch (IllegalArgumentException e) {
			fail(e.getMessage());
		} 
		
		CONFIGURATION = GlobalConfiguration.getConfiguration(new String[] { ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY });
		
	}
	
	/**
	 * Starts Nephele in Local Mode
	 * @return The Extendes Management Protocol for further use.
	 * TODO Which further use?
	 */
	public ExtendedManagementProtocol startNephele() {
		//Add in Stop and Start LOG Infos, if something unexpected happen
		if (this.nepheleInLocalMode == null) {
			this.nepheleInLocalMode = new NepheleInLocalMode(CONFIGURATION, CONF_DIR);
		} 
		this.nepheleInLocalMode.startNephele();
		
		this.jobManager = this.nepheleInLocalMode.getJobManager();
		
		//Connect the GUI with the given port and IP
		return connect();
		
	}
	
	/**
	 * If you have started Nephele otherwise, than you have to set the jobManager here
	 * to ensure that the API works.
	 * @param jobManager The JobManager if you have not started Nephele with this Function
	 */
	public void setJobManager(JobManager jobManager) {
		this.jobManager = jobManager;
	}
	
	/**
	 * Connects Nephele with the in the Configuration given IP and Port.
	 * Is used by the function startNephele()
	 * @return The Extendes Management Protocol for further use.
	 * TODO Which further use?
	 */
	public ExtendedManagementProtocol connect() {
		// Try to load global configuration
		GlobalConfiguration.loadConfiguration(CONF_DIR);

		final String address = GlobalConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
		if (address == null) {
			LOG.error("Cannot find address to job manager's RPC service in configuration");
			System.exit(1);
			return null;
		}

		final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1);

		if (port < 0) {
			LOG.error("Cannot find port to job manager's RPC service in configuration");
			System.exit(1);
			return null;
		}

		RPCService rpcService = null;
		try {
			rpcService = new RPCService(ManagementTypeUtils.getRPCTypesToRegister());
		} catch (IOException ioe) {
			LOG.error("Error initializing the RPC service: " + StringUtils.stringifyException(ioe));
			System.exit(1);
			return null;
		}

		final InetSocketAddress inetaddr = new InetSocketAddress(address, port);
		ExtendedManagementProtocol jobManager = null;
		int queryInterval = -1;
		try {
			jobManager = rpcService.getProxy(inetaddr, ExtendedManagementProtocol.class);

			// Get the query interval
			queryInterval = jobManager.getRecommendedPollingInterval();
			System.out.println("Query Interval: " + queryInterval);

		} catch (Exception e) {
			e.printStackTrace();
			rpcService.shutDown();
			LOG.warn("Cant connect to RPC Service");
		}
		
		return jobManager;
	}
	
	/**
	 * Stops Nephele, shuts down and deletes the JobManager. 
	 */
	public void stopNephele() {
		if (this.nepheleInLocalMode != null) {
			this.nepheleInLocalMode.stopNephele();
			
			this.nepheleInLocalMode = null;
			
			this.jobManager = null;
		}
	}
	
	/*
	 * Create a simple default Job Graph.
	 * Every JobGraph will have one Input and one Output Vertex
	 * and <code>numberOfInnerVertex<\code> inner Vertex (Zero Inner Vertex are possible). 
	 * TODO Outsource this in a Testfile, here is no professional use for this.
	 */
	public JobGraph createJobGraph(String jobName, int degreeOfParallelism, int numberOfInnerVertex) {
		//Use default values if no valid values are given
		if (degreeOfParallelism < 1) {
			degreeOfParallelism = 4;
		}
		if (numberOfInnerVertex < 0) {
			numberOfInnerVertex = 1;
		}
		
		final JobGraph jobGraph = new JobGraph(jobName);
		
		//create input Vertex
		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(degreeOfParallelism);
		input.setNumberOfSubtasksPerInstance(degreeOfParallelism);
		
		//create inner Vertex
		JobTaskVertex[] innerVertexArray = new JobTaskVertex[numberOfInnerVertex];
		
		for (int i = 0; i < numberOfInnerVertex; i++) {
			JobTaskVertex innerVertex = new JobTaskVertex("Inner vertex " + i, jobGraph);
			innerVertex.setTaskClass(InnerTask.class);
			innerVertex.setNumberOfSubtasks(degreeOfParallelism);
			innerVertex.setNumberOfSubtasksPerInstance(degreeOfParallelism);
			// Configure instance sharing
			innerVertex.setVertexToShareInstancesWith(input);
			innerVertexArray[i] = innerVertex;
		}
		
		//create output Vertex
		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(degreeOfParallelism);
		output.setNumberOfSubtasksPerInstance(degreeOfParallelism);
		// Configure instance sharing
		output.setVertexToShareInstancesWith(input);
		
		try {
			for (int i = 0; i < numberOfInnerVertex; i++) {
				//LOG.warn(i);
				//LOG.warn(innerVertexArray.length);
				if (i == 0) {
					input.connectTo(innerVertexArray[i], ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
				} else {
					innerVertexArray[i-1].connectTo(innerVertexArray[i], ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
				}
			}
			
			if (numberOfInnerVertex > 0) {
				innerVertexArray[numberOfInnerVertex-1].connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			} else {
				input.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			}

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}
		
		return jobGraph;
	}
	
	/*
	 * Thread Class for parallel running test
	 * TODO Outsource this test, if possible
	 */
	public class LongRuntimeTestThread implements Runnable {
		@Override
		public void run() {
			LongRuntimeTest test = new LongRuntimeTest();
			
			JobGraph jobGraph = test.createJobGraph();	
			
			createAndLaunchJob(jobGraph);	
		}
		
	}
	
	/*
	 * Run a parallel Test
	 * TODO Outsource
	 */
	public void longRuntimeTestParallel() {
		Thread test = new Thread(new LongRuntimeTestThread());
		test.start();
	}
	
	
	/*
	 * Uses the test classes for a test with long runtime and low processing power.
	 * TODO Outsource
	 */
	public JobGraph longRuntimeTest() {
		LongRuntimeTest test = new LongRuntimeTest();
		
		JobGraph jobGraph = test.createJobGraph();		
		
		return jobGraph;
	}
	
	/**
	 * Launch the Job from the given Job Graph
	 * @param jobGraph The Job Graph which should run
	 */
	public void createAndLaunchJob(JobGraph jobGraph) {
		getAllJobVertex(jobGraph);
		createAndLaunchJob(jobGraph, CONFIGURATION);
	}
	
	/**
	 * Launch the Job from the given Job Graph and Configuration
	 * @param jobGraph The Job Graph which should run
	 * @param conf The Configuration, if you want to use another Configuration
	 * TODO Is this Function necessary? 
	 */
	public void createAndLaunchJob(JobGraph jobGraph, Configuration conf) {
		try {
			this.jobClient = new JobClient(jobGraph, conf);
			this.jobClient.submitJobAndWait();
		} catch (Exception e) {
			//fail(StringUtils.stringifyException(e));
			StringUtils.stringifyException(e);
		} finally {
			if (this.jobClient != null) {
				this.jobClient.close();
				this.jobClient = null;
			}
		}
	}
	
	//TODO 
	/* Es werden Funktionen benötigt die RuntimeTask, ExecutionVertex und ExecutionVertexID jeweils 
	 * passend zurück geben. Für manche Funktionen wird immer was anderes benötigt.
	 * CheckpointUtils verwendet beispielsweise hauptsächlich ExecutionVertexID.
	 * 
	 */
	
	/**
	 * Returns the corresponding Execution Vertex ID to the Runtime Task.
	 * @param task
	 * @return The corresponding Execution Vertex ID to the Runtime Task.
	 */
	public ExecutionVertexID getExecutionVertexID(RuntimeTask task) {
		return task.getVertexID();
	}
	
	/**
	 * Returns the corresponding Execution Vertex ID to the Execution Vertex.
	 * @param executionVertex
	 * @return The corresponding Execution Vertex ID to the Execution Vertex.
	 */
	public ExecutionVertexID getExecutionVertexID(ExecutionVertex executionVertex) {
		return executionVertex.getID();
	}
	
	/**
	 * Returns the corresponding Execution Vertex to the Runtime Task.
	 * @param task
	 * @return The corresponding Execution Vertex to the Runtime Task.
	 */
	public ExecutionVertex runtimeTaskToExecutionVertex(RuntimeTask task) {	
		return this.jobManager.getExecutionGraph().getVertexByID(task.getVertexID());
	}
	
	/**
	 * Returns the corresponding Runtime Task to the Execution Vertex.
	 * @param executionVertex
	 * @return The corresponding Runtime Task to the Execution Vertex.
	 */
	public RuntimeTask executionVertexToRuntimeTask(ExecutionVertex executionVertex) {
		
		TaskManager taskManager = this.jobManager.getInstanceManager().getLocalTaskManagerThread().getTaskManager();
		Map<ExecutionVertexID, Task> runningTasks =  taskManager.getRunningTasks();
		
		Task task = runningTasks.get(executionVertex.getID());
		
		if (task instanceof RuntimeTask) {
			return (RuntimeTask) task;
		}
		
		return null;
	}
	
	
	/**
	 * Returns the EventList from the current running Job Client, which contains
	 * all occurred Events, since the Job is running.
	 * @return The AbstractEvent ArrayList of the current running JobClient
	 */
	public ArrayList<AbstractEvent> getJobClientEventList() {
		if (this.jobClient != null) {
			return this.jobClient.getEventList();
		}
		return null;
	}
	
	/**
	 * TODO Now Follow the Method Stubs for all Functions I should implement
	 */
	
	public AbstractJobVertex[] getAllJobVertex(JobGraph jobGraph) {
		AbstractJobVertex[] tmp = jobGraph.getAllJobVertices();
		for (int i = 0; i < tmp.length; i++) {
			System.out.println("AbstractJobVertex: " + tmp[i].toString());
		}
		return jobGraph.getAllJobVertices();
	}
	
	//Noch eine Funktion die alle ExecutionVertex zurückgibt?
	//Dran denken: auf die Vertices soll auch nach Ablauf noch zugegriffen werden können
	//Vllt den Zugriff anders gestalten? Werden ExecutionVertex nach beendigung des Jobs ungültig?
	
	//
	//
	//Ephemeral Checkpoint Functions
	//
	//
	/**
	 * getCheckpointDecisionState(task)
	 * 
	 */
	
	/**
	 * Changes the Checkpoint State from a Runtime Task at runtime.
	 * @param runtimeTask The Task which should have another Checkpoint State
	 * @param checkpointState The new CheckpointState
	 */
	public void changeCheckpointState(RuntimeTask runtimeTask, CheckpointState checkpointState) {
		//Ändere die Funktion um. Hier mach nur ein Boolean Sinn der auf Checkpointing oder nicht setzt
		//undecided und partial sind nicht sinnvoll zu setzen.
		//TODO Diese Funktion muss nochmal überdacht werden, ob die auch so überhaupt funktionieren würde
		//Wenn bisher ein Checkpoint erstellt wurde und nun ein anderer State gesetzt wird, soll 
		//der aktuelle Checkpoint gelöscht werden
		 
		switch (checkpointState) {
		case UNDECIDED: 
			try {
				runtimeTask.checkpointStateChanged(checkpointState);
			} catch (Exception e) {
				System.out.println("Change Checkpoint State was not successful");
			}
			break;
			
		case NONE:
			try {
				runtimeTask.checkpointStateChanged(checkpointState);
			} catch (Exception e) {
				System.out.println("Change Checkpoint State was not successful");
			}
			break;
			
		case PARTIAL:
			try {
				runtimeTask.checkpointStateChanged(checkpointState);
			} catch (Exception e) {
				System.out.println("Change Checkpoint State was not successful");
			}
			break;
			
		case COMPLETE:
			//Do nothing, because we cant create a complete Checkpoint from nothing.
			break;		
		}	
	}
	
	/**
	 * Returns the CheckpointDecisionState of the given Task.
	 * Values can be NO_CHECKPOINTING, UNDECIDED, CHECKPOINTING
	 * @param task 
	 * @return the CheckpointDecisionState of the given Task: NO_CHECKPOINTING, UNDECIDED or CHECKPOINTING.
	 */
	public CheckpointingDecisionState getCheckpointDecisionState(RuntimeTask task) {
		return task.getEphemeralCheckpoint().getCheckpointingDecision();
	}
	
	/**
	 * Returns a String that explains why the Checkpoint is set, if the Task creates a checkpoint,
	 * else the function returns null.
	 * @param task
	 * @return A String that explains why the Checkpoint is set. If the Task doesn't create a 
	 * checkpoint, the function returns null.
	 */
	public String getWhyCheckpointing(RuntimeTask task) {
		//TODO Funktioniert bisher nur, wenn wegen CPU oder Input/Output der Checkpoint erstellt wird
		if (getCheckpointDecisionState(task) == CheckpointingDecisionState.CHECKPOINTING) {
			return task.getWhyCheckpointing();
		}
		
		//return null if the Checkpointing of the Task is undecided or no_checkpointing
		return null;
	}
	
	
	//
	//
	//Replay
	//
	//
	/**
	 * Abfrage ob sich ein bestimmter Task im Replay befindet.
	 * Wie häufig wurde ein Task replayed.
	 * 
	 */
	
	
	
	//
	//
	//Record Skipping und Consumption Logging (siehe EnvelopeConsumptionLog)
	//
	//
	/**
	 * Welche Records wurden geskipped 
	 * Wie viele Records wurden insgesamt geskipped
	 */
	
	
}























