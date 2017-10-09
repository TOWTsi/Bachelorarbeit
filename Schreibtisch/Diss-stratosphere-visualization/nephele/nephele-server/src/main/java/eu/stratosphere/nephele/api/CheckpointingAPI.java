package eu.stratosphere.nephele.api;

import java.util.Map;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * 
 * @author bel
 *
 */
public class CheckpointingAPI {

	
	
	/**
	 * Returns a map of vertices this graph consists of.
	 * @param managementGraph The Management Graph of a Job.
	 * @return A map of vertices this graph consists of.
	 */
	public static Map<ManagementVertexID, ManagementVertex> getVertices(ManagementGraph managementGraph) {
		if (managementGraph != null) {
			return managementGraph.getVertices();
		}
		return null;
	}
	
	/**
	 * 
	 * @param managementVertex
	 * @param executionGraph
	 * @return
	 */
	public static ExecutionVertex fromManagementVertex(ManagementVertex managementVertex, ExecutionGraph executionGraph) {
		//TODO TESTEN TESTEN TESTEN
		ManagementVertexID managementVertexID = managementVertex.getID();
		if (managementVertexID instanceof AbstractID) {
			AbstractID aID = (AbstractID) managementVertexID;
			ExecutionVertexID eID = (ExecutionVertexID) aID;
			return executionGraph.getVertexByID(eID);
		}		
		
		return null;
	}
	
	//
	//
	//Ephemeral Checkpoint Functions
	//
	//
	/**
	 * CheckpointDecisionState
	 * CheckpointDecision
	 * 
	 */
	
	/**
	 * Returns the Execution State of the Management Vertex.
	 * @param managementVertex A Management Vertex from the Management Graph.
	 * @return The Execution State of the Management Vertex.
	 */
	public static ExecutionState getExecutionState(ManagementVertex managementVertex) {
		if (managementVertex != null) {
			return managementVertex.getExecutionState();
		}
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
