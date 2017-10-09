package eu.stratosphere.nephele.visualization.swt;

import org.eclipse.swt.custom.StyledText;

import eu.stratosphere.nephele.managementgraph.ManagementVertex;

/**
 * This class is for creating and formating the Text for
 * the Information Box
 * 
 * @author bel
 */
public class SWTInfoBox {
	
	/**
	 * The StyledText Box which shows the Information
	 */
	private StyledText infoText;
	
	/**
	 * Shows if Nephele is startet or not.
	 */
	private String nepheleStatus;
	
	/**
	 * Contains the Information about the chosen Vertex
	 */
	private String vertexInformation;

	
	//Default Constructor
	public SWTInfoBox() {
		//nepheleStatus is off per Default
		this.nepheleStatus = "Nephele is off \n";
		this.vertexInformation = "";
	}
	
	
	public SWTInfoBox(StyledText infoText) {
		this.infoText = infoText;
		
		//nepheleStatus is off per Default
		this.nepheleStatus = "Nephele is off \n";
		
		this.vertexInformation = "";
	}
	
	
	public void setNepheleStatus(boolean isOn) {
		if (isOn) {
			this.nepheleStatus = "Nephele is on \n";
		} else {
			this.nepheleStatus = "Nephele is off \n";
		}
	}
	
	
	public String getInfoString() {
		//Concatinate all Informations and return them
		
		return (this.nepheleStatus + "\n" + this.vertexInformation);
	}
	
	public void updateInfoBox() {
		this.infoText.setText(getInfoString());
	}
	
	public void setVertexInformation(ManagementVertex managementVertex) {
		String taskName = managementVertex.getName() + " (" + (managementVertex.getIndexInGroup() + 1) + " of "
				+ managementVertex.getNumberOfVerticesInGroup() + ")";
		
		String newInformation = ("Task Name: " + taskName
				+ "\nExecution State: " + managementVertex.getExecutionState().toString()
				+ "\nCheckpoint State: " + managementVertex.getCheckpointState().toString()
				+ "\nInstance Name: " + managementVertex.getInstanceName().toString()
				+ "\nInstance Type: " + managementVertex.getInstanceType().toString());
		
		this.vertexInformation = newInformation;
		//System.out.println("Vertex Information: " + this.vertexInformation);
	}
	
}
