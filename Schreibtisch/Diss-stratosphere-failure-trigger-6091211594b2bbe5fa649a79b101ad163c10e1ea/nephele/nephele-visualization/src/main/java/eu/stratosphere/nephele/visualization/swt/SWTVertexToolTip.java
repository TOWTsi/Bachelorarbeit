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

package eu.stratosphere.nephele.visualization.swt;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.experimental.chart.swt.ChartComposite;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

public class SWTVertexToolTip extends SWTToolTip {

	private final static String WARNINGTEXT = "Nephele has identified this task as a CPU bottleneck";

	private final Label instanceTypeLabel;

	private final Label instanceIDLabel;

	private final Label executionStateLabel;

	private final Label checkpointStateLabel;

	private Composite warningComposite;

	private final ManagementVertex managementVertex;

	private final ChartComposite threadChart;

	private final static int WIDTH = 450;

	public SWTVertexToolTip(Shell parent, final SWTToolTipCommandReceiver commandReceiver,
			ManagementVertex managementVertex, int x, int y) {
		super(parent, x, y);

		this.managementVertex = managementVertex;

		final VertexVisualizationData vertexVisualizationData = (VertexVisualizationData) managementVertex
			.getAttachment();

		int height;

		final Color backgroundColor = getShell().getBackground();
		final Color foregroundColor = getShell().getForeground();

		// Set the title
		final String taskName = managementVertex.getName() + " (" + (managementVertex.getIndexInGroup() + 1) + " of "
			+ managementVertex.getNumberOfVerticesInGroup() + ")";
		setTitle(taskName);
		
		

		// Only create chart if profiling is enabled
		if (vertexVisualizationData.isProfilingEnabledForJob()) {
			this.threadChart = createThreadChart(vertexVisualizationData, backgroundColor);
			this.threadChart.setLayoutData(new GridData(GridData.FILL_BOTH));
			height = 240; // should be 265 when cancel button is enabled
		} else {
			this.threadChart = null;
			height = 125;
		}

		final Composite tableComposite = new Composite(getShell(), SWT.NONE);
		tableComposite.setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
		tableComposite.setBackground(backgroundColor);
		tableComposite.setForeground(foregroundColor);
		final GridLayout tableGridLayout = new GridLayout(3, false);
		tableGridLayout.marginHeight = 0;
		tableGridLayout.marginLeft = 0;
		tableComposite.setLayout(tableGridLayout);

		final GridData gridData1 = new GridData();
		gridData1.horizontalSpan = 2;
		gridData1.grabExcessHorizontalSpace = true;
		gridData1.widthHint = 300;
		height = 200;

		final GridData gridData2 = new GridData();
		gridData2.grabExcessHorizontalSpace = true;

		//Start Bachelorarbeit Vetesi
		
		final Label killingLabel = new Label(tableComposite, SWT.NONE);
		killingLabel.setBackground(backgroundColor);
		killingLabel.setForeground(foregroundColor);
		killingLabel.setText("Killing this Task:");
		killingLabel.setLayoutData(gridData1);
		
		final Button killTaskButton = new Button(tableComposite, SWT.PUSH);
		final ManagementVertexID mID = this.managementVertex.getID();
		killTaskButton.setText("Kill Task...");
		if(this.managementVertex.getExecutionState() == ExecutionState.RUNNING) {
			killTaskButton.setEnabled(true);
		} else {
			killTaskButton.setVisible(false);
		}
		killTaskButton.setLayoutData(gridData2);
		killTaskButton.addListener(SWT.Selection, new Listener() {

			@Override
			public void handleEvent(Event arg0) {
				commandReceiver.killTask(mID, taskName);
			}

		});
		
		//Ende Bachelorarbeit Vetesi		
		
		// Instance type
		final Label instanceTypeTextLabel = new Label(tableComposite, SWT.NONE);
		instanceTypeTextLabel.setBackground(backgroundColor);
		instanceTypeTextLabel.setForeground(foregroundColor);
		instanceTypeTextLabel.setText("Instance type:");

		this.instanceTypeLabel = new Label(tableComposite, SWT.NONE);
		this.instanceTypeLabel.setBackground(backgroundColor);
		this.instanceTypeLabel.setForeground(foregroundColor);
		this.instanceTypeLabel.setText(this.managementVertex.getInstanceType());
		this.instanceTypeLabel.setLayoutData(gridData1);

		// Instance ID
		final Label instanceIDTextLabel = new Label(tableComposite, SWT.NONE);
		instanceIDTextLabel.setBackground(backgroundColor);
		instanceIDTextLabel.setForeground(foregroundColor);
		instanceIDTextLabel.setText("Instance ID:");

		this.instanceIDLabel = new Label(tableComposite, SWT.NONE);
		this.instanceIDLabel.setBackground(backgroundColor);
		this.instanceIDLabel.setForeground(foregroundColor);
		this.instanceIDLabel.setText(this.managementVertex.getInstanceName());
		this.instanceIDLabel.setLayoutData(gridData2);

		final Button switchToInstanceButton = new Button(tableComposite, SWT.PUSH);
		switchToInstanceButton.setText("Switch to instance...");
		switchToInstanceButton.setEnabled(vertexVisualizationData.isProfilingEnabledForJob());
		switchToInstanceButton.setVisible(false);
		
		

		/*
		 * final String instanceName = this.managementVertex.getInstanceName();
		 * switchToInstanceButton.addListener(SWT.Selection, new Listener() {
		 * @Override
		 * public void handleEvent(Event arg0) {
		 * commandReceiver.switchToInstance(instanceName);
		 * }
		 * });
		 */

		// Execution state
		final Label executionStateTextLabel = new Label(tableComposite, SWT.NONE);
		executionStateTextLabel.setBackground(backgroundColor);
		executionStateTextLabel.setForeground(foregroundColor);
		executionStateTextLabel.setText("Execution state:");

		this.executionStateLabel = new Label(tableComposite, SWT.NONE);
		this.executionStateLabel.setBackground(backgroundColor);
		this.executionStateLabel.setForeground(foregroundColor);
		this.executionStateLabel.setText(this.managementVertex.getExecutionState().toString());
		this.executionStateLabel.setLayoutData(gridData1);

		// Checkpoint state
		final Label checkpointStateTextLabel = new Label(tableComposite, SWT.NONE);
		checkpointStateTextLabel.setBackground(backgroundColor);
		checkpointStateTextLabel.setForeground(foregroundColor);
		checkpointStateTextLabel.setText("Checkpoint state:");

		this.checkpointStateLabel = new Label(tableComposite, SWT.NONE);
		this.checkpointStateLabel.setBackground(backgroundColor);
		this.checkpointStateLabel.setForeground(foregroundColor);
		this.checkpointStateLabel.setText(this.managementVertex.getCheckpointState().toString());
		this.checkpointStateLabel.setLayoutData(gridData1);

		

		final ManagementGroupVertex groupVertex = this.managementVertex.getGroupVertex();
		final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) groupVertex
			.getAttachment();
		if (groupVertexVisualizationData.isCPUBottleneck()) {
			this.warningComposite = createWarningComposite(WARNINGTEXT, SWT.ICON_WARNING);
			height += ICONSIZE;
		} else {
			this.warningComposite = null;
		}

		// Available task actions
		final Composite taskActionComposite = new Composite(getShell(), SWT.NONE);
		taskActionComposite.setLayout(new RowLayout(SWT.HORIZONTAL));
		taskActionComposite.setBackground(backgroundColor);
		taskActionComposite.setForeground(foregroundColor);

		/*
		 * final Button cancelTaskButton = new Button(taskActionComposite, SWT.PUSH);
		 * final ManagementVertexID vertexID = this.managementVertex.getID();
		 * cancelTaskButton.setText("Cancel task");
		 * cancelTaskButton.setEnabled(this.managementVertex.getExecutionState() == ExecutionState.RUNNING);
		 * cancelTaskButton.addListener(SWT.Selection, new Listener() {
		 * @Override
		 * public void handleEvent(Event arg0) {
		 * commandReceiver.cancelTask(vertexID, taskName);
		 * }
		 * });
		 */

		getShell().setSize(WIDTH, height);
		
		finishInstantiation(x, y, WIDTH, false);
		//finishInstantiation(0, 0, WIDTH, false);
	}

	private ChartComposite createThreadChart(VertexVisualizationData visualizationData, Color backgroundColor) {

		final JFreeChart chart = ChartFactory.createStackedXYAreaChart(null, "Time [sec.]", "Thread Utilization [%]",
			visualizationData.getThreadDataSet(), PlotOrientation.VERTICAL, true, true, false);

		chart.setBackgroundPaint(new java.awt.Color(backgroundColor.getRed(), backgroundColor.getGreen(),
			backgroundColor.getBlue()));

		// Set axis properly
		final XYPlot xyPlot = chart.getXYPlot();
		xyPlot.getDomainAxis().setAutoRange(true);
		xyPlot.getDomainAxis().setAutoRangeMinimumSize(60);

		// xyPlot.getRangeAxis().setAutoRange(true);
		xyPlot.getRangeAxis().setRange(0, 100);

		return new ChartComposite(getShell(), SWT.NONE, chart, true);
	}

	public void updateView() {

		// Redraw the chart
		if (this.threadChart != null) {
			this.threadChart.getChart().getXYPlot().configureDomainAxes();
			this.threadChart.getChart().fireChartChanged();
		}

		// Update the labels
		this.executionStateLabel.setText(this.managementVertex.getExecutionState().toString());
		this.checkpointStateLabel.setText(this.managementVertex.getCheckpointState().toString());
		this.instanceIDLabel.setText(this.managementVertex.getInstanceName());
		this.instanceTypeLabel.setText(this.managementVertex.getInstanceType());

		final ManagementGroupVertex groupVertex = this.managementVertex.getGroupVertex();
		final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) groupVertex
			.getAttachment();
		if (groupVertexVisualizationData.isCPUBottleneck()) {
			if (this.warningComposite == null) {
				this.warningComposite = createWarningComposite(WARNINGTEXT, SWT.ICON_WARNING);
				Rectangle clientRect = getShell().getClientArea();
				clientRect.height += ICONSIZE;
				getShell().setSize(clientRect.width, clientRect.height);
			}
		} else {
			if (this.warningComposite != null) {
				this.warningComposite.dispose();
				this.warningComposite = null;
				Rectangle clientRect = getShell().getClientArea();
				clientRect.height -= ICONSIZE;
				getShell().setSize(clientRect.width, clientRect.height);
			}
		}
	}
}
