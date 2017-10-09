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

package eu.stratosphere.pact.compiler.plan;

import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ForwardSS;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.NoneSS;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Map</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MapNode extends SingleInputNode {

	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract
	 *        The map contract object.
	 */
	public MapNode(MapContract pactContract) {
		super(pactContract);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Copy constructor to create a copy a MapNode with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected MapNode(MapNode template, OptimizerNode pred, PactConnection conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	@Override
	public MapContract getPactContract() {
		return (MapContract) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Map";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		return 0;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		super.setInputs(contractToNode);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// the map itself has no interesting properties.
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
			this, 0);
		
		if (!props.isEmpty()) {
			this.inConn.addAllInterestingProperties(props);
		} else {
			this.inConn.setNoInterestingProperties();
		} 
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.SingleInputNode#computeValidPlanAlternatives(java.util.List, eu.stratosphere.pact.compiler.costs.CostEstimator, java.util.List)
	 */
	@Override
	protected void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans, CostEstimator estimator, List<OptimizerNode> outputPlans) {
		
		// we have to check if all input ShipStrategies are the same or at least compatible
		ShipStrategy ss = new NoneSS();

		// check hint shipping strategy
		ShipStrategy hintSS = this.inConn.getShipStrategy();
		if(hintSS.type() == ShipStrategyType.BROADCAST || hintSS.type() == ShipStrategyType.SFR)
			// invalid strategy: we do not produce an alternative node
			return;
		else
			ss = hintSS;
	
		// if no hint for a strategy was provided, we use the default
		if(ss.type() == ShipStrategyType.NONE)
			ss = new ForwardSS();
		
		for(OptimizerNode subPlan : altSubPlans) {
		
			GlobalProperties gp = PactConnection.getGlobalPropertiesAfterConnection(subPlan, this, 0, ss);
			LocalProperties lp = PactConnection.getLocalPropertiesAfterConnection(subPlan, this, ss);
			
			MapNode nMap = new MapNode(this, subPlan, this.inConn, gp, lp);
			nMap.inConn.setShipStrategy(ss);
					
			// now, the properties (copied from the inputs) are filtered by the output contracts
			nMap.getGlobalProperties().filterByNodesConstantSet(this, 0);
			nMap.getLocalProperties().filterByNodesConstantSet(this, 0);
	
			// copy the cumulative costs and set the costs of the map itself to zero
			estimator.costOperator(nMap);
		
			outputPlans.add(nMap);
		}
	}

	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
				
		if(this.getPredNode() != null)
			return this.getPredNode().estimatedNumRecords;
		else
			return -1;
		
	}

}
