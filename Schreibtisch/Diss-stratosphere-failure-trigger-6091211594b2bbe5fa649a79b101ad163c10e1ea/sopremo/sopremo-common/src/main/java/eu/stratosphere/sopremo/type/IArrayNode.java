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
package eu.stratosphere.sopremo.type;

import java.util.Collection;

import eu.stratosphere.sopremo.cache.ArrayCache;

/**
 * Interface for all array type nodes.
 * 
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public interface IArrayNode extends IStreamArrayNode {

	/**
	 * Returns the actual size of this node.
	 * 
	 * @return size
	 */
	public abstract int size();

	/**
	 * Adds the given {@link IJsonNode} to the end of the array
	 * 
	 * @param node
	 *        the node wich should be added
	 * @return this node
	 */
	public abstract IArrayNode add(final IJsonNode node);

	/**
	 * Adds the given {@link IJsonNode} at the specified <code>index</code> to the array. The node which was saved at
	 * this index before and all nodes with a higher index get there index incremeted by 1.
	 * 
	 * @param index
	 *        the index where the node should be added
	 * @param element
	 *        the node that should be added
	 * @return this node
	 */
	public abstract IArrayNode add(final int index, final IJsonNode element);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IJsonNode#clone()
	 */
	@Override
	public IArrayNode clone();

	/**
	 * Returns the node which is saved in the array at the specified <code>index</code>.
	 * 
	 * @param index
	 *        the index that should be returned
	 * @return element at <code>index</code> or {@link eu.stratosphere.sopremo.type.MissingNode MissingNode}, when
	 *         <code>index</code> is out of bounds or not present
	 */
	public abstract IJsonNode get(final int index);

	/**
	 * Sets the given {@link IJsonNode} at the specified <code>index</code> in the array. The node which was saved at
	 * this index before will be overwriten.
	 * 
	 * @param index
	 *        the index for which the node should be set
	 * @param node
	 *        the node that should be set
	 * @return the node which has been overwriten
	 */
	public abstract IJsonNode set(final int index, final IJsonNode node);

	/**
	 * Removes the node which is saved at the specified <code>index</code>. All nodes with a higher index then the given
	 * get there index decremented by 1.
	 * 
	 * @param index
	 *        the index in the array where the saved node should be removed
	 * @return the removed node
	 */
	public abstract IJsonNode remove(final int index);

	/**
	 * Clears this array from all saved nodes.
	 */
	@Override
	public abstract void clear();

	/**
	 * Adds all {@link IJsonNode}s in the given Collection to the end of this array.
	 * 
	 * @param c
	 *        a Collection of all nodes that should be added
	 * @return this node
	 */
	public abstract IArrayNode addAll(final Collection<? extends IJsonNode> c);

	/**
	 * Adds all {@link IJsonNode}s in the given {@link IArrayNode} to the end of this array.
	 * 
	 * @param node
	 *        an IArrayNode with all nodes that should be added
	 * @return this node
	 */
	public abstract IArrayNode addAll(final IArrayNode node);

	/**
	 * Transforms this node into a standard Java-Array containing all saved nodes.
	 * 
	 * @param array
	 *        preallocated array that should be used, when the size matches
	 * @return Array of all saved nodes
	 */
	public abstract IJsonNode[] toArray(ArrayCache<IJsonNode> arrayCache);

	/**
	 * Adds all {@link IJsonNode}s to the end of this array.
	 * 
	 * @param node
	 *        an IArrayNode with all nodes that should be added
	 * @return this node
	 */
	public abstract IArrayNode addAll(IJsonNode[] nodes);

	/**
	 * @param nodes
	 */
	public abstract void setAll(IJsonNode[] nodes);

	public abstract Collection<IJsonNode> asCollection();

	/**
	 * @param node
	 * @return
	 */
	public abstract boolean contains(IJsonNode node);
}