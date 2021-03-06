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
package eu.stratosphere.sopremo.query;

import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.IRegistry;

/**
 * Registry that manages {@link Operator}s.
 * 
 * @author Arvid Heise
 */
public interface IOperatorRegistry extends IRegistry<OperatorInfo<?>> {
	void put(Class<? extends Operator<?>> clazz);

	String getName(final Class<? extends Operator<?>> operatorClass);
}
