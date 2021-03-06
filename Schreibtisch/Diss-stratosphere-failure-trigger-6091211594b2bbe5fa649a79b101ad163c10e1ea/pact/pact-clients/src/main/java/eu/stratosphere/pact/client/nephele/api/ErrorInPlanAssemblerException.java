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

package eu.stratosphere.pact.client.nephele.api;

/**
 * Exception used to indicate that there is an error in the user-provided plan assembler.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ErrorInPlanAssemblerException extends Exception {
	/**
	 * Serial version UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 7379104350920237486L;

	/**
	 * Creates a <tt>ErrorInPlanAssemblerException</tt> for the given exception.
	 * 
	 * @param cause
	 *        The exception that occurred in the plan assembler.
	 */
	public ErrorInPlanAssemblerException(Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a <tt>ErrorInPlanAssemblerException</tt> for the given exception with an
	 * additional message.
	 * 
	 * @param message
	 *        The additional message.
	 * @param cause
	 *        The exception that occurred in the plan assembler.
	 */
	public ErrorInPlanAssemblerException(String message, Throwable cause) {
		super(message, cause);
	}

}
