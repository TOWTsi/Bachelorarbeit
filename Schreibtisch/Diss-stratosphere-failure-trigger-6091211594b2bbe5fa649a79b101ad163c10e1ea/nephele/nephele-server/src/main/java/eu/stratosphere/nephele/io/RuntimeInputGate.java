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

package eu.stratosphere.nephele.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.channels.FileInputChannel;
import eu.stratosphere.nephele.io.channels.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.NetworkInputChannel;
import eu.stratosphere.nephele.io.channels.RecordDeserializerFactory;
import eu.stratosphere.nephele.io.compression.CompressionException;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.IntegerRecord;
import eu.stratosphere.nephele.types.Record;

/**
 * In Nephele input gates are a specialization of general gates and connect input channels and record readers. As
 * channels, input gates are always parameterized to a specific type of record which they can transport. In contrast to
 * output gates input gates can be associated with a {@link DistributionPattern} object which dictates the concrete
 * wiring between two groups of vertices.
 * <p>
 * This class is in general not thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of record that can be transported through this gate
 */
public class RuntimeInputGate<T extends Record> extends AbstractGate<T> implements InputGate<T> {
	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(InputGate.class);

	/**
	 * The factory used to instantiate new records deserializers.
	 */
	private final RecordDeserializerFactory<T> deserializerFactory;

	/**
	 * The list of input channels attached to this input gate.
	 */
	private final ArrayList<AbstractInputChannel<T>> inputChannels = new ArrayList<AbstractInputChannel<T>>();

	/**
	 * Queue with indices of channels that store at least one available record.
	 */
	private final BlockingQueue<Integer> availableChannels = new LinkedBlockingQueue<Integer>();

	/**
	 * The listener object to be notified when a channel has at least one record available.
	 */
	private final AtomicReference<RecordAvailabilityListener<T>> recordAvailabilityListener = new AtomicReference<RecordAvailabilityListener<T>>(
		null);

	/**
	 * If the value of this variable is set to <code>true</code>, the input gate is closed.
	 */
	private boolean isClosed = false;

	/**
	 * The channel to read from next.
	 */
	private int channelToReadFrom = -1;

	/**
	 * The thread which executes the task connected to the input gate.
	 */
	private Thread executingThread = null;

	private long firstRecordTime = -1;
	
	private long recordNumber = -1;
	
	private List<Long> skipList;

	/**
	 * Constructs a new runtime input gate.
	 * 
	 * @param jobID
	 *        the ID of the job this input gate belongs to
	 * @param gateID
	 *        the ID of the gate
	 * @param index
	 *        the index assigned to this input gate at the {@link Environment} object
	 * @param channelType
	 *        the type of the channels connected to this input gate
	 * @param compressionLevel
	 *        the compression level of the channels which are connected to this input gate
	 * @param deserializerFactory
	 *        the factory for the record deserializer
	 */
	public RuntimeInputGate(final JobID jobID, final GateID gateID, final int index, final ChannelType channelType,
			final CompressionLevel compressionLevel, final RecordDeserializerFactory<T> deserializerFactory) {
		super(jobID, gateID, index, channelType, compressionLevel);
		this.deserializerFactory = deserializerFactory;
	}

	/**
	 * Adds a new input channel to the input gate.
	 * 
	 * @param inputChannel
	 *        the input channel to be added.
	 */
	private void addInputChannel(AbstractInputChannel<T> inputChannel) {

		if (!this.inputChannels.contains(inputChannel)) {
			this.inputChannels.add(inputChannel);
		}
	}

	/**
	 * Removes the input channel with the given ID from the input gate if it exists.
	 * 
	 * @param inputChannelID
	 *        the ID of the channel to be removed
	 */
	public void removeInputChannel(ChannelID inputChannelID) {

		for (int i = 0; i < this.inputChannels.size(); i++) {

			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			if (inputChannel.getID().equals(inputChannelID)) {
				this.inputChannels.remove(i);
				return;
			}
		}

		LOG.debug("Cannot find output channel with ID " + inputChannelID + " to remove");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeAllInputChannels() {

		this.inputChannels.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isInputGate() {

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createNetworkInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		final NetworkInputChannel<T> enic = new NetworkInputChannel<T>(inputGate, this.inputChannels.size(), channelID,
			connectedChannelID, compressionLevel, this.deserializerFactory.createDeserializer());
		addInputChannel(enic);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createFileInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		final FileInputChannel<T> efic = new FileInputChannel<T>(inputGate, this.inputChannels.size(), channelID,
			connectedChannelID, compressionLevel, this.deserializerFactory.createDeserializer());
		addInputChannel(efic);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void createInMemoryInputChannel(final InputGate<T> inputGate, final ChannelID channelID,
			final ChannelID connectedChannelID, final CompressionLevel compressionLevel) {

		final InMemoryInputChannel<T> eimic = new InMemoryInputChannel<T>(inputGate, this.inputChannels.size(),
			channelID, connectedChannelID, compressionLevel, this.deserializerFactory.createDeserializer());
		addInputChannel(eimic);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T readRecord(final T target) throws IOException, InterruptedException {
		
		T record = null;

		if (this.executingThread == null) {
			this.executingThread = Thread.currentThread();
		}

		if (this.executingThread.isInterrupted()) {
			throw new InterruptedException();
		}

		
		while (true) {

			if (this.channelToReadFrom == -1) {

				if (this.isClosed()) {
					return null;
				}

				this.channelToReadFrom = waitForAnyChannelToBecomeAvailable();
			}
			try {
				if(this.firstRecordTime == -1){
					//save time for arrival of first record
					this.firstRecordTime = System.currentTimeMillis();
					this.recordNumber = 0;
				}
				record = this.inputChannels.get(this.channelToReadFrom).readRecord(target);
				if (record != null) {
					this.recordNumber++;
					if(this.skipList != null && !skipList.isEmpty()){
						if(skipList.get(0) == this.recordNumber){
							System.out.println("Skipping Record " + skipList.get(0) + " " + this.recordNumber + " " + ((IntegerRecord) record).getValue());
							skipList.remove(0);
							record = this.inputChannels.get(this.channelToReadFrom).readRecord(target);
						}
					}
				}	
			} catch (EOFException e) {
				if (this.isClosed()) {
					this.channelToReadFrom = -1;
					return null;
				}
			}

		if (record != null) {
//				this.recordNumber++;
//				if(this.skipList != null){
//					if(skipList.get(0) == this.recordNumber){
//					
//					this.channelToReadFrom = -1;
//					System.out.println("Skipping Record " + skipList.get(0) + " " + this.recordNumber + " " + ((IntegerRecord) record).getValue());
//					record = null;
//	
//					}else if(skipList.get(0) < this.recordNumber){
//						System.out.println("Not skipping " + this.recordNumber);
//					}
//				}else{
				break;
	//			}
			} else {
				this.channelToReadFrom = -1;
			}
		}
		
		return record;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void notifyRecordIsAvailable(int channelIndex) {

		this.availableChannels.add(Integer.valueOf(channelIndex));

		final RecordAvailabilityListener<T> listener = this.recordAvailabilityListener.get();
		if (listener != null) {
			listener.reportRecordAvailability(this);
		}
	}

	/**
	 * This method returns the index of a channel which has at least
	 * one record available. The method may block until at least one
	 * channel has become ready.
	 * 
	 * @return the index of the channel which has at least one record available
	 */
	public int waitForAnyChannelToBecomeAvailable() throws InterruptedException {

		return this.availableChannels.take().intValue();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isClosed() throws IOException, InterruptedException {

		if (this.isClosed) {
			return true;
		}

		for (int i = 0; i < this.inputChannels.size(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			if (!inputChannel.isClosed()) {
				return false;
			}
		}

		this.isClosed = true;

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException, InterruptedException {

		for (int i = 0; i < this.inputChannels.size(); i++) {
			final AbstractInputChannel<T> inputChannel = this.inputChannels.get(i);
			inputChannel.close();
		}
		
	}

	/**
	 * Returns the list of InputChannels that feed this RecordReader
	 * 
	 * @return the list of InputChannels that feed this RecordReader
	 */
	@Deprecated
	public List<AbstractInputChannel<T>> getInputChannels() {
		return inputChannels;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return "Input " + super.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {

		// Copy event to all connected channels
		final Iterator<AbstractInputChannel<T>> it = this.inputChannels.iterator();
		while (it.hasNext()) {
			it.next().transferEvent(event);
		}
	}

	/**
	 * Returns the {@link RecordDeserializerFactory} used by this input gate.
	 * 
	 * @return The {@link RecordDeserializerFactory} used by this input gate.
	 */
	public RecordDeserializerFactory<T> getRecordDeserializerFactory() {
		return this.deserializerFactory;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllChannelResources() {

		final Iterator<AbstractInputChannel<T>> it = this.inputChannels.iterator();
		while (it.hasNext()) {
			it.next().releaseAllResources();
		}
	}

	@Override
	public void activateInputChannels() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerRecordAvailabilityListener(final RecordAvailabilityListener<T> listener) {

		if (!this.recordAvailabilityListener.compareAndSet(null, listener)) {
			throw new IllegalStateException(this.recordAvailabilityListener
				+ " is already registered as a record availability listener");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasRecordAvailable() throws IOException, InterruptedException {

		if (this.channelToReadFrom == -1) {

			if (this.isClosed()) {
				return true;
			}

			return !(this.availableChannels.isEmpty());
		}

		return true;
	}

	public void notifyDataUnitConsumed(final int channelIndex) {

		this.channelToReadFrom = -1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void initializeDecompressors() throws CompressionException {

		// In-memory channels to not use compression, so there is nothing to initialize
		if (getChannelType() == ChannelType.INMEMORY) {
			return;
		}

		// Check if this channel is configured to use compression
		if (getCompressionLevel() == CompressionLevel.NO_COMPRESSION) {
			return;
		}

		for (int i = 0; i < this.inputChannels.size(); ++i) {
			this.inputChannels.get(i).initializeDecompressor();
		}
	}

	/**
	 * Returns the number of input channels associated with this input gate.
	 * 
	 * @return the number of input channels associated with this input gate
	 */
	public int getNumberOfInputChannels() {

		return this.inputChannels.size();
	}

	/**
	 * Returns the input channel from position <code>pos</code> of the gate's internal channel list.
	 * 
	 * @param pos
	 *        the position to retrieve the channel from
	 * @return the channel from the given position or <code>null</code> if such position does not exist.
	 */
	public AbstractInputChannel<T> getInputChannel(final int pos) {

		if (pos < this.inputChannels.size()) {
			return this.inputChannels.get(pos);
		}

		return null;
	}

	public long getFirstRecordTime() {
		return this.firstRecordTime;
	}

	public long getRecordNumber() {
		return this.recordNumber;
	}

	public void addToSkipList(long recordNum) {
		if(this.skipList == null){
			this.skipList = new ArrayList<Long>();
		}
		this.skipList.add(recordNum);
	}

	public long getAndResetRecordNumber() {
		long num = this.recordNumber;
		this.recordNumber = -1;
		return num;
	}
}
