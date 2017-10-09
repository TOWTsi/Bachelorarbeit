package eu.stratosphere.nephele.api.test;

import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.io.MutableRecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobGenericInputVertex;
import eu.stratosphere.nephele.jobgraph.JobGenericOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;
import eu.stratosphere.nephele.template.AbstractOutputTask;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

/*
 * This Test should run long, but don't use much processing power
 */
public class LongRuntimeTest {
	
	//Ähnlich den bisherigen Tests, sollte die wichtigsten Sachen übergeben bekommen
	//Wird nur aufgerufen, wenn nephele schon gestartet wurde und alle 
	//Konfigurationen schon eingelesen wurden.
	
	/**
	 * The degree of parallelism for the job.
	 */
	private static final int DEGREE_OF_PARALLELISM = 4;
	
	/**
	 * The number of records to generate by each producer.
	 */
	private static final int RECORDS_TO_GENERATE = 256 * 512 / 4;
	//ca 38750ms - RECORDS_TO_GENERATE = 256 * 512 / 4;
	
	/**
	 * The size of an individual record in bytes.
	 */
	private static final int RECORD_SIZE = 256;
	
	/**
	 * 
	 */
	private static int countRecords = 0;
	
	
	public static class JobRecord implements Record {

		private final byte[] data = new byte[RECORD_SIZE];

		//Create a record
		public JobRecord() {
			for (int i = 0; i < this.data.length; ++i) {
				this.data[i] = (byte) (i % 31);
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void write(final DataOutput out) throws IOException {
			out.write(this.data);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void read(final DataInput in) throws IOException {
			in.readFully(this.data);
		}
	}
	
	public final static class InputTask extends AbstractGenericInputTask {

		private RecordWriter<JobRecord> recordWriter;
		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			this.recordWriter = new RecordWriter<JobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			final JobRecord record = new JobRecord();
			for (int i = 0; i < RECORDS_TO_GENERATE; ++i) {
				this.recordWriter.emit(record);

				if (this.isCanceled) {
					break;
				}
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			this.isCanceled = true;
		}
	}
	
	
	public final static class InnerTask extends AbstractTask {

		private MutableRecordReader<JobRecord> recordReader;

		private RecordWriter<JobRecord> recordWriter;

		private volatile boolean isCanceled = false;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {
			this.recordWriter = new RecordWriter<JobRecord>(this);
			this.recordReader = new MutableRecordReader<LongRuntimeTest.JobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {

			final JobRecord record = new JobRecord();

			int count = 0;

			while (this.recordReader.next(record)) {
				count++;
				
				countRecords++;
				if (countRecords % 10000 == 0) {
					System.out.println("LongInnerTask sleep " + countRecords);
				}
				
				this.recordWriter.emit(record);
				if (this.isCanceled) {
					break;
				}
				Thread.sleep(1);
			}
			if(this.getEnvironment().getTaskName().contains("Inner vertex 2")){
				System.out.println(this.getEnvironment().getTaskName() + " received " + count + " records");
			}
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void cancel() {
			this.isCanceled = true;
		}
	}
	
	
	public static final class OutputTask extends AbstractOutputTask {

		private MutableRecordReader<JobRecord> recordReader;

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void registerInputOutput() {

			this.recordReader = new MutableRecordReader<LongRuntimeTest.JobRecord>(this);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void invoke() throws Exception {
			final JobRecord record = new JobRecord();
			while (this.recordReader.next(record)) {
				//Do nothing
			}
		}

	}
	
	
	
	/* 
	 * Constructor
	 */
	public LongRuntimeTest() {
		
	}
	
	public JobGraph createJobGraph() {
		
		final JobGraph jobGraph = new JobGraph("Long Runtime Test");

		final JobGenericInputVertex input = new JobGenericInputVertex("Input", jobGraph);
		input.setInputClass(InputTask.class);
		input.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		input.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex1 = new JobTaskVertex("Inner vertex 1", jobGraph);
		innerVertex1.setTaskClass(InnerTask.class);
		innerVertex1.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex1.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex2 = new JobTaskVertex("Inner vertex 2", jobGraph);
		innerVertex2.setTaskClass(InnerTask.class);
		innerVertex2.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex2.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobTaskVertex innerVertex3 = new JobTaskVertex("Inner vertex 3", jobGraph);
		innerVertex3.setTaskClass(InnerTask.class);
		innerVertex3.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		innerVertex3.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		final JobGenericOutputVertex output = new JobGenericOutputVertex("Output", jobGraph);
		output.setOutputClass(OutputTask.class);
		output.setNumberOfSubtasks(DEGREE_OF_PARALLELISM);
		output.setNumberOfSubtasksPerInstance(DEGREE_OF_PARALLELISM);

		// Configure instance sharing
		innerVertex1.setVertexToShareInstancesWith(input);
		innerVertex2.setVertexToShareInstancesWith(input);
		innerVertex3.setVertexToShareInstancesWith(input);
		output.setVertexToShareInstancesWith(input);

		try {

			input.connectTo(innerVertex1, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex1.connectTo(innerVertex2, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex2.connectTo(innerVertex3, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			innerVertex3.connectTo(output, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

		} catch (JobGraphDefinitionException e) {
			fail(StringUtils.stringifyException(e));
		}

		return jobGraph;
	}

}
