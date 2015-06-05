package com.driedtoast.example;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;
import java.util.UUID;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;

public class StreamItemService implements IRecordProcessor, IRecordProcessorFactory {
	private AmazonKinesisClient client;
	private String shardId;
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	private ProfileCredentialsProvider credentialsProvider;
	private SimpleStreamConsumer consumer;

	public StreamItemService(AmazonKinesisClient client) {
		this.client = client;
	}

	public StreamItemService(SimpleStreamConsumer consumer) {
		this.consumer = consumer;
	}

	public PutRecordResult put(String streamName, String message, String partition) {
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(streamName);
		putRecordRequest.setData(ByteBuffer.wrap(message.getBytes()));
		putRecordRequest.setPartitionKey(partition);
		PutRecordResult putRecordResult = client.putRecord(putRecordRequest);
		return putRecordResult;
	}

	public void startConsuming(String streamName, SimpleStreamConsumer consumer) {
		this.consumer = consumer;
		String workerId = UUID.randomUUID().toString();
		KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(consumer.consumerName(), streamName, getProvider(),
				workerId);
		kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.LATEST);

		IRecordProcessorFactory recordProcessorFactory = this;
		Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

		worker.run();
	}

	@Override
	public IRecordProcessor createProcessor() {
		return new StreamItemService(consumer);
	}

	@Override
	public void initialize(String shardId) {
		this.shardId = shardId;
	}

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		for (Record record : records) {
			try {
				// TODO add retry and blocking
				consumer.consume(decoder.decode(record.getData()).toString());				
			} catch (Exception e) {
				// TODO log this
				e.printStackTrace();
			}
		}
		try {
			checkpointer.checkpoint();
		} catch (Exception e) {
			
		}
	}

	@Override
	public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
		if (reason == ShutdownReason.TERMINATE) {
			try {
				checkpointer.checkpoint();
			} catch (Exception e) {
				// TODO handle
				e.printStackTrace();
			}
		}
	}

	private ProfileCredentialsProvider getProvider() {
		if (credentialsProvider != null)
			return credentialsProvider;
		credentialsProvider = new ProfileCredentialsProvider();
		return credentialsProvider;
	}

}
