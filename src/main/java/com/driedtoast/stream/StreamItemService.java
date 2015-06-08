package com.driedtoast.stream;

import java.util.List;
import java.util.UUID;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
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

public class StreamItemService<T,E> implements IRecordProcessor, IRecordProcessorFactory {
	private AmazonKinesisClient client;
    private String shardId;
	private ProfileCredentialsProvider credentialsProvider;
	private SimpleStreamer<T,E> consumer;


	public StreamItemService(AmazonKinesisClient client, SimpleStreamer<T,E> streamer) {
		this.client = client;
		this.consumer = streamer;
	}

	public StreamItemService(SimpleStreamer<T, E> consumer) {
		this.consumer = consumer;
	}

	public PutRecordResult put(String streamName, E message, String partition) {
		PutRecordRequest putRecordRequest = new PutRecordRequest();
		putRecordRequest.setStreamName(streamName);
		putRecordRequest.setData(consumer.encoder().encode(message));
		putRecordRequest.setPartitionKey(partition);
		PutRecordResult putRecordResult = client.putRecord(putRecordRequest);
		return putRecordResult;
	}

	public void startConsuming(String streamName, SimpleStreamer<T, E> consumer) {
		this.consumer = consumer;
		String workerId = UUID.randomUUID().toString();
		final KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(consumer.consumerName(), streamName, getProvider(),
				workerId);
		kinesisClientLibConfiguration.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

		final IRecordProcessorFactory recordProcessorFactory = this;
		Thread consumingThread = new Thread(new Runnable() {

			@Override
			public void run() {
				Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
				worker.run();				
			}
			
		}, "Consumer");		
		consumingThread.start();
	}

	@Override
	public IRecordProcessor createProcessor() {
		return new StreamItemService<T, E>(consumer);
	}

	@Override
	public void initialize(String shardId) {
		System.out.println("Setting up consumer for " +shardId);
		this.shardId = shardId;
	}

	@Override
	public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
		System.out.println("Processing records for shard " + shardId + " " + records.size());
		for (Record record : records) {
			try {
				// TODO add retry and blocking
				consumer.consume(consumer.decoder().decode(record.getData()));				
			} catch (Exception e) {
				// TODO log this
				e.printStackTrace();
			}
		}
		try {
			checkpointer.checkpoint();
		} catch (Exception e) {
			e.printStackTrace();
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
