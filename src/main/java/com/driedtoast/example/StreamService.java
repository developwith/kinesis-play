package com.driedtoast.example;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;

public class StreamService {

	private AmazonKinesisClient client;

	public static final int STREAM_LIMIT = 10;
	public static final int SHARD_LIMIT = 5;

	public StreamService(AmazonKinesisClient client) {
		this.client = client;
	}

	public List<String> list() {
		// List all of my streams.
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		listStreamsRequest.setLimit(STREAM_LIMIT);
		ListStreamsResult listStreamsResult = client.listStreams(listStreamsRequest);
		List<String> streamNames = listStreamsResult.getStreamNames();
		while (listStreamsResult.isHasMoreStreams()) {
			if (streamNames.size() > 0) {
				listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
			}

			listStreamsResult = client.listStreams(listStreamsRequest);
			streamNames.addAll(listStreamsResult.getStreamNames());
		}
		return streamNames;
	}

	public void delete(String streamName) {
		try {
			client.deleteStream(streamName);
		} catch (ResourceNotFoundException ex) {
			// The stream doesn't exist.
		}
	}

	public DescribeStreamResult findOrCreate(String name) throws Exception {
		// Describe the stream and check if it exists.
		DescribeStreamResult result = null;
		try {
			result = find(name);
		} catch (ResourceNotFoundException ex) {
			System.out.printf("Stream %s does not exist. Creating it now.\n", name);
			create(name, true);
		}
		return result;
	}

	public DescribeStreamResult create(String name, boolean wait) throws Exception {
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(name);
		createStreamRequest.setShardCount(SHARD_LIMIT);
		client.createStream(createStreamRequest);
		// The stream is now being created. Wait for it to become active.
		if (wait) {
			return findAndWait(name);
		}
		return find(name);
	}

	public DescribeStreamResult find(String name) {
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(name);
		// ask for no more than 10 shards at a time -- this is an
		// optional parameter
		describeStreamRequest.setLimit(SHARD_LIMIT);
		DescribeStreamResult describeStreamResponse = client.describeStream(describeStreamRequest);
		String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
		if (StreamStatus.DELETING.is(streamStatus)) {
			describeStreamResponse = null;
		}
		return describeStreamResponse;
	}

	public DescribeStreamResult findAndWait(final String name) throws Exception {
		FutureTask<DescribeStreamResult> result = new FutureTask<DescribeStreamResult>(new Callable<DescribeStreamResult>() {
			public DescribeStreamResult call() {
				DescribeStreamResult describeStreamResponse = find(name);
				String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
				while (!StreamStatus.ACTIVE.is(streamStatus) && !StreamStatus.DELETING.is(streamStatus)) {
					describeStreamResponse = find(name);
					streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
				}
				return describeStreamResponse;
			}
		});

		return result.get(60, TimeUnit.SECONDS);
	}

}
