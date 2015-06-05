package com.driedtoast.example;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;

public class KinesisTest {

	private static AmazonKinesisClient kinesis;

	private static void init() throws Exception {
		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (~/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider().getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct " + "location (~/.aws/credentials), and is in valid format.", e);
		}

		kinesis = new AmazonKinesisClient(credentials);
	}

	public static void main(String[] args) throws Exception {
		init();

		StreamService service = new StreamService(kinesis);
		DescribeStreamResult result = service.findOrCreate("testDanStream");

		List<String> streamNames = service.list();
		// Print all of my streams.
		System.out.println("List of my streams: ");
		for (int i = 0; i < streamNames.size(); i++) {
			System.out.println("\t- " + streamNames.get(i));
		}

		if (!streamNames.isEmpty()) {
			String streamName = streamNames.get(0);

			long start = System.currentTimeMillis();
			StreamItemService itemService = new StreamItemService(kinesis);
			itemService.put(streamNames.get(0), "Hello Stream", "partition-" + start);

			itemService.startConsuming(streamName, new SimpleStreamConsumer() {

				@Override
				public void consume(String message) {
					System.out.println("Gettting message " + message);					
				}

				@Override
				public String consumerName() {
					return "testDanConsumer";
				}
				
			});
			
		}
	}

}
