package com.driedtoast.example;

import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.driedtoast.stream.SimpleStreamer;
import com.driedtoast.stream.StreamDecoder;
import com.driedtoast.stream.StreamEncoder;
import com.driedtoast.stream.StreamItemService;
import com.driedtoast.stream.StreamService;
import com.driedtoast.stream.decoders.StringEncoderDecoder;

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
		// Setup initial stream
		service.findOrCreate("testDanStream");

		List<String> streamNames = service.list();
		// Print all of my streams.
		System.out.println("List of my streams: ");
		for (int i = 0; i < streamNames.size(); i++) {
			System.out.println("\t- " + streamNames.get(i));
		}

		if (!streamNames.isEmpty()) {
			String streamName = streamNames.get(0);

			final StringEncoderDecoder decoderEncoder = new StringEncoderDecoder();
			SimpleStreamer<String, String> streamer = new SimpleStreamer<String, String>() {

				public StreamEncoder<String> encoder() {
					return decoderEncoder;
				}
				
				public StreamDecoder<String> decoder() {
					return decoderEncoder;
				}
				
				@Override
				public void consume(String message) {
					System.out.println("Getting message " + message);					
				}

				@Override
				public String consumerName() {
					return "testDanConsumer";
				}
				
			};
			StreamItemService<String, String> itemService = new StreamItemService<String, String>(kinesis, streamer);
			itemService.startConsuming(streamName, streamer);

			Thread.sleep(1500);
			
			int max_records = 50;
			for(int i = 0; i < max_records; i++) {
			  long start = System.currentTimeMillis();
			  itemService.put(streamName, "Hello Stream" + i, ", partition-" + start);
			  System.out.println(" STREAM PUSH IS " + streamName);
			}

						
		}
	}

}
