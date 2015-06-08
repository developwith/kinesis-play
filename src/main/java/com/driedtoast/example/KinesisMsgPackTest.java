package com.driedtoast.example;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;

import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.InputStreamBufferInput;
import org.msgpack.value.holder.ValueHolder;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.driedtoast.stream.SimpleStreamer;
import com.driedtoast.stream.StreamDecoder;
import com.driedtoast.stream.StreamEncoder;
import com.driedtoast.stream.StreamItemService;
import com.driedtoast.stream.StreamService;
import com.driedtoast.stream.decoders.MsgPackEncoderDecoder;

public class KinesisMsgPackTest {
	
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

			final MsgPackEncoderDecoder decoderEncoder = new MsgPackEncoderDecoder();
			SimpleStreamer<MessageUnpacker, MessagePacker> streamer = new SimpleStreamer<MessageUnpacker, MessagePacker>() {

				public StreamEncoder<MessagePacker> encoder() {
					return decoderEncoder;
				}
				
				public StreamDecoder<MessageUnpacker> decoder() {
					return decoderEncoder;
				}
				
				@Override
				public void consume(MessageUnpacker message) {
					try {
						System.out.println( " MESSAGE HAS NEXT: " + message.hasNext());
						while(message.hasNext()) {
							MessageFormat format = message.getNextFormat();
							ValueHolder valueHolder = new ValueHolder();
							format = message.unpackValue(valueHolder);
							switch(format.getValueType()) {
							case STRING:
								System.out.println("STRING : "+ valueHolder.get().asString());
								break;
							case INTEGER:
								System.out.println("INTEGER : "+ valueHolder.get().asInteger().asInt());
								break;
							default:
								System.out.println("GOT TYPE " + format.getValueType());								
							}
							
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					System.out.println("Getting message " + message.getTotalReadBytes());					
				}

				@Override
				public String consumerName() {
					return "testDanConsumer";
				}
				
			};
			StreamItemService<MessageUnpacker, MessagePacker> itemService = new StreamItemService<MessageUnpacker, MessagePacker>(kinesis, streamer);
			itemService.startConsuming(streamName, streamer);

			// So bad, but gives consumers time to get setup
			System.out.println(" How many messages to send?");
			BufferedInputStream input = new BufferedInputStream(System.in);
			int readLen = 0;
			while((readLen = input.available()) <=0) {
				Thread.sleep(100);
			}
			byte[] readBytes = new byte[readLen];
			input.read(readBytes);
			String maxString = new String(readBytes).trim();
			if(maxString.matches("\\d+")) {
				int maxRecords = Integer.parseInt(maxString);
				for(int i = 0; i < maxRecords; i++) {
				  long start = System.currentTimeMillis();
				  MessagePacker packer = new MessagePacker(decoderEncoder.newMessageOutput());
				  packer.packString("Hello Stream");
				  packer.packInt(i);
				  itemService.put(streamName, packer, "partition-" + start);
				  System.out.println(" STREAM PUSH IS " + streamName);
				}
			}
					
		}
	}


}
