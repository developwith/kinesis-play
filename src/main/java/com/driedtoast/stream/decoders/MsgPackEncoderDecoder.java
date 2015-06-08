package com.driedtoast.stream.decoders;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.MessageBufferOutput;
import org.msgpack.core.buffer.OutputStreamBufferOutput;

import com.driedtoast.stream.StreamDecoder;
import com.driedtoast.stream.StreamEncoder;

public class MsgPackEncoderDecoder implements StreamDecoder<MessageUnpacker>, StreamEncoder<MessagePacker> {

	private ByteArrayOutputStream currentBufferStream = null;
	
	@Override
	public MessageUnpacker decode(ByteBuffer bytes) {
		return MessagePack.newDefaultUnpacker(bytes.array());
	}

	
	public MessageBufferOutput newMessageOutput() {
		currentBufferStream = new ByteArrayOutputStream();
		MessageBufferOutput currentBufferOutput = new OutputStreamBufferOutput(currentBufferStream);
		return currentBufferOutput;
	}
	
	
	@Override
	public ByteBuffer encode(MessagePacker value) {
		ByteBuffer buffer = null;
		if (currentBufferStream == null) {
			return buffer;
		}
		try {			
			value.flush();
			buffer = ByteBuffer.wrap(currentBufferStream.toByteArray());
		} catch (IOException e) {
			// TODO log or throw
			e.printStackTrace();
		}
		return buffer;
	}

}
