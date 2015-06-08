package com.driedtoast.stream.decoders;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import com.driedtoast.stream.StreamDecoder;
import com.driedtoast.stream.StreamEncoder;

public class StringEncoderDecoder implements StreamDecoder<String>, StreamEncoder<String> {
	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
	
	@Override
	public String decode(ByteBuffer bytes) {		
		return decodeString(bytes);
	}
	
	protected String decodeString(ByteBuffer bytes) {
		String results = null;
		try {
			results = decoder.decode(bytes).toString();
		} catch (CharacterCodingException e) {
			// TODO log this
		}
		return results;
	}

	@Override
	public ByteBuffer encode(String value) {
		try {
			return Charset.forName("UTF-8").newEncoder().encode(CharBuffer.wrap(value));
		} catch (CharacterCodingException e) {
			// TODO log or throw this 
			e.printStackTrace();
		}
		return null;
	}

}
