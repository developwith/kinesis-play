package com.driedtoast.stream;

import java.nio.ByteBuffer;

public interface StreamDecoder<T> {

	T decode(ByteBuffer bytes);
	
}
