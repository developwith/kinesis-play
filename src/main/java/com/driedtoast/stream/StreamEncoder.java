package com.driedtoast.stream;

import java.nio.ByteBuffer;

public interface StreamEncoder<E> {

	ByteBuffer encode(E value);
	
}
