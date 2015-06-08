package com.driedtoast.stream;


public interface SimpleStreamer<T, E> {

	StreamDecoder<T> decoder();
	void consume(T message);
	String consumerName();
	
	StreamEncoder<E> encoder();
}
