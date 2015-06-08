package com.driedtoast.stream;

public enum StreamStatus {
	DELETING,
	ACTIVE;
	
	public boolean is(String value) {
	  return name().equals(value);
	}
}
