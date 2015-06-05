package com.driedtoast.example;

public enum StreamStatus {
	DELETING,
	ACTIVE;
	
	public boolean is(String value) {
	  return name().equals(value);
	}
}
