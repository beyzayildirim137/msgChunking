package com.example;

public class MessageChunk {
    private String msgName;
	private long timestamp;
	private int totalPart;
	private int index;
	private byte[] bytes;

    public MessageChunk(String msgName, long timestamp, int totalPart, int index, byte[] bytes) {
		this.msgName = msgName;
		this.timestamp = timestamp;
		this.totalPart = totalPart;
		this.index = index;
		this.bytes = bytes;
	}

	public String getMessage() {
		return msgName;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getTotalPart() {
		return totalPart;
	}

	public int getIndex() {
		return index;
	}

	public byte[] getBytes() {
		return bytes;
	}

}
