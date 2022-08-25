package com.example;


import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

public class MessageChunkDeserializer implements Deserializer<MessageChunk> {
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public MessageChunk deserialize(String topic, byte[] data) {
		if (data == null)
			return null;

		ByteBuffer buffer = ByteBuffer.wrap(data);
		int messageSize = buffer.getInt();
		byte[] messageByte = new byte[messageSize];
		buffer.get(messageByte);
		String imageName = new String(messageByte);

		long ts = buffer.getLong();
		int totalParts = buffer.getInt();
		int index = buffer.getInt();

		int byteSize = buffer.getInt();
		byte[] bytes = new byte[byteSize];
		buffer.get(bytes);

		return new MessageChunk(imageName, ts, totalParts, index, bytes);
	}

	@Override
	public void close() {

	}
}