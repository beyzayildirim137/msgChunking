package com.example;

import java.nio.ByteBuffer;

import org.apache.kafka.common.serialization.Serializer;

public class MessageChunkSerializer implements Serializer<MessageChunk> {

    @Override
    public byte[] serialize(String topic, MessageChunk data) {
        if (data == null)
			return null;

        byte[] MessageBytes = data.getMessage().getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(4 + MessageBytes.length + 8 + 4 + 4 + 4 + data.getBytes().length);

        buffer.putInt(MessageBytes.length);
		buffer.put(MessageBytes);
		buffer.putLong(data.getTimestamp());
		buffer.putInt(data.getTotalPart());
		buffer.putInt(data.getIndex());
		buffer.putInt(data.getBytes().length);
		buffer.put(data.getBytes());

		return buffer.array();
            
    }

    @Override
	public void close() {
	}
    
}
