package com.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.BasicConfigurator;

public class MessageProducer 
{
    private static final String TOPIC_NAME = "chucked-message";
    private static final String MSG_TO_SEND = "Hello Hello!";
    private static final int MESSAGE_SIZE = 100;
    public static void main( String[] args ) throws InterruptedException, ExecutionException
    {
        BasicConfigurator.configure();
        // broker configure
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.UUIDSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.example.MessageChunkSerializer");
        

        byte[] bytes = MSG_TO_SEND.getBytes();
        System.out.println("== total message size : " + bytes.length);
        System.out.println("BYTES: "+bytes);
        if (bytes.length == 0)
			return;

        // chop the large message
		// to small chunks that under `request.max.size`
        long ts = System.currentTimeMillis();
		int totalParts = (bytes.length / MESSAGE_SIZE) + 1;
		int index = 0;
        List<MessageChunk> msgChunks = new ArrayList<>();
        while (index * MESSAGE_SIZE < bytes.length) {
			int byteSize = MESSAGE_SIZE;
			if ((index + 1) * MESSAGE_SIZE > bytes.length)
				byteSize = bytes.length - (index * MESSAGE_SIZE);

			byte[] chuckBytes = new byte[byteSize];
			System.arraycopy(bytes, index * MESSAGE_SIZE, chuckBytes, 0, byteSize);
			msgChunks.add(new MessageChunk(MSG_TO_SEND, ts, totalParts, index, chuckBytes));

			index++;
		}

        Producer<UUID, MessageChunk> producer = new KafkaProducer<>(props);
        UUID uuid = UUID.randomUUID();
        for (MessageChunk msgChunk : msgChunks) {
			ProducerRecord<UUID, MessageChunk> record = new ProducerRecord<>(TOPIC_NAME, uuid, msgChunk);
			RecordMetadata recordMetadata = producer.send(record).get();
            producer.flush();
			printResult(recordMetadata);
		}
    }
    
    private static void printResult(RecordMetadata recordMetadata) {
		System.out.println("== send result");
		System.out.println("* partition  : " + recordMetadata.partition());
		System.out.println("* offset     : " + recordMetadata.offset());
		System.out.println("* timestamp  : " + recordMetadata.timestamp());
		System.out.println("* value size : " + recordMetadata.serializedValueSize());
	}
}
