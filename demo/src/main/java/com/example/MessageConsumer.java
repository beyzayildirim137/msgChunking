package com.example;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;

public class MessageConsumer 
{
    private static final String TOPIC_NAME = "chucked-message";
    public static void main( String[] args )
    {
        BasicConfigurator.configure();
        // broker configure
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.UUIDDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.example.MessageChunkDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "chucked-message");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        Consumer<UUID, MessageChunk> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singleton(TOPIC_NAME));
		Map<UUID, List<MessageChunk>> dictionary = new HashMap<>();

        while (true) {        // infinite loop
			// subscribe topic
			consumer
					.poll(Duration.ofMillis(1000))
					.records(TOPIC_NAME)
					.forEach(record -> {
						UUID key = record.key();
						if (!dictionary.containsKey(key))
							dictionary.put(key, new ArrayList<>());
						dictionary.get(key).add(record.value());
					});

			// merge messages
			try {
                MergeChunks(dictionary);
            } catch (IOException e) {
                e.printStackTrace();
            }
			consumer.commitSync(Duration.ofMillis(2000));
		} 
    }

    private static void MergeChunks(Map<UUID, List<MessageChunk>> dictionary) throws IOException {
		for (UUID key : dictionary.keySet()) {
			List<MessageChunk> msgChunks = dictionary.get(key);
			int totalPart = msgChunks.get(0).getTotalPart();
			if (totalPart != msgChunks.size())
				continue;

			// sort by index
			msgChunks.sort(Comparator.comparingInt(MessageChunk::getIndex));
			int totalByteSize = msgChunks.stream()
					.mapToInt(msgChunk -> msgChunk.getBytes().length)
					.sum();
			byte[] bytes = new byte[totalByteSize];

			// merge bytes
			int offset = 0;
			for (MessageChunk msgChunk : msgChunks) {
				byte[] msgBytes = msgChunk.getBytes();
				System.arraycopy(msgBytes, 0, bytes, offset, msgBytes.length);
				offset += msgBytes.length;
			}

			// write file
			printLog(msgChunks, bytes);

			dictionary.remove(key);
		}
	}

    private static void printLog(List<MessageChunk> msgChunks, byte[] bytes) throws IOException {
		String msg = msgChunks.get(0).getMessage();
		System.out.println("Message produced = "+msg);
		System.out.println("* size : " + bytes.length);
	}
}
