package consumer.simple;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import producer.simple.KafkaProperties;
import producer.simple.Producer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by matt on 6/2/16.
 */
public class SimpleConsumerDemo {

	private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
		for (MessageAndOffset messageAndOffset : messageSet) {
			ByteBuffer payload = messageAndOffset.message().payload();
			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);
			System.out.println(new String(bytes, "UTF-8"));
		}
	}

	private static void generateData(String topic1, String topic2, String brokerList) {
		Producer producer2 = new Producer(topic1, false, brokerList);
		producer2.start();
		Producer producer3 = new Producer(topic2, false, brokerList);
		producer3.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		// start producer
		generateData(args[0],args[1],args[2]);

		SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.KAFKA_SERVER_URL,
				KafkaProperties.KAFKA_SERVER_PORT,
				KafkaProperties.CONNECTION_TIMEOUT,
				KafkaProperties.KAFKA_PRODUCER_BUFFER_SIZE,
				KafkaProperties.CLIENT_ID);

		System.out.println("Testing single fetch");
		FetchRequest req = new FetchRequestBuilder()
				.clientId(KafkaProperties.CLIENT_ID)
				.addFetch(args[0], 0, 0L, 100)
				.build();
		FetchResponse fetchResponse = simpleConsumer.fetch(req);
		printMessages(fetchResponse.messageSet(args[0], 0));

		System.out.println("Testing multi-fetch");
		Map<String, List<Integer>> topicMap = new HashMap<>();
		topicMap.put(args[0], Collections.singletonList(0));
		topicMap.put(args[1], Collections.singletonList(0));
		req = new FetchRequestBuilder()
				.clientId(KafkaProperties.CLIENT_ID)
				.addFetch(args[0], 0, 0L, 100)
				.addFetch(args[1], 0, 0L, 100)
				.build();
		fetchResponse = simpleConsumer.fetch(req);
		int fetchReq = 0;
		for (Map.Entry<String, List<Integer>> entry : topicMap.entrySet()) {
			String topic = entry.getKey();
			for (Integer offset : entry.getValue()) {
				System.out.println("Response from fetch request no: " + ++fetchReq);
				printMessages(fetchResponse.messageSet(topic, offset));
			}
		}
	}
}

