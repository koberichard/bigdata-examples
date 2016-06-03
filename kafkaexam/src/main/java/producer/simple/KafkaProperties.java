package producer.simple;

/**
 * Created by matt on 6/2/16.
 */
public class KafkaProperties {
	public static final String TOPIC = "topic1";
	public static final String KAFKA_SERVER_URL = "datanode71";
	public static final int KAFKA_SERVER_PORT = 9092;
	public static final int KAFKA_PRODUCER_BUFFER_SIZE = 64 * 1024;
	public static final int CONNECTION_TIMEOUT = 100000;
	public static final String CLIENT_ID = "SimpleConsumerDemoClient";

	private KafkaProperties() {
	}
}
