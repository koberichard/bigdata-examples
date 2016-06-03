package producer.monitorfold;

/**
 * 可以进行手动配置参数
 * Created by matt on 6/2/16.
 */
public class KafkaProperties {
	public static final String BOOTSTRAP_SERVERS = "datanode71:9092,datanode72:9092,datanode73:9092";
	public static final String CLIENT_ID = "MonitorProducer";
	public static final String KEY_SER = "org.apache.kafka.common.serialization.IntegerSerializer";
	public static final String VALUE_SER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String ACKS = "1";
	public static final int RETRIES = 0;
	public static final long BUFFER_MEMORY = 33554432;
	public static final String COMPRESSION_TYPE = "none";
	public static final int BATCH_SIZE = 16384;
	public static final int MAX_REQUEST_SIZE = 1048576;
	public static final String PARTITIONER_CLASS = "producer.monitorfold.Partitionerwm";
	public static final int RECEIVE_BUFFER_BYTES = 32768;
	public static final int SEND_BUFFER_BYTES = 131072;

	private KafkaProperties() {
	}
}
