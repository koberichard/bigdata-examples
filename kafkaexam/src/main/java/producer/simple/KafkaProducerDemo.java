package producer.simple;

/**
 * SimpleProducer 启动
 * Created by matt on 6/2/16.
 */
public class KafkaProducerDemo {
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("参数总共有：" + args.length + "\n");
			System.err.println(
					"please input <topic><isAsync><brokerlist>");
			System.exit(0);
		}
		String topic = args[0];
		boolean isAsync = !args[1].trim().equalsIgnoreCase("sync");
		String brokerList = args[2];
		Producer producerThread = new Producer(topic, isAsync, brokerList);
		producerThread.start();
	}
}
