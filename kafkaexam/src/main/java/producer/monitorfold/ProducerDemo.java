package producer.monitorfold;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by matt on 6/3/16.
 */
public class ProducerDemo {
	public static void main(String[] args) {
		if (args.length != 8) {
			System.out.println("The number of parameters is ：" + args.length + "\n");
			System.err.println(
					"please input <topic><isAsync><brokerlist><isDeleteFile><foldPath><partition><threadNum><isMonitor" +
							">");
			System.exit(0);
		}
		String topic = args[0];
		boolean isAsync = !args[1].trim().equalsIgnoreCase("sync");
		String brokerList = args[2];
		Boolean isDeleteFiles = args[3].equals("1") || args[3].trim().equalsIgnoreCase("true");
		int threadNum = Integer.parseInt(args[6]);
		Boolean isMonitor = args[7].equals("1") || args[7].trim().equalsIgnoreCase("true");
		if (threadNum == 1) { // 单线程时
			String foldPath = args[4];
			int partition = Integer.parseInt(args[5]);
			Producer producerThread = new Producer(topic, isAsync, brokerList, isDeleteFiles, foldPath, partition,
					isMonitor);
			producerThread.start();
		} else { // 多线程时，需要指出每个线程的foldPath和partition，中间使用","作为分割符
			ExecutorService executor = Executors.newFixedThreadPool(threadNum);
			String[] flods = args[4].split(",");
			String[] partitions = args[5].split(",");
			for (int i = 0; i < threadNum; i++) {
				Producer producer = new Producer(topic, isAsync, brokerList, isDeleteFiles, flods[i], Integer
						.parseInt(partitions[i]), isMonitor);
				executor.execute(producer);
			}
		}

	}
}
