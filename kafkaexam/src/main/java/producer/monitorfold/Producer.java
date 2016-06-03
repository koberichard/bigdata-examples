package producer.monitorfold;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by matt on 6/2/16.
 */
public class Producer extends Thread {
	Logger logger = LoggerFactory.getLogger(Producer.class); // 记录日志

	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Boolean isAsync;
	private final Boolean isDeleteFiles;
	private final String foldPath;
	private final int partition;
	private final Boolean isMonitor;

	public Producer(String topic, Boolean isAsync, String brokerList, Boolean isDeleteFiles, String foldPath, int
			partition, Boolean isMonitor) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("client.id", KafkaProperties.CLIENT_ID);
		props.put("key.serializer", KafkaProperties.KEY_SER);
		props.put("value.serializer", KafkaProperties.VALUE_SER);
		props.put("acks", KafkaProperties.ACKS);
		props.put("retries", KafkaProperties.RETRIES);
		props.put("buffer.memory", KafkaProperties.BUFFER_MEMORY);
		props.put("compression.type", KafkaProperties.COMPRESSION_TYPE);
		props.put("batch.size", KafkaProperties.BATCH_SIZE);
		props.put("max.request.size", KafkaProperties.MAX_REQUEST_SIZE);
		props.put("partitioner.class", KafkaProperties.PARTITIONER_CLASS);
		props.put("receive.buffer.bytes", KafkaProperties.RECEIVE_BUFFER_BYTES);
		props.put("send.buffer.bytes", KafkaProperties.SEND_BUFFER_BYTES);

		producer = new KafkaProducer<>(props);
		this.topic = topic;
		this.isAsync = isAsync;
		this.isDeleteFiles = isDeleteFiles;
		this.foldPath = foldPath;
		this.partition = partition;
		this.isMonitor = isMonitor;
	}

	public void run() {
		BufferedReader reader = null;
		File fold = new File(foldPath);
		if (fold.exists()) {
			while (isMonitor) {
				long fileSize = 0l;
				File[] files = fold.listFiles();
				if (files.length == 0) {
					logger.warn("The fold \"" + foldPath + "\" does not have any files");
					System.exit(0);
				} else {
					long start_time = System.currentTimeMillis();
					logger.info("The process start at " + start_time + ".");
					for (File file : files) {
						sendFile(file, fileSize, reader); // send the file
					}
					long end_time = System.currentTimeMillis();
					logger.info("The process end at " + end_time + ".");
					long timer = (end_time - start_time) / 1000;
					double in_size = fileSize / (1024 * 1024);
					double speed = in_size / timer;
					logger.info("The time of the process is " + timer + ".");
					logger.info("The speed of the process is (MB/s): " + speed);
				}
			}
		} else {
			logger.error("The fold does not exist!");
		}
	}

	/**
	 * send the file
	 *
	 * @param file     the file need to send
	 * @param fileSize the size of all files of the fold
	 * @param reader   BufferedReader
	 */
	public void sendFile(File file, long fileSize, BufferedReader reader) {
		fileSize += file.length();
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
		} catch (Exception e) {
			logger.error("There is an error during the read the file.");
			e.printStackTrace();
			System.exit(0);
		}
		while (true) {
			String line = null;
			try {
				line = reader.readLine();
			} catch (IOException e) {
				logger.error("The file can not read!");
				e.printStackTrace();
				System.exit(0);
			}
			if (line == null)
				break;
			if (isAsync) { // 异步发送
				producer.send(new ProducerRecord<>(topic, partition, line));
			} else { // 同步发送
				try {
					producer.send(new ProducerRecord<>(topic, partition, line)).get(); // 通过get方法进行阻塞
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		String time = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
				.format(new java.util.Date(System.currentTimeMillis()));
		logger.info("The file \"" + file.getName() + "\" has sent at " + time + ".");
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (isDeleteFiles) { // 可以设置是否删除已经读取完成的文件
			file.delete();
		}
	}
}
