package producer.monitorfold;

import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 重写了partitioner，可以通过key值指定发往哪个partition
 * Created by matt on 6/2/16.
 */
public class Partitionerwm implements Partitioner {

	public Partitionerwm(VerifiableProperties props) {

	}

	public int partition(Object key, int a_numPartitions) {
		int partition = 0;
		String stringKey = (String) key;

		partition = Integer.parseInt(stringKey) % a_numPartitions;
		return partition;
	}

	private final AtomicInteger counter = new AtomicInteger(new Random().nextInt());

	/**
	 * A cheap way to deterministically convert a number to a positive value. When the input is
	 * positive, the original value is returned. When the input number is negative, the returned
	 * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
	 * value.
	 * <p/>
	 * Note: changing this method in the future will possibly cause partition selection not to be
	 * compatible with the existing messages already placed on a partition.
	 *
	 * @param number a given number
	 * @return a positive number.
	 */
	private static int toPositive(int number) {
		return number & 0x7fffffff;
	}

	@Override
	public void configure(Map<String, ?> configs) {
	}

	/**
	 * Compute the partition for the given record.
	 *
	 * @param topic      The topic name
	 * @param key        The key to partition on (or null if no key)
	 * @param keyBytes   serialized key to partition on (or null if no key)
	 * @param value      The value to partition on or null
	 * @param valueBytes serialized value to partition on or null
	 * @param cluster    The current cluster metadata
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		if (keyBytes == null) {
			int nextValue = counter.getAndIncrement();
			List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
			if (availablePartitions.size() > 0) {
				int part = Partitionerwm.toPositive(nextValue) % availablePartitions.size();
				return availablePartitions.get(part).partition();
			} else {
				// no partitions are available, give a non-available partition
				return Partitionerwm.toPositive(nextValue) % numPartitions;
			}
		} else {
			String stringKey = (String) key;

			return Integer.parseInt(stringKey) % numPartitions; // 只重写了这一部分
		}
	}

	@Override
	public void close() {
	}

}
