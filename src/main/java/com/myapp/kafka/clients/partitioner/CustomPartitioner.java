package com.myapp.kafka.clients.partitioner;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author chkrishna
 * 
 * Custom Partitioner
 *
 */
public class CustomPartitioner implements Partitioner {

	private static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);
	
	private static final List<Integer> intList = Stream.of(1,2,3,4,5,6).collect(Collectors.toList());
	
	@Override
	public void configure(Map<String, ?> configs) {
		
	}

	/**
	 * Choose a Random algorithm, to figure out the proper partition
	 */
	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

		List<PartitionInfo> partitionList = cluster.availablePartitionsForTopic("accountTopic");
		logger.info(" partitions size from Custom partitioner: "+partitionList.size());
		logger.info("key received for sending message: "+key);
		boolean partitionStatus = this.checkKeyStatus(key);
		if(partitionStatus) {
			return 0;
		}
		else {
			return Utils.toPositive(Utils.murmur2(keyBytes)) % partitionList.size();
		}
	}
	
	/**
	 * This will check if the exists in the defined list, then returns true
	 * @param key
	 * @return
	 */
	private boolean checkKeyStatus(Object key) {
		
		Integer keyReceived = Integer.parseInt(String.valueOf(key));
		if(intList.contains(keyReceived)) {
			logger.info("Choosing same partition for testing purposes");
			return true;
		}
		 return false;
	}

	@Override
	public void close() {
		
	}
}
