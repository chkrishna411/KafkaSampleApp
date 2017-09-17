package com.myapp.kafka.clients.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

public class KafkaHighLevelConsumer {

	private static Integer numberOfPartitions = 5;

	private static final String topicName = "accountTopic";

	private static final Logger logger = LoggerFactory.getLogger(KafkaHighLevelConsumer.class);

	private ConsumerConnector connector;

	private ExecutorService executorService;

	public ConsumerConfig configProps() {

		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "accountSecondGroup");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.timeout.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("enable.auto.commit", "true");
		return new ConsumerConfig(props);
	}

	public void startConsumers() {

		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topicName, numberOfPartitions);

		connector = Consumer.createJavaConsumerConnector(this.configProps());

		Map<String, List<KafkaStream<String, String>>> consumerMap = connector.createMessageStreams(topicCountMap,
				new StringDecoder(null), new StringDecoder(null));

		List<KafkaStream<String, String>> streams = consumerMap.get(topicName);

		logger.info("Kafka Number of streams create: " + streams.size());
		executorService = Executors.newFixedThreadPool(numberOfPartitions);

		int threadNumber = 0;
		for (KafkaStream<String, String> stream : streams) {

			executorService.submit(new ConsumerThread(stream, threadNumber));
			threadNumber++;
		}

	}

	public void shutdown() {

		if (connector != null) {
			connector.shutdown();
		}
		if (executorService != null) {
			executorService.shutdown();
		}

	}

	class ConsumerThread implements Runnable {

		private KafkaStream<String, String> stream;
		private int threadNumber;

		public ConsumerThread(KafkaStream<String, String> stream, int threadNumber) {
			this.stream = stream;
			this.threadNumber = threadNumber;
		}

		@Override
		public void run() {
			logger.info("Consumer Thread starting:: " + threadNumber);
			ConsumerIterator<String, String> consumerIterator = stream.iterator();
			while (consumerIterator.hasNext()) {
				String requestMessage = consumerIterator.next().message();
				logger.info("Request message consuming:" + requestMessage + " with thread: " + threadNumber);
			}
			
		}

	}

}

