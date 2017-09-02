package com.myapp.kafka.clients.producers;


import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleSyncProducer implements Producer {

	private KafkaProducer<String, String> producer = null;
	
	private static final Logger logger = LoggerFactory.getLogger(SampleSyncProducer.class);
	
	public SampleSyncProducer() {
		
		logger.info(" ==== Sample Sync producer running ===== ");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);		
	}
	
	public void sendMessage(String key, String value) {
		
		ProducerRecord<String, String> record = new ProducerRecord<>("accountTopic", key, value);
		Future<RecordMetadata> metaDataFuture = producer.send(record);
		this.addDelay();
		try {
			RecordMetadata metadata = metaDataFuture.get();
			logger.info("Message sent to Topic:"+metadata.topic() + " offset:"
						+metadata.offset() + " partition:"+metadata.partition());
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	public void closeProducer() {
		if(producer != null) 
			producer.close();
	}
}
