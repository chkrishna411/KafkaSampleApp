package com.myapp.kafka.clients.producers;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SampleProducer implements Producer {

	private KafkaProducer<String, String> producer = null;
	
	private static final Logger logger = LoggerFactory.getLogger(SampleProducer.class);
	
	public SampleProducer() {
		
		logger.info(" ==== Sample Fire and Forget producer running ===== ");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);
	}
	
	public void sendMessage(String key, String value) {
		
		ProducerRecord<String, String> record = new ProducerRecord<>("accountTopic", key, value);
		producer.send(record);
		logger.info("Message sent successfully");
		this.addDelay();
	}
	
	public void closeProducer() {
		if(producer != null) 
			producer.close();
	}
}
