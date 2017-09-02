package com.myapp.kafka.clients.producers;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myapp.kafka.clients.callback.ProducerCallback;

public class SampleAsyncProducer implements Producer {

	private KafkaProducer<String, String> producer = null;
	
	private static final Logger logger = LoggerFactory.getLogger(SampleAsyncProducer.class);
	
	public SampleAsyncProducer() {
		
		logger.info(" ==== Sample Async producer running ===== ");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<>(props);		
	}
	

	public void sendMessage(String key, String value) {
		
		ProducerRecord<String, String> record = new ProducerRecord<>("accountTopic", key, value);
		producer.send(record, new ProducerCallback());
		this.addDelay();
	}
	
	
	public void closeProducer() {
		if(producer != null) 
			producer.close();
	}
}
