package com.myapp.kafka.clients.producers;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myapp.kafka.clients.domain.Employee;
import com.myapp.kafka.clients.serialize.CustomSerializer;



public class EmployeeProducer implements Producer {

	private KafkaProducer<String, Employee> producer = null;
	
	private static final Logger logger = LoggerFactory.getLogger(EmployeeProducer.class);
	
	public EmployeeProducer() {
		
		logger.info(" ==== Sample Fire and Forget producer running ===== ");
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", CustomSerializer.class.getName());
		
		producer = new KafkaProducer<>(props);
	}
	
	public void sendMessage(String key, Employee emp) {
		
		ProducerRecord<String, Employee> record = new ProducerRecord<>("accountTopic", key, emp);
		producer.send(record);
		logger.info("Employee record sent successfully");
		this.addDelay();
	}
	
	public void closeProducer() {
		if(producer != null) 
			producer.close();
	}

	@Override
	public void sendMessage(String key, String value) {
		// TODO Auto-generated method stub
		
	}
}
