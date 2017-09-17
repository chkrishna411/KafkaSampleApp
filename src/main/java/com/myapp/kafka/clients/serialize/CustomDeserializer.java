package com.myapp.kafka.clients.serialize;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myapp.kafka.clients.domain.Employee;

public class CustomDeserializer implements Deserializer<Employee> {

	private static final Logger logger = LoggerFactory.getLogger(CustomDeserializer.class);
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public Employee deserialize(String topic, byte[] data) {
		
		logger.info("Custom DeSerialization in process");
		Employee emp = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream inputStream = new ObjectInputStream(bis);
			emp = (Employee)inputStream.readObject();		
			inputStream.close();
			bis.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return emp;
	}

	@Override
	public void close() {
	}
	
}
