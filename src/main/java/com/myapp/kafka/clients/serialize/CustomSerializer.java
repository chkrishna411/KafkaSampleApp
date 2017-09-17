package com.myapp.kafka.clients.serialize;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myapp.kafka.clients.domain.Employee;

public class CustomSerializer implements Serializer<Employee> {

	private static final Logger logger = LoggerFactory.getLogger(CustomSerializer.class);
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public byte[] serialize(String topic, Employee data) {
		
		logger.info("Custom Serialization in process");
		ByteArrayOutputStream bos = null;
		ObjectOutputStream outputStream = null;
		byte[] serializedBytes = null;
		try {
			bos = new ByteArrayOutputStream();
			outputStream = new ObjectOutputStream(bos);
			outputStream.writeObject(data);
			outputStream.flush();
			serializedBytes = bos.toByteArray();
			bos.close();
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return serializedBytes;
	}

	@Override
	public void close() {
		
	}
	
	
}
