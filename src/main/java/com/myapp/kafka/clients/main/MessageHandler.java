package com.myapp.kafka.clients.main;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.myapp.kafka.clients.consumer.EmployeeConsumer;
import com.myapp.kafka.clients.consumer.KafkaHighLevelConsumer;
import com.myapp.kafka.clients.consumer.MessageConsumer;
import com.myapp.kafka.clients.domain.Employee;
import com.myapp.kafka.clients.producers.EmployeeProducer;
import com.myapp.kafka.clients.producers.Producer;
import com.myapp.kafka.clients.producers.ProducerFactory;

public class MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);

	public void start() {

		boolean loopStatus = true;
		Scanner scanner = null;

		while (loopStatus) {
			defaultMessage();
			scanner = new Scanner(System.in);
			int selection = scanner.nextInt();
			if (selection == -1) {
				logger.info("Exiting...");
				loopStatus = false;
			}
			callSelection(selection);
		}
		if (scanner != null) {
			scanner.close();
		}
	}

	private void callSelection(int selection) {
		switch (selection) {
		case 1:
			callProducer(selection);
			break;
		case 2:
			callProducer(selection);
			break;
		case 3:
			callProducer(selection);
			break;
		case 4:
			callEmployeeProducer(selection);
			break;
		case 5:
			sampleConsumer();
			break;
		case 6:
			highLevelConsumer();
			break;
		case 7:
			employeeConsumer();
			break;
		case -1:
			break;
		default:
			defaultMessage();
		}
	}

	private void defaultMessage() {
		System.out.println("====================================");
		System.out.println(" Select an Option, Enter -1 to exit");
		System.out.println("1. Sample Producer");
		System.out.println("2. Synchronized Producer");
		System.out.println("3. Async Callback Producer");
		System.out.println("4. Call Employee Producer");
		System.out.println("5. Sample Consumer");
		System.out.println("6. High level Consumer");
		System.out.println("7. Start Employee Consumer");
		System.out.print("Enter your option: ");
	}

	public void callProducer(Integer selection) {
		
		Producer producer = ProducerFactory.resolveProducer(selection);
		Gson gson = new Gson();
		for(int i =1; i <=10; i++) {
			Employee emp = new Employee();
			emp.setId(i);
			emp.setName("steve");
			String strEmployee = gson.toJson(emp);
			logger.info("Sending Employee in json format: "+strEmployee);
			producer.sendMessage(i,strEmployee);
		}
		producer.closeProducer();
	}

	public void callEmployeeProducer(Integer selection) {
		EmployeeProducer producer = new EmployeeProducer();

		Employee emp = new Employee();
		emp.setId(100);
		emp.setName("steve");
		producer.sendMessage(String.valueOf(selection), emp);
	}
	
	public void sampleConsumer() {

		MessageConsumer consumer = new MessageConsumer();
		consumer.consumeMessage();
	}

	public void employeeConsumer() {

		EmployeeConsumer consumer = new EmployeeConsumer();
		consumer.consumeMessage();
	}

	public void highLevelConsumer() {

		KafkaHighLevelConsumer consumer = new KafkaHighLevelConsumer();
		consumer.startConsumers();
	}

}
