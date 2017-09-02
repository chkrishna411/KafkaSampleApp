package com.myapp.kafka.clients.main;

import java.util.Scanner;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.myapp.kafka.clients.consumer.MessageConsumer;
import com.myapp.kafka.clients.producers.Producer;
import com.myapp.kafka.clients.producers.ProducerFactory;

public class MessageHandler {

	private static final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
	
	public void start() {
		
		boolean loopStatus = true;
		Scanner scanner = null;
		
		while(loopStatus) {
			defaultMessage();
			scanner = new Scanner(System.in);
			int selection = scanner.nextInt();
			if (selection == -1) {
				logger.info("Exiting...");
				loopStatus = false;
			}
			callSelection(selection);
		}
		if(scanner != null) {
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
			sampleConsumer();
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
		System.out.println("4. Sample Consumer");
		System.out.print("Enter your option: ");
	}
	
	
	public void callProducer(Integer selection) {
		
		Producer producer = ProducerFactory.resolveProducer(selection);
		IntStream.range(1, 10).forEach(i -> producer.sendMessage(i,"message sending:" + i));
		producer.closeProducer();
	}

	public void sampleConsumer() {

		MessageConsumer consumer = new MessageConsumer();
		consumer.consumeMessage();
	}
	
}
