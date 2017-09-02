package com.myapp.kafka.clients.main;


import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class ApplicationMain {

	private static final Logger logger = LoggerFactory.getLogger(ApplicationMain.class);
	
	public static void main(String[] args) {

		BasicConfigurator.configure();
		logger.info("App Starting....");
		
		MessageHandler messageHandler = new MessageHandler();
		messageHandler.start();
	}
	

}