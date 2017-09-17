package com.myapp.kafka.clients.main;


import java.util.Enumeration;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationMain {

	private static final Logger logger = LoggerFactory.getLogger(ApplicationMain.class);
	
	public static void main(String[] args)  {

		BasicConfigurator.configure();
		logger.info("App Starting....");
		setLogLevel();
		MessageHandler messageHandler = new MessageHandler();
		messageHandler.start();
	}
	
	@SuppressWarnings({ "rawtypes" })
	private static void setLogLevel() {
		Enumeration list = org.apache.log4j.Logger.getRootLogger().getAllAppenders();
		if(list.hasMoreElements()) {
			ConsoleAppender console = (ConsoleAppender)list.nextElement();
			console.setThreshold(Level.INFO);
		}
	}
	
}