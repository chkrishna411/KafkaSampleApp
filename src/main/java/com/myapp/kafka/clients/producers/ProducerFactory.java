package com.myapp.kafka.clients.producers;

import com.myapp.kafka.clients.types.ProducerType;

public class ProducerFactory {

	public static Producer resolveProducer(Integer selection) {
		
		ProducerType producerType = ProducerType.resolve(selection);
		
		if(producerType == null || producerType == ProducerType.FIREANDFORGET) {
			return new SampleProducer();
		} else if(producerType == ProducerType.SYNC) {
			return new SampleSyncProducer();
		}else if( producerType == ProducerType.ASYNC) {
			return new SampleAsyncProducer();
		}
		return null;
	}
	
}
