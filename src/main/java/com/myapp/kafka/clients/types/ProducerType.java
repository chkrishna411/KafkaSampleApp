package com.myapp.kafka.clients.types;

public enum ProducerType {

	FIREANDFORGET(1), SYNC(2), ASYNC(3);
	
	private Integer value;

	private ProducerType(Integer value) {
		this.value = value;
	}

	public Integer getValue() {
		return value;
	}
	
	public static ProducerType resolve(Integer value) {
		
		if(value == null) {
			return ProducerType.FIREANDFORGET;
		}
		
		for(ProducerType producerType : ProducerType.values()) {
			 if(value == producerType.getValue()) {
				 return producerType;
			 }
		}
		return ProducerType.FIREANDFORGET;
	}
	
}
