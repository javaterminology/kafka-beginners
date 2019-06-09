package com.raja.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerDemoCallback {

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(KafkaProducerDemoCallback.class);
		
		String bootstrapServers = "127.0.0.1:9092";
    	Properties properties  = new Properties();
    	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    	
    	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    	
    	
    	for(int i=0;i<10;i++){
    		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hi Raja-"+i);
    		//send data asynchronous
    		producer.send(record, new Callback() {

    			@Override
    			public void onCompletion(RecordMetadata metadata, Exception e) {
    				if(e==null){
    					logger.info("New Metadata received Topic: "+metadata.topic()+"\n"
    							+"partition: "+metadata.partition()+"\n"
    							+"offsetid: "+metadata.offset()
    							+"Timestamp: "+metadata.timestamp());
    					
    				}else{
    					logger.error("Error while producing message:",e);
    				}

    			}
    		});
    	}
    	
    	//flush data
    	producer.flush();
    	
    	//flush and close producer
    	producer.close();
	}

}
