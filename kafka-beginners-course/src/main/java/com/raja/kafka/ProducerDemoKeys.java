package com.raja.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		
		String bootstrapServers = "127.0.0.1:9092";
    	Properties properties  = new Properties();
    	properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    	
    	properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
    	
    	
    	
    	for(int i=0;i<5;i++){
    		
    		String topic = "first_topic";
        	String message = "Hi Rajasekhar:"+Integer.toString(i);
    		String key = "id_"+Integer.toString(i);
    		
    		logger.info("Key:"+key);
    		
    		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);
    		//send data asynchronous
    		producer.send(record, new Callback() {

    			@Override
    			public void onCompletion(RecordMetadata metadata, Exception e) {
    				if(e==null){
    					logger.info("Metadata received Topic: "+metadata.topic()+"\n"
    							+"partition:"+metadata.partition()+"\n"
    							+"offsetid:"+metadata.offset());
    				}else{
    					logger.error("Error while producing message:",e);
    				}

    			}
    		}).get();//block the .send() to make it synchronous - don't use it in production
    	}
    	
    	//flush data - since producer.send() is asynchronous call it will not wait for response during that time program may be exit/terminated.
    	producer.flush();
    	
    	//flush and close producer
    	producer.close();
	}

}
