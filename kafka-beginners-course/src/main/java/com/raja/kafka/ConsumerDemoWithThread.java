package com.raja.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
	
	public static void main(String[] args) {
		new ConsumerDemoWithThread().executeConsumerTask();
	}
	private ConsumerDemoWithThread(){
		
	}
	private void executeConsumerTask(){
				String bootstrapServers = "127.0.0.1:9092";
				String topic = "consumer-demo";
				String groupId = "demo-app2";
				
				//latch for dealing with multiple threads
				CountDownLatch latch = new CountDownLatch(1);
				//creating consumer runnable task
				Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, topic, groupId, latch);
				//starting thread
				Thread thread = new Thread(myConsumerRunnable);
				thread.start();
				
				//add a shutdown hook
				Runtime.getRuntime().addShutdownHook(new Thread( ()->{
							logger.info("---Caught shutdown hook!");
							((ConsumerRunnable)myConsumerRunnable).shutdown();
							
							try {
								latch.await();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}finally{
								logger.info("---application has exited!");
							}
						}));
				
				//await method will wait until when the countDown latch becomes zero meaning until all tasks completed
				//when the task is completed then latch.countDown() will decrement its count - 1 to 0
				//telling main code the task is done.
				try {
					latch.await();
				} catch (InterruptedException e) {
					logger.error("---application got interrupted",e);
				}finally{
					logger.info("---application is closing!");
				}
	}
	
	public class ConsumerRunnable implements Runnable{
		private KafkaConsumer<String, String> consumer;
		private CountDownLatch latch;
		public ConsumerRunnable(String bootstrapServers,
				String topic,
				String groupId,
				CountDownLatch latch){
			
			this.latch = latch;
			//create properties object
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest or none
			
			//create kafka consumer
			consumer = new KafkaConsumer<String,String>(properties);
			
			//subscribe to topic
			consumer.subscribe(Arrays.asList(topic));
			
		}

		@Override
		public void run() {
			try{
				//poll data from topic
				while(true){
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

					for(ConsumerRecord<String, String> record:consumerRecords){
						logger.info("Consumer topic:{}",record.topic());
						logger.info("Consumer key:{} - value:{}",record.key(),record.value());
						logger.info("Consumer partition:{} offsetid:{}",record.partition(),record.offset());
					}
					
				}
			}catch(WakeupException we){
				logger.info("---Received shutdown signal!");
			}finally{
				consumer.close();
				latch.countDown();
			}
		}
		
		
		public void shutdown() {
			//this wakeup() method is a special method to interrupt consumer.poll()
			//it will throw the exception WakeUpException
			consumer.wakeup();
		}
		
	}
}



