package com.raja.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerAssignSeekDemo {

	public static final Logger logger = LoggerFactory.getLogger(ConsumerAssignSeekDemo.class.getName());
	public static void main(String[] args) {
		Properties properties = new Properties();
		//create properties object
		String bootstrapServers = "127.0.0.1:9092";
		String topic = "consumer-demo";
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest or none

		//create kafka consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(properties);
		//assign and seek are mostly used to replay data or fetch a specific message
		//assign
		TopicPartition partitionReadFrom = new TopicPartition(topic, 0);
		consumer.assign(Arrays.asList(partitionReadFrom));
		long offsetReadFrom = 3L;
		boolean keepOnReading = true;
		//seek
		consumer.seek(partitionReadFrom, offsetReadFrom);
		
		int numberOfMessageToRead = 2;
		int noMessagesReadSofar = 0;
		
		//poll data from topic
		while(keepOnReading){
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

			for(ConsumerRecord<String, String> record:consumerRecords){
				noMessagesReadSofar +=1;
				logger.info("Consumer topic:{}",record.topic());
				logger.info("Consumer key:{} - value:{}",record.key(),record.value());
				logger.info("Consumer partition:{} offsetid:{}",record.partition(),record.offset());
				if(noMessagesReadSofar >= numberOfMessageToRead){
					keepOnReading = false; // to exit while loop
					break; // to exit for loop
				}
				
			}
			logger.info("Exiting the application.");
		}
	

	}

}
