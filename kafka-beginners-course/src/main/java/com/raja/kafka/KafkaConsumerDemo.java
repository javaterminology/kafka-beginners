package com.raja.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo {

	public static final Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName());
	public static void main(String[] args) {
		Properties properties = new Properties();
		//create properties object
		String bootstrapServers = "127.0.0.1:9092";
		String topic = "first_topic";
		String groupId = "demo-app1";
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//latest or none

		//create kafka consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		//subscribe to topic
		consumer.subscribe(Arrays.asList(topic));
		
		//poll data from topic
		ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
		
		for(ConsumerRecord<String, String> record:consumerRecords){
			logger.info("Consumer topic:{}",record.topic());
			logger.info("Consumer key:{} value:{}",record.key(),record.value());
			logger.info("Consumer partition:{} offsetid:{}",record.partition(),record.offset());
		}
		consumer.close();

	}

}
