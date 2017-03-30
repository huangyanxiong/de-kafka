package com.dataeye.kafka.producer;

import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.dataeye.kafka.util.ProfileManager;

public class KafkaProducerAgent {

	private static Producer<String, String> producer = null;
	public static String topic;
	private static Logger logger = LogManager.getLogger(KafkaProducerAgent.class.getName());

	static {
		init();
	}

	private KafkaProducerAgent() {
	}

	private static void init() {
		Properties properties = ProfileManager.get();
		ProducerConfig config = new ProducerConfig(properties);
		producer = new Producer<String, String>(config);

		topic = properties.getProperty("kafka.event.topic", "test");

		if (topic.equals("")) {
			logger.error("miss kafka.event.callback.topic, exit!!!");
			System.exit(-1);
		}

		logger.warn("KAFKA_TOPIC_CONFIG:\n kafka.event.topic: " + topic);

	}

	public static void send(List<KeyedMessage<String, String>> list) {
		producer.send(list);
	}
}
