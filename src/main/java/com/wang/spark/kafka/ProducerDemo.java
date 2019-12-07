package com.wang.spark.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ProducerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 1001; i <= 1100; i++)
			producer.send(new KeyedMessage<String, String>("wangg", "wangg-msg" + i));
	}
}