package com.wang.spark.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerDemo {
	private static final String topic = "wangg";
	private static final Integer threads = 2;

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
		props.put("group.id", "vvvvv");
		//smallest重最开始消费,largest代表重消费者启动后产生的数据才消费
		//--from-beginning
		props.put("auto.offset.reset", "smallest");

		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer =Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
			new Thread(new Runnable() {
				public void run() {
					for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
						String msg = new String(mm.message());
						System.out.println(msg);
					}
				}
			}).start();
		}
	}
}