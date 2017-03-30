package com.dataeye.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import kafka.producer.KeyedMessage;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class ProducerQueue {

	private final static ArrayBlockingQueue<KeyedMessage<String, String>> queue = new ArrayBlockingQueue<KeyedMessage<String, String>>(1000000);

	private static Logger logger = LogManager.getLogger(ProducerQueue.class.getName());
	
	private static boolean flush2Kafka = true;
	private static int NextSleepTime = 2;

	// Start send msg to kafka
	public static void start() {
		new Thread(flushData2Kafka).start();
	}

	public static void offer(String notice) {
		if (!queue.offer(new KeyedMessage<String, String>(KafkaProducerAgent.topic, null, notice))) {
			// 队列满，非常危险 TODO:告警？
			logger.error("notice queue full!!!");
		}

	}

	private final static Runnable flushData2Kafka = new Runnable() {

		public void run() {
			while (flush2Kafka) {
				try {
					if (queue.size() > 0) {
						List<KeyedMessage<String, String>> noticeList = new ArrayList<KeyedMessage<String, String>>();
						queue.drainTo(noticeList);
						// 下一次睡眠时间
						NextSleepTime = getSleepTime(noticeList.size());
						// 记录日志
						logger.info("NoticeQueue queue size=" + noticeList.size() + ", NextSleepTime=" + NextSleepTime + "s");

						KafkaProducerAgent.send(noticeList);
					}
				} catch (Throwable t) {
					logger.error("flush msg 2 kafka error", t);
				}

				try {
					Thread.sleep(NextSleepTime * 1000);
				} catch (InterruptedException e) {
					logger.error("flush2Kafka thread sleep error:" + e.getMessage());
				}
			}
		}
	};

	public static int getSleepTime(int queueSize) {
		return 1;
	}
}
