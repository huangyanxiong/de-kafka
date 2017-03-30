package com.dataeye.kafka.consumer.simple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ConsumerQueue {

	private final static ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<String>(100000);

	private static Logger logger = LogManager.getLogger(ConsumerQueue.class.getName());

	private static boolean flush2Kafka = true;
	private static int NextSleepTime = 10;

	// Start send msg to file
	public static void start() {
		new Thread(flushData2File).start();
	}

	public static void offer(String notice) {
		if (!queue.offer(notice)) {
			logger.error("notice queue full!!!");
		}

	}

	private final static Runnable flushData2File = new Runnable() {

		public void run() {
			while (flush2Kafka) {
				try {
					if (queue.size() > 0) {

						List<String> lineList = new ArrayList<String>();

						queue.drainTo(lineList);

						System.out.println("queue size=" + lineList.size() + ", NextSleepTime=" + NextSleepTime + "s");

						for (String line : lineList) {
							System.out.println(line);
						}
					}
				} catch (Throwable t) {
					System.out.println("flush msg 2 file error");
					logger.error("flush msg 2 file error", t);
				}

				try {
					Thread.sleep(NextSleepTime * 1000);
				} catch (InterruptedException e) {
					System.out.println("flush thread sleep error");
					logger.error("flush thread sleep error:" + e.getMessage());
				}
			}
		}
	};

}
