package com.dataeye.kafka.consumer.simple;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class ConsumerActiveRunner implements Runnable {
	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;

	public ConsumerActiveRunner(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {
		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
	}

	public void run() {
		
		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
		
		while (it.hasNext()) {
			
			String json = new String(it.next().message());
		
			System.out.println("get line ==> " + json);
			
			ConsumerQueue.offer(json);
			
		}
	}
}
