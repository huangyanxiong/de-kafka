package com.dataeye.kafka.producer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ProducerRunner {

	public static void main(String[] args) throws Exception {
		
		ProducerQueue.start();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

		for (int i = 0; i < 1000; i++) {
			
			String dateString = sdf.format(new Date());
			
			ProducerQueue.offer(dateString + " ==> " + String.valueOf(i));
			
			Thread.sleep(1000);
		}
	}

}
