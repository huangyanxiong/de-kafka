package com.dataeye.kafka.util;

import java.io.IOException;
import java.util.Properties;

public class ProfileManager {

	private static Properties properties;

	static {
		init();
	}

	public static void init() {
		properties = new Properties();
		try {
			properties.load(ClassLoader.getSystemResourceAsStream("config.properties"));
		} catch (IOException e) {
			System.err.println("read properties in classpath error");
			e.printStackTrace();
		}
	}

	public static Properties get() {
		return properties;
	}

	public static int getValueInt(String key, int defaultValue) {
		try {
			return Integer.parseInt(get().getProperty(key));
		} catch (Exception e) {
			return defaultValue;
		}
	}

	public static String getValue(String key, String defaultValue) {
		String value = get().getProperty(key);
		if (value == null || "".equals(value)) {
			value = defaultValue;
		}
		return value;
	}

}
