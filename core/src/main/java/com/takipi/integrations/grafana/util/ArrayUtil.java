package com.takipi.integrations.grafana.util;

public class ArrayUtil {
	public static String[] safeSplitArray(String value, String seperator, boolean removeWhitespace) {
		if ((value == null) || (value.isEmpty())) {
			return new String[0];
		}

		String[] result;
		
		if (removeWhitespace) {
			result = value.replaceAll("\\s", "").split(seperator);
		}  else {
			result = value.split(seperator);
		}
		
		return result;
	}
}
