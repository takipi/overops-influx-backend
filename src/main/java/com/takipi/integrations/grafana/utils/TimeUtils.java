package com.takipi.integrations.grafana.utils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.ocpsoft.prettytime.PrettyTime;

import com.takipi.common.api.util.Pair;

public class TimeUtils {

	private static final String LAST_TIME_WINDOW = "time >= now() - ";
	private static final String SO_FAR_WINDOW = "time >= ";
	private static final String RANGE_WINDOW = "and time <= ";
	private static final String MILLI_UNIT = "ms";

	private static final DateTimeFormatter fmt = ISODateTimeFormat.dateTime().withZoneUTC();
	private static final PrettyTime prettyTime = new PrettyTime();


	private static final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";

	public static String encodeBase64(long v) {
		char[] a = alphabet.toCharArray();
		v = Math.abs(v);
		String s = "";
		for (int i = 0; i < 11; i++) {
			long val = v & 63;
			s = a[(int) val] + s;
			v >>= 6;
		}
		while (s.startsWith("A") && s.length() > 1)
			s = s.substring(1, s.length());
		return s;
	}

	public static long decodeBase64(String s) {
		char[] a = alphabet.toCharArray();
		Map<Character, Integer> map = new HashMap<>();
		for (int i = 0; i < a.length; i++)
			map.put(a[i], i);
		char[] chars = s.toCharArray();
		long v = 0;
		for (char c : chars) {
			v <<= 6;
			v = v | map.get(c);
		}
		return v;
	}

	public static String getDateTimeFromEpoch(long epoch) {
		return new DateTime(epoch).toString(fmt);
	}

	public static long getDateTimeDelta(Pair<String, String> timespan) {
		long to = fmt.parseDateTime(timespan.getSecond()).getMillis();
		long from = fmt.parseDateTime(timespan.getFirst()).getMillis();

		return to - from;
	}

	public static String getDateTimeFromEpoch(String epoch) {
		return new DateTime(Long.valueOf(epoch)).toString(fmt);
	}

	private static DateTime getTimeGreaterThan(String timeFilter) {

		int unitIndex = timeFilter.indexOf(MILLI_UNIT);
		String value = timeFilter.substring(SO_FAR_WINDOW.length(), unitIndex);
		DateTime result = new DateTime(Long.valueOf(value));

		return result;
	}

	private static DateTime getTimeLessThan(String timeFilter) {
		int rangeIndex = timeFilter.indexOf(RANGE_WINDOW);
		String timeWindow = timeFilter.substring(rangeIndex + RANGE_WINDOW.length(),
				timeFilter.length() - MILLI_UNIT.length());

		DateTime result = new DateTime(Long.valueOf(timeWindow));

		return result;
	}
	
	public static int parseInterval(String timeWindowWithUnit) {
		String timwWindow = timeWindowWithUnit.substring(0, timeWindowWithUnit.length() - 1);

		char timeUnit = timeWindowWithUnit.charAt(timeWindowWithUnit.length() - 1);

		int delta = Integer.valueOf(timwWindow);
		if (timeUnit == 'd') {
			return delta * 24 * 60;
		} else if (timeUnit == 'h') {
			return delta * 60;
		} else if (timeUnit == 'm') {
			return delta;
		} else {
			throw new IllegalStateException("Uknown time unit for " + timeWindowWithUnit);
		}
	}

	private static int getTimeDelta(String timeFilter) {
		String timeWindowWithUnit = timeFilter.substring(LAST_TIME_WINDOW.length(), timeFilter.length());
		return parseInterval(timeWindowWithUnit);
	}

	public static long getLongTime(String value) {
		return fmt.parseDateTime(value).getMillis();
	}
	
	public static DateTime getDateTime(String value) {
		return fmt.parseDateTime(value);
	}

	public static Pair<DateTime, DateTime> getTimeFilter(String timeFilter) {
		if ((timeFilter == null) || (timeFilter.isEmpty())) {
			throw new IllegalArgumentException("time cannot be empty");
		}

		DateTime from;
		DateTime to;

		if (timeFilter.startsWith(LAST_TIME_WINDOW)) {
			to = DateTime.now();
			from = to.minusMinutes(getTimeDelta(timeFilter));
			return Pair.of(from, to);
		}

		if (timeFilter.contains(RANGE_WINDOW)) {
			from = getTimeGreaterThan(timeFilter);
			to = getTimeLessThan(timeFilter);

			return Pair.of(from, to);
		}

		if (timeFilter.startsWith(SO_FAR_WINDOW)) {
			to = DateTime.now();
			from = getTimeGreaterThan(timeFilter);
			return Pair.of(from, to);
		}

		throw new IllegalArgumentException("Could not parse time filter " + timeFilter);
	}
	
	public static Pair<String, String> toTimespan(Pair<DateTime, DateTime> pair) {
		return Pair.of(pair.getFirst().toString(fmt), pair.getSecond().toString(fmt));
	}
	
	public static Pair<String, String> parseTimeFilter(String timeFilter) {
		Pair<DateTime, DateTime> pair = getTimeFilter(timeFilter);
		return toTimespan(pair);
	}
	
	public static String prettifyTime(String value) {
		DateTime dateTime = fmt.parseDateTime(value);
		String result = prettyTime.format(new Date(dateTime.getMillis()));
		return result;
	}
}
