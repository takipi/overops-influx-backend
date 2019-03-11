package com.takipi.integrations.grafana.util;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.ocpsoft.prettytime.PrettyTime;

import com.takipi.common.util.Pair;

public class TimeUtil {
	private static final String LAST_TIME_WINDOW = "time >= now() - ";
	private static final String SO_FAR_WINDOW = "time >= ";
	private static final String RANGE_WINDOW = "and time <= ";
	private static final String MILLI_UNIT = "ms";
	
	public static final String MINUTE_POSTFIX = "m";
	public static final String HOUR_POSTFIX = "h";
	public static final String DAY_POSTFIX = "d";
	
	public static final String DEFAULT_TIME_RANGE_STR = "1d";
	
	private static final DateTimeFormatter fmt = ISODateTimeFormat.dateTime().withZoneUTC();
	private static final PrettyTime prettyTime = new PrettyTime();

	public static String getDateTimeFromEpoch(long epoch) {
		return new DateTime(epoch).toString(fmt);
	}

	public static long getDateTimeDelta(DateTime from, DateTime to) {
		return to.getMillis() - from.getMillis();
	}
	
	public static long getDateTimeDelta(Pair<String, String> timespan) {
		DateTime from = fmt.parseDateTime(timespan.getFirst());
		DateTime to = fmt.parseDateTime(timespan.getSecond());

		return getDateTimeDelta(from, to);
	}

	public static String getDateTimeFromEpoch(String epoch) {
		return new DateTime(Long.valueOf(epoch)).toString(fmt);
	}
	
	public static String getTimeInterval(long timeDelta) {
		
		String result; 
		
		if (timeDelta > TimeUnit.DAYS.toMillis(1)) {
			result = TimeUnit.MILLISECONDS.toDays(timeDelta) + DAY_POSTFIX;
		} else if (timeDelta > TimeUnit.HOURS.toMillis(1)) {
			result = TimeUnit.MILLISECONDS.toHours(timeDelta) + HOUR_POSTFIX;
		} else {
			result =  TimeUnit.MILLISECONDS.toMinutes(timeDelta) + MINUTE_POSTFIX;
		}
		
		return result;
	}
	
	public static String getTimeRange(int min) {
       
		//if (min % (24 * 60) == 0) {
        //		double days = (double)(min) / 24 / 60;
		//	return String.valueOf(Math.round(days)) + DAY_POSTFIX;
        //}
		
		if (min % 60 == 0) {
    			double hours = (double)(min) / 60;
			return String.valueOf(Math.round(hours)) + HOUR_POSTFIX;
		}
		
		return min + MINUTE_POSTFIX;
 
	}
	
	public static String getLastWindowMinTimeFilter(int minDelta) {
		return LAST_TIME_WINDOW + getTimeRange(minDelta);
	}
	
	public static String getLastWindowTimeFilter(long timeDelta) {
		return LAST_TIME_WINDOW + getTimeInterval(timeDelta);
	}

	public static int parseInterval(String timeWindowWithUnit) {
		
		String timeWindow = timeWindowWithUnit.substring(0, timeWindowWithUnit.length() - 1);
		char timeUnit = timeWindowWithUnit.charAt(timeWindowWithUnit.length() - 1);

		int delta = Integer.valueOf(timeWindow);
		if (timeUnit == DAY_POSTFIX.charAt(0)) {
			return delta * 24 * 60;
		} else if (timeUnit == HOUR_POSTFIX.charAt(0)) {
			return delta * 60;
		} else if (timeUnit == MINUTE_POSTFIX.charAt(0)) {
			return delta;
		} else {
			throw new IllegalStateException("Unknown time unit for " + timeWindowWithUnit);
		}
	}

	public static String getMillisAsString(DateTime date) {
		return String.valueOf(date.getMillis());
	}
	
	public static long getLongTime(String value) {
		return fmt.parseDateTime(value).getMillis();
	}
	
	public static DateTime getDateTime(String value) {
		return fmt.parseDateTime(value);
	}

	public static int getStartDateTimeIndex(List<Pair<DateTime, DateTime>> intervals, String value) {
		
		DateTime dateTime = TimeUtil.getDateTime(value);

		for (int i = 0; i < intervals.size(); i++) {
			
			Pair<DateTime, DateTime> interval = intervals.get(i);
			
			if ((i == intervals.size() -1) && (dateTime.isAfter(interval.getSecond()))) {
				return i;
			}
			
			if ((dateTime.isAfter(interval.getFirst())) && (dateTime.isBefore(interval.getSecond()))) {
				return i;
			}
		}

		return -1;
	}

	public static String getTimeFilter(Pair<DateTime, DateTime> timespan) {
		
		DateTime now = DateTime.now();

		long toDelta =  now.getMillis() - timespan.getSecond().getMillis();
		
		String result;
		long toMinDelta = TimeUnit.MILLISECONDS.toMinutes(toDelta);
		
		if (toMinDelta <= 2) {
			long fromDelta = timespan.getSecond().getMillis() - timespan.getFirst().getMillis();
			long minDelta = TimeUnit.MILLISECONDS.toMinutes(fromDelta);
				
			String timeRange = TimeUtil.getTimeRange((int)minDelta);
			result = LAST_TIME_WINDOW + timeRange;	
		} else {
			result = toTimeFilter(timespan.getFirst(), timespan.getSecond());	
		}
		
		return result;
	}
	
	public static Pair<DateTime, DateTime> getTimeFilter(String timeFilter) {
		if ((timeFilter == null) || (timeFilter.isEmpty())) {
			throw new IllegalArgumentException("timeFilter cannot be empty");
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
	
	public static String toString(DateTime value) {
		return value.toString(fmt);
	}
	
	public static Pair<String, String> toTimespan(Pair<DateTime, DateTime> pair) {
		return Pair.of(pair.getFirst().toString(fmt), pair.getSecond().toString(fmt));
	}
	
	public static Pair<String, String> toTimespan(DateTime from, DateTime to) {
		return Pair.of(from.toString(fmt), to.toString(fmt));
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
	
	public static int toMinutes(long milli) {
		return (int) (milli / 1000 / 60);
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
	
	public static String getTimeRange(String timeFilter) {
		
		String result = getTimeUnit(timeFilter);
		
		if (result == null) {
			return DEFAULT_TIME_RANGE_STR;
		}
		
		return result;
	}
	
	public static String getTimeUnit(String timeFilter) {
		
		if ((timeFilter == null) || (!timeFilter.contains(LAST_TIME_WINDOW))) {
			return null;
		}
		
		String result = timeFilter.substring(LAST_TIME_WINDOW.length(), timeFilter.length());

		return result;
	}
	
	private static int getTimeDelta(String timeFilter) {
		return parseInterval(getTimeUnit(timeFilter));
	}
	
	public static String toTimeFilter(DateTime from, DateTime to) {
		return SO_FAR_WINDOW + from.getMillis() + MILLI_UNIT + " " + RANGE_WINDOW + to.getMillis() + MILLI_UNIT;
	}
	
	public static String getTimeRange(Pair<DateTime, DateTime> timespan) {
		
		String result;
		
		long toDelta =  DateTime.now().getMillis() - timespan.getSecond().getMillis();

		if (TimeUnit.MILLISECONDS.toMinutes(toDelta) < 1) {
			
			long fromDelta = timespan.getSecond().getMillis() - timespan.getFirst().getMillis();
			long minDelta = TimeUnit.MILLISECONDS.toMinutes(fromDelta);
				
			result = TimeUtil.getTimeRange((int)minDelta);	
		} else {
			result = null;
		}
		
		return result;
	}
}
