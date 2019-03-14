package com.takipi.integrations.grafana.cache.key.util;

import org.joda.time.DateTime;
import org.joda.time.Duration;

public class KeyDateUtil
{
	public static DateTime roundDate(DateTime dateTime, int minutes)
	{
	    if ((minutes < 1) ||
	    	(60 % minutes != 0))
	    {
	        throw new IllegalArgumentException("minutes must be a factor of 60");
	    }
	    
	    DateTime hour = dateTime.hourOfDay().roundFloorCopy();
	    
	    long millisSinceHour = new Duration(hour, dateTime).getMillis();
	    int roundedMinutes = ((int)Math.round(millisSinceHour / 60000.0 / minutes)) * minutes;
	    
	    return hour.plusMinutes(roundedMinutes);
	}
}
