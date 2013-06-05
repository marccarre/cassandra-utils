package com.carmatech.cassandra;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.joda.time.MutableDateTime;

public enum ShardingFrequency {
	// @formatter:off
	SECONDLY(1000L), 
	MINUTELY(60 * 1000L), 
	HOURLY(60 * 60 * 1000L), 
	DAILY(24 * 60 * 60 * 1000L), 
	WEEKLY(7 * 24 * 60 * 60 * 1000L), 
	MONTHLY(30 * 24 * 60 * 60 * 1000L);
	// @formatter:on

	private final long frequencyInMillis;

	private ShardingFrequency(final long frequencyInMillis) {
		this.frequencyInMillis = frequencyInMillis;
	}

	public long toMillis() {
		return frequencyInMillis;
	}

	private final static long KB = 1024;
	private final static long MB = 1024 * KB;
	private final static long ROW_MAX_SIZE = 10 * MB;

	public static ShardingFrequency calculateFrequency(final long averageSizeInBytes, final long writesPerTimeUnit, final TimeUnit timeUnit) {
		return calculateFrequency(averageSizeInBytes, writesPerTimeUnit, timeUnit, ROW_MAX_SIZE);
	}

	public static ShardingFrequency calculateFrequency(final long averageSizeInBytes, final long writesPerTimeUnit, final TimeUnit timeUnit,
			final long rowMaxSizeInBytes) {
		final double shardingFrequencyInMillis = 1000 * rowMaxSizeInBytes / averageSizeInBytes / timeUnit.toSeconds(writesPerTimeUnit);

		if (shardingFrequencyInMillis > MONTHLY.toMillis())
			return MONTHLY;
		if (shardingFrequencyInMillis > WEEKLY.toMillis())
			return WEEKLY;
		if (shardingFrequencyInMillis > DAILY.toMillis())
			return DAILY;
		if (shardingFrequencyInMillis > HOURLY.toMillis())
			return HOURLY;
		if (shardingFrequencyInMillis > MINUTELY.toMillis())
			return MINUTELY;
		if (shardingFrequencyInMillis > SECONDLY.toMillis())
			return SECONDLY;

		throw new RuntimeException("Sharding will be too aggressive as bucket size is: " + shardingFrequencyInMillis
				+ " ms. You may want to reconsider your data storage strategy.");
	}

	public static long calculateBucket(final long timestamp, final ShardingFrequency frequency) {
		final MutableDateTime dateTime = new MutableDateTime(timestamp);

		if (frequency.compareTo(SECONDLY) >= 0)
			dateTime.setMillisOfSecond(0);
		if (frequency.compareTo(MINUTELY) >= 0)
			dateTime.setSecondOfMinute(0);
		if (frequency.compareTo(HOURLY) >= 0)
			dateTime.setMinuteOfHour(0);
		if (frequency.compareTo(DAILY) >= 0)
			dateTime.setHourOfDay(0);
		if (frequency.compareTo(WEEKLY) >= 0)
			dateTime.setDayOfWeek(1);
		if (frequency.compareTo(MONTHLY) >= 0)
			dateTime.setDayOfMonth(1);

		return dateTime.getMillis();
	}

	public static Set<Long> getBuckets(final long from, final long to, final ShardingFrequency frequency) {
		checkArgument(from <= to, "'To' timestamp must be greater than, or equal to 'from' timestamp: 'from'=[" + from + "], 'to'=[" + to + "].");

		final Set<Long> buckets = new LinkedHashSet<Long>();
		buckets.add(calculateBucket(from, frequency));

		long timestamp = from + frequency.toMillis();
		while (timestamp < to) {
			buckets.add(calculateBucket(timestamp, frequency));
			timestamp += frequency.toMillis();
		}

		buckets.add(calculateBucket(to, frequency));
		return buckets;
	}
}