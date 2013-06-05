package com.carmatech.cassandra;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Formula: shardSizeInSeconds / updateFrequency * avgDataSizeInBytes == rowSizeInBytes <br />
 * For example: <br />
 * -1) Daily — 86400 / (1 / 10) * 200 = 172800000 (172.8 MB) --> probably too large to be efficient and safe in Cassandra. <br />
 * -2) Hourly — 3600 / (1 / 10) * 200 = 7200000 (7.2 MB) --> ideal size.
 */
public class ShardingFrequencyTest {
	@Test
	public void calculateShardingFrequencyFromQueryVolumeDetailsAndSpecifiedRowMaxSize() {
		long averageSizeInBytes = 200L;
		long writesPerTimeUnit = 10L;
		TimeUnit timeUnit = TimeUnit.SECONDS;
		long rowMaxSizeInBytes = 180000000L; // 180 MB

		ShardingFrequency frequency = ShardingFrequency.calculateFrequency(averageSizeInBytes, writesPerTimeUnit, timeUnit, rowMaxSizeInBytes);
		assertThat(frequency, is(ShardingFrequency.DAILY));
	}

	@Test
	public void calculateShardingFrequencyFromQueryVolumeDetailsAndDefaultSize() {
		long averageSizeInBytes = 200L;
		long writesPerTimeUnit = 10L;
		TimeUnit timeUnit = TimeUnit.SECONDS;

		ShardingFrequency frequency = ShardingFrequency.calculateFrequency(averageSizeInBytes, writesPerTimeUnit, timeUnit);
		assertThat(frequency, is(ShardingFrequency.HOURLY));
	}

	@Test
	public void calculateBucketShouldTruncateSpecifiedTimestampAccordingToSpecifiedFrequency() {
		long now = 1370456684678L; // Wed Jun 5 19:24:44 BST 2013
		assertThat(ShardingFrequency.calculateBucket(now, ShardingFrequency.SECONDLY), is(1370456684000L)); // Wed Jun 5 19:24:44 BST 2013
		assertThat(ShardingFrequency.calculateBucket(now, ShardingFrequency.MINUTELY), is(1370456640000L)); // Wed Jun 5 19:24:00 BST 2013
		assertThat(ShardingFrequency.calculateBucket(now, ShardingFrequency.HOURLY), is(1370455200000L)); // Wed Jun 5 19:00:00 BST 2013
		assertThat(ShardingFrequency.calculateBucket(now, ShardingFrequency.DAILY), is(1370386800000L)); // Wed Jun 5 00:00:00 BST 2013
		assertThat(ShardingFrequency.calculateBucket(now, ShardingFrequency.WEEKLY), is(1370214000000L)); // Mon Jun 3 00:00:00 BST 2013
		assertThat(ShardingFrequency.calculateBucket(now, ShardingFrequency.MONTHLY), is(1370041200000L)); // Sat Jun 1 00:00:00 BST 2013
	}

	@Test
	public void getBucketsShouldGenerateAllBucketsBetweenFromAndToIncluded() {
		long from = 1370456684678L; // Wed Jun 5 19:24:44 BST 2013
		long to = 1370543084000L; // Thu Jun 6 19:24:44 BST 2013

		Set<Long> buckets = ShardingFrequency.getBuckets(from, to, ShardingFrequency.HOURLY);
		assertThat(buckets, is(not(nullValue())));
		assertThat(buckets.size(), is(25));

		// Every hour, from "Wed Jun 5 10:00:00 BST 2013" to "Thu Jun 6 10:00:00 BST 2013" included:
		Set<Long> expectedSet = new LinkedHashSet<Long>(Arrays.asList(1370455200000L, 1370458800000L, 1370462400000L, 1370466000000L, 1370469600000L,
				1370473200000L, 1370476800000L, 1370480400000L, 1370484000000L, 1370487600000L, 1370491200000L, 1370494800000L, 1370498400000L, 1370502000000L,
				1370505600000L, 1370509200000L, 1370512800000L, 1370516400000L, 1370520000000L, 1370523600000L, 1370527200000L, 1370530800000L, 1370534400000L,
				1370538000000L, 1370541600000L));
		assertThat(buckets, equalTo(expectedSet));
	}
}
