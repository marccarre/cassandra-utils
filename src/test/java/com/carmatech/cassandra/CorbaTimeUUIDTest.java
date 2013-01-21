/*******************************************************************************
 * Copyright (c) 2013 Marc CARRE
 * Licensed under the MIT License, available at http://opensource.org/licenses/MIT
 * 
 * Contributors:
 *     Marc CARRE - initial API and implementation
 ******************************************************************************/
package com.carmatech.cassandra;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.util.Date;
import java.util.concurrent.Callable;

import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.eaio.uuid.UUID;

public class CorbaTimeUUIDTest {
	private static final int TOLERANCE_IN_MS = 100;

	@Before
	public void before() throws InterruptedException {
		Thread.sleep(5); // Sleep 5 ms to avoid "collisions" in the UUID generator.
	}

	@Test
	public void createUUIDAsOfNow() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID uuid = CorbaTimeUUID.createUUID();
		long actualTimestamp = CorbaTimeUUID.toMillis(uuid);

		assertThat((double) expectedTimestamp, is(closeTo(actualTimestamp, TOLERANCE_IN_MS)));
	}

	@Test
	public void createUUIDFromTimestampAndConvertUUIDBackToTimestamp() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID uuid = CorbaTimeUUID.createUUID(expectedTimestamp);
		long actualTimestamp = CorbaTimeUUID.toMillis(uuid);

		assertEquals(expectedTimestamp, actualTimestamp);
	}

	@Test
	public void createUUIDFromDateAndConvertUUIDBackToDate() {
		Date date = new Date();

		UUID uuid = CorbaTimeUUID.createUUID(date);
		long actualTimestamp = CorbaTimeUUID.toMillis(uuid);

		assertEquals(date, new Date(actualTimestamp));
	}

	@Test
	public void createUUIDFromDateTimeAndConvertUUIDBackToDateTime() {
		DateTime dateTime = new DateTime();

		UUID uuid = CorbaTimeUUID.createUUID(dateTime);
		long actualTimestamp = CorbaTimeUUID.toMillis(uuid);

		assertEquals(dateTime, new DateTime(actualTimestamp));
	}

	@Test
	public void getTwoIdenticalUUID() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID first = CorbaTimeUUID.toUUID(expectedTimestamp);
		UUID second = CorbaTimeUUID.toUUID(expectedTimestamp);

		assertEquals(first, second);
		assertEquals(first.toString(), second.toString());
		assertEquals(first.getTime(), second.getTime()); // Same internal time in 100s of ns since epoch.
		assertEquals(CorbaTimeUUID.toMillis(first), CorbaTimeUUID.toMillis(second));
	}

	@Test
	public void generateTwoDifferentUUIDsWithSameTimeComponent() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID first = CorbaTimeUUID.createUUID(expectedTimestamp);
		UUID second = CorbaTimeUUID.createUUID(expectedTimestamp);

		assertNotSame(first, second);
		assertNotSame(first.toString(), second.toString());
		assertEquals(first.toString().substring(8, 36), second.toString().substring(8, 36)); // Only first 8 chars differ.

		assertEquals(CorbaTimeUUID.toMillis(first), CorbaTimeUUID.toMillis(second));
	}

	@Test
	public void getTimestampFromUUIDAndConvertTimestampBackToUUID() {
		UUID expectedUuid = CorbaTimeUUID.createUUID();

		long timestamp = CorbaTimeUUID.toMillis(expectedUuid);
		UUID actualUuid = CorbaTimeUUID.toUUID(timestamp);

		assertEquals(expectedUuid, actualUuid);
		assertEquals(expectedUuid.toString(), actualUuid.toString());

		// REMINDER: if we "create" a new UUID instead of just converting it back, we get a totally different UUID:
		UUID newUuid = CorbaTimeUUID.createUUID(timestamp);
		assertNotSame(expectedUuid, newUuid);
		assertNotSame(expectedUuid.toString(), newUuid.toString());
		assertEquals(expectedUuid.toString().substring(8, 36), newUuid.toString().substring(8, 36)); // Only first 8 chars differ.
		assertEquals(CorbaTimeUUID.toMillis(expectedUuid), CorbaTimeUUID.toMillis(newUuid));
	}

	@Test
	public void twiceSameTimestampIncrementsTimestampUsedToGenerateUUID() {
		long t0 = new DateTime().getMillis();

		// Approximately 10,000 UUIDs can be generated with an identical time component.
		// Therefore, creating 9,000 UUIDs is both safe and good enough for this test.
		for (int i = 0; i < 4500; i++) {
			UUID first = CorbaTimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
			UUID second = CorbaTimeUUID.createUUID(t0);// Increment of 100 nanoseconds internally

			assertThat(first, is(not(second)));
			assertThat(CorbaTimeUUID.toMillis(first), is(t0));
			assertThat(CorbaTimeUUID.toMillis(second), is(t0));
		}

		UUID third = CorbaTimeUUID.createUUID(t0 + 1); // Increment of 1 millisecond internally
		UUID fourth = CorbaTimeUUID.createUUID(t0); // Create UUID back in time, latest time will be used instead

		assertThat(CorbaTimeUUID.toMillis(third), is(t0 + 1));
		assertThat(CorbaTimeUUID.toMillis(fourth), is(t0 + 1));
	}

	@Test
	public void uuidsAreNaturallySortedEvenForSameTimestampInMilliseconds() {
		long t0 = new DateTime().getMillis();

		UUID first = CorbaTimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t1 = CorbaTimeUUID.toMillis(first);

		UUID second = CorbaTimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t2 = CorbaTimeUUID.toMillis(second);

		UUID third = CorbaTimeUUID.createUUID(t0 + 1); // Increment of 1 millisecond internally
		long t3 = CorbaTimeUUID.toMillis(third);

		assertThat(first, is(not(second)));
		assertThat(first, is(not(third)));
		assertThat(second, is(not(third)));

		assertThat(first, is(lessThan(second)));
		assertThat(first, is(lessThan(third)));
		assertThat(second, is(lessThan(third)));

		assertThat(t1, is(t0));
		assertThat(t2, is(t0));

		assertThat(t3, is(not(t0)));
		assertThat(t3, is(t0 + 1));

		assertThat(t3, is(greaterThan(t1)));
		assertThat(t3, is(greaterThan(t2)));
	}

	@Test
	public void resetAllowToGenerateUUIDsWithTimestampsEarlierThanLatestUsed() {
		long t0 = new DateTime().getMillis();

		UUID first = CorbaTimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t1 = CorbaTimeUUID.toMillis(first);

		UUID second = CorbaTimeUUID.createUUID(t0 + 1); // Increment of 1 millisecond internally
		long t2 = CorbaTimeUUID.toMillis(second);

		assertThat(second, is(not(first)));
		assertThat(t1, is(t0));
		assertThat(t2, is(not(t0)));
		assertThat(t2, is(greaterThan(t0)));

		// WARNING: Use only for testing purposes, as it may lead to duplicate UUIDs.
		CorbaTimeUUID.reset();

		UUID third = CorbaTimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t3 = CorbaTimeUUID.toMillis(third);

		assertThat(third, is(first)); // Duplicate UUIDs, as using the same sequence number after reset
		assertThat(t3, is(t0));
		assertThat(t3, is(t1));
		assertThat(t3, is(not(t2)));
		assertThat(t3, is(lessThan(t2)));
	}

	@Test
	public void comparePerformanceToHector() throws Exception {
		// Warm-up: to initialize all static members in all classes:
		java.util.UUID javaUuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUIDSerializer.get().toByteBuffer(javaUuid);
		TimeUUIDSerializer.get().toByteBuffer(new UUID(javaUuid.getMostSignificantBits(), javaUuid.getLeastSignificantBits()));
		TimeUUIDSerializer.get().toByteBuffer(CorbaTimeUUID.createUUID());

		// Typical results for 10,000 iterations using each method:
		// - Hector (first way): 14 ms.
		// - Hector (second way): 16 ms.
		// - Candidate (best according to this test): 5 ms.

		final int numOfRuns = 10000;

		long elapsedTimeForHector1 = new TimedOperation(new RepeatedOperation(numOfRuns, new Runnable() {
			@Override
			public void run() {
				java.util.UUID javaUuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
				UUIDSerializer.get().toByteBuffer(javaUuid);
			}
		})).call();
		System.out.println("Hector (first way): " + elapsedTimeForHector1 / 1000000 + " ms.");

		long elapsedTimeForHector2 = new TimedOperation(new RepeatedOperation(numOfRuns, new Runnable() {
			@Override
			public void run() {
				java.util.UUID javaUuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
				UUID uuid = new UUID(javaUuid.getMostSignificantBits(), javaUuid.getLeastSignificantBits());
				TimeUUIDSerializer.get().toByteBuffer(uuid);
			}
		})).call();
		System.out.println("Hector (second way): " + elapsedTimeForHector2 / 1000000 + " ms.");

		long elapsedTimeForThisClass = new TimedOperation(new RepeatedOperation(numOfRuns, new Runnable() {
			@Override
			public void run() {
				UUID uuid = CorbaTimeUUID.createUUID();
				TimeUUIDSerializer.get().toByteBuffer(uuid);
			}
		})).call();
		System.out.println("Candidate (best according to this test): " + elapsedTimeForThisClass / 1000000 + " ms.");
	}

	class RepeatedOperation implements Runnable {
		private final int numOfRuns;
		private final Runnable runnable;

		public RepeatedOperation(final int numOfRuns, final Runnable runnable) {
			if (numOfRuns <= 0)
				throw new IllegalArgumentException("Number of times operation will be repeated must be STRICTLY POSITIVE but was " + numOfRuns);
			if (runnable == null)
				throw new IllegalArgumentException("Operation to run must NOT be null.");

			this.numOfRuns = numOfRuns;
			this.runnable = runnable;
		}

		@Override
		public void run() {
			for (int i = 0; i < numOfRuns; i++) {
				runnable.run();
			}
		}
	}

	class TimedOperation implements Callable<Long> {
		private final Runnable runnable;

		public TimedOperation(final Runnable runnable) {
			if (runnable == null)
				throw new IllegalArgumentException("Operation to run must NOT be null.");

			this.runnable = runnable;
		}

		@Override
		public Long call() throws Exception {
			final long begin = System.nanoTime();
			runnable.run();
			final long end = System.nanoTime();
			return end - begin;
		}
	}
}
