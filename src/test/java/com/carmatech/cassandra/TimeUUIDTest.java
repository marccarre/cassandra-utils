/*******************************************************************************
 * Copyright 2013 Marc CARRE
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.carmatech.cassandra;

import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class TimeUUIDTest {
	private static final int TOLERANCE_IN_MS = 200;

	@Before
	public void before() throws InterruptedException {
		Thread.sleep(5); // Sleep 5 ms to avoid "collisions" in the UUID generator.
	}

	@Test
	public void createUUIDAsOfNow() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID uuid = TimeUUID.createUUID();
		long actualTimestamp = TimeUUID.toMillis(uuid);

		assertThat((double) expectedTimestamp, is(closeTo(actualTimestamp, TOLERANCE_IN_MS)));
	}

	@Test
	public void createUUIDFromTimestampAndConvertUUIDBackToTimestamp() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID uuid = TimeUUID.createUUID(expectedTimestamp);
		long actualTimestamp = TimeUUID.toMillis(uuid);

		assertEquals(expectedTimestamp, actualTimestamp);
	}

	@Test
	public void createUUIDFromDateAndConvertUUIDBackToDate() {
		Date date = new Date();

		UUID uuid = TimeUUID.createUUID(date);
		long actualTimestamp = TimeUUID.toMillis(uuid);

		assertEquals(date, new Date(actualTimestamp));
	}

	@Test
	public void createUUIDFromDateTimeAndConvertUUIDBackToDateTime() {
		DateTime dateTime = new DateTime();

		UUID uuid = TimeUUID.createUUID(dateTime);
		long actualTimestamp = TimeUUID.toMillis(uuid);

		assertEquals(dateTime, new DateTime(actualTimestamp));
	}

	@Test
	public void getTwoIdenticalUUID() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID first = TimeUUID.toUUID(expectedTimestamp);
		UUID second = TimeUUID.toUUID(expectedTimestamp);

		assertEquals(first, second);
		assertEquals(first.toString(), second.toString());
		// Same internal time in 100s of ns since epoch.
		assertEquals(first.timestamp(), second.timestamp());
		assertEquals(TimeUUID.toMillis(first), TimeUUID.toMillis(second));
	}

	@Test
	public void generateTwoDifferentUUIDsWithSameTimeComponent() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID first = TimeUUID.createUUID(expectedTimestamp);
		UUID second = TimeUUID.createUUID(expectedTimestamp);

		assertNotSame(first, second);
		assertNotSame(first.toString(), second.toString());
		// Only first 8 chars differ.
		assertEquals(first.toString().substring(8, 36), second.toString().substring(8, 36));
		assertEquals(TimeUUID.toMillis(first), TimeUUID.toMillis(second));
	}

	@Test
	public void getTimestampFromUUIDAndConvertTimestampBackToUUID() {
		UUID expectedUuid = TimeUUID.createUUID();

		long timestamp = TimeUUID.toMillis(expectedUuid);
		UUID actualUuid = TimeUUID.toUUID(timestamp);

		assertEquals(expectedUuid, actualUuid);
		assertEquals(expectedUuid.toString(), actualUuid.toString());

		// REMINDER: if we "create" a new UUID instead of just converting it back, we get a totally
		// different UUID:
		UUID newUuid = TimeUUID.createUUID(timestamp);
		assertNotSame(expectedUuid, newUuid);
		assertNotSame(expectedUuid.toString(), newUuid.toString());
		// Only first 8 chars differ.
		assertEquals(expectedUuid.toString().substring(8, 36), newUuid.toString().substring(8, 36));
		assertEquals(TimeUUID.toMillis(expectedUuid), TimeUUID.toMillis(newUuid));
	}

	@Test
	public void twiceSameTimestampChangesClockSequenceUsedToGenerateUUID() {
		long t0 = new DateTime().getMillis();

		// 10,000 UUIDs can be generated with an identical time component and clock sequence
		UUID first = null;
		UUID second = null;

		for (int i = 0; i < 5000; i++) {
			first = TimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
			second = TimeUUID.createUUID(t0);// Increment of 100 nanoseconds internally

			assertThat(first, is(not(second)));
			assertThat(TimeUUID.toMillis(first), is(t0));
			assertThat(TimeUUID.toMillis(second), is(t0));
			assertThat(first.clockSequence(), is(second.clockSequence()));
		}

		UUID overflow = TimeUUID.createUUID(t0);
		assertThat(TimeUUID.toMillis(overflow), is(t0));
		assertThat(overflow.clockSequence(), is(not(first.clockSequence())));

		UUID third = TimeUUID.createUUID(t0 + 1); // Increment of 1 millisecond internally
		UUID fourth = TimeUUID.createUUID(t0); // Create UUID back in time, this will use a different clock seq

		assertThat(TimeUUID.toMillis(third), is(t0 + 1));
		assertThat(TimeUUID.toMillis(fourth), is(t0));
		assertThat(third.clockSequence(), is(not(fourth.clockSequence())));
	}

	@Test
	public void uuidsAreNaturallySortedEvenForSameTimestampInMilliseconds() {
		long t0 = new DateTime().getMillis();

		UUID first = TimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t1 = TimeUUID.toMillis(first);

		UUID second = TimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t2 = TimeUUID.toMillis(second);

		UUID third = TimeUUID.createUUID(t0 + 1); // Increment of 1 millisecond internally
		long t3 = TimeUUID.toMillis(third);

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
	public void resettingTimeUuidGeneratorAllowsToGoBackInTime() {
		long t0 = new DateTime().getMillis();

		UUID first = TimeUUID.createUUID(t0); // Increment of 100 nanoseconds internally
		long t1 = TimeUUID.toMillis(first);

		UUID second = TimeUUID.createUUID(t0 + 1); // Increment of 1 millisecond internally
		long t2 = TimeUUID.toMillis(second);

		assertThat(second, is(not(first)));
		assertThat(t1, is(t0));
		assertThat(t2, is(t0 + 1));

		// WARNING: should only be used for testing purposes:
		TimeUUID.resetGenerator();

		UUID third = TimeUUID.createUUID(t0);
		long t3 = TimeUUID.toMillis(third);

		assertThat(third, is(first));
		assertThat(t3, is(t0));
	}

	@Test
	public void comparePerformanceToHector() throws Exception {
		// Warm-up: to initialize all static members in all classes:
		java.util.UUID javaUuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUIDSerializer.get().toByteBuffer(javaUuid);
		UUIDSerializer.get().toByteBuffer(new UUID(javaUuid.getMostSignificantBits(), javaUuid.getLeastSignificantBits()));
		UUIDSerializer.get().toByteBuffer(TimeUUID.createUUID());

		// Typical results for 10,000 iterations using each method:
		// - Hector (first way): 14 ms.
		// - Hector (second way): 16 ms.
		// - Candidate (best according to this test): 5 ms.

		final int numOfRuns = 50000; // N.B. 10k = default JIT compilation threshold.

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
				UUIDSerializer.get().toByteBuffer(uuid);
			}
		})).call();
		System.out.println("Hector (second way): " + elapsedTimeForHector2 / 1000000 + " ms.");

		long elapsedTimeForThisClass = new TimedOperation(new RepeatedOperation(numOfRuns, new Runnable() {
			@Override
			public void run() {
				UUID uuid = TimeUUID.createUUID();
				UUIDSerializer.get().toByteBuffer(uuid);
			}
		})).call();
		System.out.println("Candidate run 1 (best according to this test): " + elapsedTimeForThisClass / 1000000 + " ms.");

		elapsedTimeForThisClass = new TimedOperation(new RepeatedOperation(numOfRuns, new Runnable() {
			@Override
			public void run() {
				UUID uuid = TimeUUID.createUUID();
				UUIDSerializer.get().toByteBuffer(uuid);
			}
		})).call();
		System.out.println("Candidate run 2 (best according to this test): " + elapsedTimeForThisClass / 1000000 + " ms.");
		Thread.sleep(200);
	}

	@Test
	public void testConcurrency() throws Exception {

		final int threads = 50;
		final int iterations = 25000;
		final Set<String> uuids = Collections.newSetFromMap(Maps.<String, Boolean>newConcurrentMap());

		// Setup a bunch of threads to updates tags and aliases
		final Semaphore latch = new Semaphore(0, true);
		for (int ii = 0; ii < threads; ii++) {
			final int id = ii;
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						latch.acquire();
						for (int jj = 0; jj < iterations; jj++) {
							// Create a new UUID
							boolean newEntry = uuids.add(TimeUUID.createUUID().toString());
							assertThat(newEntry, is(true));
						}


					} catch (InterruptedException e) {
					} finally {
						latch.release();
					}
				}
			});
			t.start();
		}

		// Let the threads run.
		latch.release(threads);
		Thread.sleep(100);

		// Wait for completion
		latch.acquire(threads);

		assertThat(uuids.size(), is(threads * iterations));
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