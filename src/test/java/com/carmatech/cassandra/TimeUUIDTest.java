/*******************************************************************************
 * Copyright (c) 2013 Marc CARRE <carre.marc@gmail.com>
 * Licensed under the MIT License, available at http://opensource.org/licenses/MIT
 * 
 * Contributors:
 *     Marc CARRE - initial API and implementation
 ******************************************************************************/
package com.carmatech.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.concurrent.Callable;

import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.eaio.uuid.UUID;

public class TimeUUIDTest {
	@Before
	public void before() throws InterruptedException {
		Thread.sleep(5); // Sleep 5 ms to avoid "collisions" in the UUID generator.
	}

	@Test
	public void reminderHexadecimalAndBitwiseOperators() {
		assertEquals(1, 0x01); // 1 = 0000 0001 = 0x01
		assertEquals(2, 0x02); // 2 = 0000 0010 = 0x02
		assertEquals(15, 0x0F); // 15 = 0000 1111 = 0x0F
		assertEquals(18, 0x12); // 18 = 0001 0010 = 0x12

		// Java uses two's complement. Example on int32:
		assertEquals(0, 0x00000000);
		assertEquals(1, 0x00000001);
		assertEquals(-1, 0xFFFFFFFE + 0x00000001);
		assertEquals(-1, 0xFFFFFFFF);

		// Java uses two's complement. Example on int64/long:
		assertEquals(Long.MAX_VALUE, 0x7FFFFFFFFFFFFFFFL);
		assertEquals(-Long.MAX_VALUE, 0x8000000000000000L + 0x0000000000000001L);
		assertEquals(Long.MIN_VALUE, 0x8000000000000000L);
		assertEquals(Long.MIN_VALUE, 0x7FFFFFFFFFFFFFFFL + 0x0000000000000001L);

		// Bit-wise operators:

		// 0xABCD = 1010 1011 1100 1101
		// &
		// 0x000F = 0000 0000 0000 1111
		// =
		// 0x000D = 0000 0000 0000 1101
		assertEquals(0x000D, 0xABCD & 0x000F);

		// 0xABCD = 1010 1011 1100 1101
		// |
		// 0x000F = 0000 0000 0000 1111
		// =
		// 0x000D = 1010 1011 1100 1111
		assertEquals(0xABCF, 0xABCD | 0x000F);

		// 0xABCD = 1010 1011 1100 1101
		// >> 8
		// 0xCD00 = 0000 0000 1010 1011
		assertEquals(0x000000AB, 0x0000ABCD >> 8);
		assertEquals(0x00000000, 0x0000ABCD >> 16);

		// >> shifts with bits equal to sign bit
		assertEquals(0xFFFFFFFF, 0xFFFFFFFF >> 8);
		assertEquals(0x000000FF, 0x0000FFFF >> 8);

		// >>> shifts with zeros
		assertEquals(0x00FFFFFF, 0xFFFFFFFF >>> 8);
		assertEquals(0x000000FF, 0x0000FFFF >>> 8);

		// 0xABCD = 1010 1011 1100 1101
		// << 8
		// 0xCD00 = 1100 1101 0000 0000
		assertEquals(0x00ABCD00, 0xABCD << 8);
		assertEquals(0xABCD0000, 0xABCD << 16);
	}

	@Test
	public void getTwoIdenticalUUID() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID first = TimeUUID.toUUID(expectedTimestamp);
		UUID second = TimeUUID.toUUID(expectedTimestamp);

		assertEquals(first, second);
		assertEquals(first.toString(), second.toString());
		assertEquals(first.getTime(), second.getTime()); // Same internal time in 100s of ns since epoch.
		assertEquals(TimeUUID.toMillis(first), TimeUUID.toMillis(second));
	}

	@Test
	public void generateTwoDifferentUUIDs() {
		long expectedTimestamp = new DateTime().getMillis();

		UUID first = TimeUUID.createUUID(expectedTimestamp);
		UUID second = TimeUUID.createUUID(expectedTimestamp);

		assertNotSame(first, second);
		assertNotSame(first.toString(), second.toString());
		assertEquals(first.toString().substring(8, 36), second.toString().substring(8, 36)); // Only first 8 chars differ.

		assertEquals(TimeUUID.toMillis(first), TimeUUID.toMillis(second));
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
	public void getTimestampFromUUIDAndConvertTimestampBackToUUID() {
		UUID expectedUuid = TimeUUID.createUUID();

		long timestamp = TimeUUID.toMillis(expectedUuid);
		UUID actualUuid = TimeUUID.toUUID(timestamp);

		assertEquals(expectedUuid, actualUuid);
		assertEquals(expectedUuid.toString(), actualUuid.toString());

		// Reminder: if we "create" a new UUID instead of just converting it back, we get a totally different UUID:
		UUID newUuid = TimeUUID.createUUID(timestamp);
		assertNotSame(expectedUuid, newUuid);
		assertNotSame(expectedUuid.toString(), newUuid.toString());
		assertEquals(expectedUuid.toString().substring(8, 36), newUuid.toString().substring(8, 36)); // Only first 8 chars differ.
		assertEquals(TimeUUID.toMillis(expectedUuid), TimeUUID.toMillis(newUuid));
	}

	@Test
	public void twiceSameTimestampIncrementsTimestampUsedToGenerateUUID() {
		long t0 = new DateTime().getMillis();

		UUID first = TimeUUID.createUUID(t0);
		UUID second = TimeUUID.createUUID(t0);
		UUID third = TimeUUID.createUUID(t0);
		UUID fourth = TimeUUID.createUUID(t0 + 1);

		assertEquals(t0, TimeUUID.toMillis(first));
		assertEquals(t0, TimeUUID.toMillis(second));
		assertEquals(t0, TimeUUID.toMillis(third));
		assertEquals(t0 + 1, TimeUUID.toMillis(fourth));
	}

	@Test
	public void uuidOrderAndUUIDGeneratorIncrementsOnSameTimestamp() {
		long t0 = new DateTime().getMillis();

		UUID first = TimeUUID.createUUID(t0);
		long t1 = TimeUUID.toMillis(first);

		UUID second = TimeUUID.createUUID(t0);
		long t2 = TimeUUID.toMillis(second);

		UUID third = TimeUUID.createUUID(t0 + 1);
		long t3 = TimeUUID.toMillis(third);

		assertNotSame(first, second);
		assertNotSame(first, third);
		assertNotSame(second, third);

		assertEquals(t0, t1);
		assertEquals(t0, t2);
		assertNotSame(t0, t3);
		assertEquals(t0, t3 - 1);

		assertEquals(-1, first.compareTo(second));
		assertEquals(-1, first.compareTo(third));
		assertEquals(-1, second.compareTo(third));

		assertEquals(t1, t2);
		assertTrue(t1 < t3);
		assertTrue(t2 < t3);
	}

	@Test
	public void comparePerformanceToHector() throws Exception {
		// Warm-up: to initialize all static members in all classes:
		java.util.UUID javaUuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis();
		UUIDSerializer.get().toByteBuffer(javaUuid);
		TimeUUIDSerializer.get().toByteBuffer(new UUID(javaUuid.getMostSignificantBits(), javaUuid.getLeastSignificantBits()));
		TimeUUIDSerializer.get().toByteBuffer(TimeUUID.createUUID());

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
				UUID uuid = TimeUUID.createUUID();
				TimeUUIDSerializer.get().toByteBuffer(uuid);
			}
		})).call();
		System.out.println("Candidate (best according to this test): " + elapsedTimeForThisClass / 1000000 + " ms.");
	}

	class RepeatedOperation implements Runnable {
		private final int numOfRuns;
		private final Runnable runnable;

		public RepeatedOperation(final int numOfRuns, final Runnable runnable) {
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
			this.runnable = runnable;
		}

		@Override
		public Long call() throws Exception {
			long begin = System.nanoTime();
			runnable.run();
			long end = System.nanoTime();
			return end - begin;
		}
	}
}
