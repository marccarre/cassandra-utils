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

/*******************************************************************************
 * Copyright (c) 2013 Marc CARRE Licensed under the MIT License, available at
 * http://opensource.org/licenses/MIT Contributors: Marc CARRE - initial API and implementation
 * See also: https://github.com/marccarre/cassandra-utils
 ******************************************************************************/

import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;

import com.eaio.uuid.UUIDGen;

public class TimeUUID {
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	private static final AtomicLong MAX_NS = new AtomicLong(Long.MIN_VALUE);

	/**
	 * Clock sequence generator.
	 */
	private static final Random CLOCK_SEQ_GENERATOR = new Random();
	private static final AtomicLong CLOCK_SEQ_COUNTER = new AtomicLong((long)(CLOCK_SEQ_GENERATOR.nextDouble() * Long.MAX_VALUE));

	/**
	 * Clock sequence and node - the least significant bits.
	 */
	private static long node;
	static {
		// Extract just the node part of clockSequenceAndNode - this will be used if a new clock sequence is required.
		node = UUIDGen.getClockSeqAndNode() & 0xC000FFFFFFFFFFFFL;
	}
	private static AtomicLong clockSequenceAndNode = new AtomicLong(newClockSequenceAndNode());


	/**
	 * Generate a new, unique UUID based on current timestamp.
	 */
	public static UUID createUUID() {
		return createUUID(System.currentTimeMillis());
	}

	/**
	 * Generate a new, unique UUID based on the provided timestamp.
	 *
	 * If more than 10000 UUIDs are requested in a given millisecond then the clock sequence will be incremented to accommodate the overflow. Note that the
	 * number of unique clock sequence numbers is 16383, so this method is limited to generating ~164 million UUIDs per millisecond.
	 *
	 * @param timestamp
	 *            timestamp used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final long timestamp) {

		// Loop trying to generate the new uuid.
		for (;;) {
			// Compare the supplied timestamp with the current max millisecond stamp
			final long max_ns = MAX_NS.incrementAndGet();

			final long ts100Ns = to100Ns(timestamp);
			final long maxMillis = max_ns / 10000;
			final long newMillis = ts100Ns / 10000;

			if (newMillis < maxMillis) {
				// Back-in-time - use the supplied timestamp as is, with a unique clock sequence
				return new UUID(toUUIDTime(ts100Ns), newClockSequenceAndNode());
			} else if (newMillis > maxMillis) {
				// Try to set the max ms - otherwise loop
				if (MAX_NS.compareAndSet(max_ns, ts100Ns)) {
					// Set the max successfully - use the timestamp as is
					return new UUID(toUUIDTime(ts100Ns), clockSequenceAndNode.get());
				}
			} else {
				// Check for overflow of the millisecond timestamp.
				if ((max_ns / 10000) > maxMillis) {
					// Overflow. Set a new clock sequence - note that it does not matter if multiple threads set a new clock sequence.
					final long newClockSequence = newClockSequenceAndNode();
					clockSequenceAndNode.set(newClockSequence);

					// Try to set the max_ns to the millisecond timestamp with no additional increment - otherwise loop.
					if (MAX_NS.compareAndSet(max_ns, ts100Ns)) {
						return new UUID(toUUIDTime(ts100Ns), newClockSequence);
					}
				} else {
					return new UUID(toUUIDTime(max_ns), clockSequenceAndNode.get());
				}
			}
		}
	}

	private static long newClockSequenceAndNode() {
		long nextClockCount = CLOCK_SEQ_COUNTER.incrementAndGet();
		while (nextClockCount < 0) {
			CLOCK_SEQ_COUNTER.set((long)(CLOCK_SEQ_GENERATOR.nextDouble() * Long.MAX_VALUE));
			nextClockCount = CLOCK_SEQ_COUNTER.incrementAndGet();
		}

		return node | ((nextClockCount % 0x3FFF) << 48);
	}

	/**
	 * Generate a new, unique UUID based on the provided date-time.
	 *
	 * @param dateTime
	 *            date-time used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final DateTime dateTime) {
		return createUUID(dateTime.getMillis());
	}

	/**
	 * Generate a new, unique UUID based on the provided date.
	 *
	 * @param javaDate
	 *            date used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final Date javaDate) {
		return createUUID(javaDate.getTime());
	}

	/**
	 * @param timestamp
	 *            timestamp used for the "time" component of the UUID.
	 */
	@Deprecated
	public static UUID toUUID(final long timestamp) {
		return createUUID(timestamp);
	}

	private static long toUUIDTime(final long timestampIn100Ns) {
		// Example:
		// Lowest 16 bits and version 1: 0123 4567 89AB CDEF -> 89AB CDEF 0000 0000 -> 89AB CDEF
		// 0000 1000
		// Middle 32 bits: 0123 4567 89AB CDEF -> 0000 4567 0000 0000 -> 0000 0000 4567 0000 -> 89AB
		// CDEF 4567 1000
		// Highest 16 bits: 0123 4567 89AB CDEF -> 0123 0000 0000 0000 -> 0000 0000 0000 0123 ->
		// 89AB CDEF 4567 1123

		long uuidTime = (timestampIn100Ns << 32) | 0x0000000000001000L;
		uuidTime |= (timestampIn100Ns & 0x0000FFFF00000000L) >>> 16;
		uuidTime |= (timestampIn100Ns & 0xFFFF000000000000L) >>> 48;
		return uuidTime;
	}

	private static long to100Ns(final long timestampInMs) {
		return (timestampInMs * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
	}

	/**
	 * @param dateTime
	 *            date-time used for the "time" component of the UUID.
	 */
	public static UUID toUUID(final DateTime dateTime) {
		return toUUID(dateTime.getMillis());
	}

	/**
	 * @param javaDate
	 *            date used for the "time" component of the UUID.
	 */
	public static UUID toUUID(final Date javaDate) {
		return toUUID(javaDate.getTime());
	}

	/**
	 * Extract the "time" component of the provided UUID.
	 *
	 * @param uuid
	 *            UUID to extract timestamp from.
	 * @return Timestamp in milliseconds.
	 */
	public static long toMillis(final UUID uuid) {
		return fromUUIDTime(uuid.timestamp());
	}

	private static long fromUUIDTime(final long timestampIn100Ns) {
		return from100Ns(timestampIn100Ns);
	}

	private static long from100Ns(long timestampIn100Ns) {
		return (timestampIn100Ns - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

	/**
	 * WARNING: May cause twice the UUID to be generated. Only use this for testing purposes, never in production code.
	 */
	public static synchronized void resetGenerator() {
		MAX_NS.set(Long.MIN_VALUE);
	}
}
