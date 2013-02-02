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

import java.util.Date;
import java.util.UUID;

import org.joda.time.DateTime;

import com.eaio.uuid.UUIDGen;

public final class TimeUUID {
	private TimeUUID() {
		// Pure utility class, do NOT instantiate.
	}

	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
	private static long lastTimestamp = Long.MIN_VALUE;

	/**
	 * WARNING: Use only for testing purposes, as it may lead to duplicate UUIDs. Re-initialize the value of the last timestamp seen.
	 */
	public static synchronized void reset() {
		lastTimestamp = Long.MIN_VALUE;
	}

	/**
	 * Generate a new, unique UUID based on current timestamp.
	 */
	public static UUID createUUID() {
		return createUUID(System.currentTimeMillis());
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
	 * Generate a new, unique UUID based on the provided timestamp.
	 * 
	 * @param timestamp
	 *            timestamp used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final long timestamp) {
		final long timestampIn100Ns = to100Ns(timestamp);
		final long uniqueTimestampIn100Ns = makeUnique(timestampIn100Ns);
		return new UUID(toUUIDTime(uniqueTimestampIn100Ns), UUIDGen.getClockSeqAndNode());
	}

	private static long to100Ns(final long timestampInMs) {
		return (timestampInMs * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
	}

	private static synchronized long makeUnique(final long timestamp) {
		if (timestamp > lastTimestamp) {
			lastTimestamp = timestamp;
			return timestamp;
		} else {
			return ++lastTimestamp;
		}
	}

	private static long toUUIDTime(final long timestampIn100Ns) {
		// Example:
		// Lowest 16 bits and version 1: 0123 4567 89AB CDEF -> 89AB CDEF 0000 0000 -> 89AB CDEF 0000 1000
		// Middle 32 bits: 0123 4567 89AB CDEF -> 0000 4567 0000 0000 -> 0000 0000 4567 0000 -> 89AB CDEF 4567 1000
		// Highest 16 bits: 0123 4567 89AB CDEF -> 0123 0000 0000 0000 -> 0000 0000 0000 0123 -> 89AB CDEF 4567 1123

		long uuidTime = (timestampIn100Ns << 32) | 0x0000000000001000L;
		uuidTime |= (timestampIn100Ns & 0x0000FFFF00000000L) >>> 16;
		uuidTime |= (timestampIn100Ns & 0xFFFF000000000000L) >>> 48;
		return uuidTime;
	}

	/**
	 * WARNING: returned UUID is not unique. Get the UUID corresponding to the provided date-time and the clock sequence, which depends on the IP and MAC
	 * addresses of the current machine, and a random component per process/JVM.
	 * 
	 * @param dateTime
	 *            date-time used for the "time" component of the UUID.
	 */
	public static UUID toUUID(final DateTime dateTime) {
		return toUUID(dateTime.getMillis());
	}

	/**
	 * WARNING: returned UUID is not unique. Get the UUID corresponding to the provided date and the clock sequence, which depends on the IP and MAC addresses
	 * of the current machine, and a random component per process/JVM.
	 * 
	 * @param javaDate
	 *            date used for the "time" component of the UUID.
	 */
	public static UUID toUUID(final Date javaDate) {
		return toUUID(javaDate.getTime());
	}

	/**
	 * WARNING: returned UUID is not unique. Get the UUID corresponding to the provided timestamp and the clock sequence, which depends on the IP and MAC
	 * addresses of the current machine, and a random component per process/JVM.
	 * 
	 * @param timestamp
	 *            timestamp used for the "time" component of the UUID.
	 */
	public static UUID toUUID(final long timestamp) {
		final long timestampIn100Ns = to100Ns(timestamp);
		return new UUID(toUUIDTime(timestampIn100Ns), UUIDGen.getClockSeqAndNode());
	}

	/**
	 * Extract the "time" component of the provided UUID.
	 * 
	 * @param uuid
	 *            UUID to extract timestamp from.
	 * @return Timestamp in milliseconds.
	 */
	public static long toMillis(final UUID uuid) {
		return from100Ns(uuid.timestamp());
	}

	private static long from100Ns(long timestampIn100Ns) {
		return (timestampIn100Ns - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}
}
