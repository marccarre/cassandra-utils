/*******************************************************************************
 * Copyright (c) 2013 Marc CARRE
 * Licensed under the MIT License, available at http://opensource.org/licenses/MIT
 * 
 * Contributors:
 *     Marc CARRE - initial API and implementation
 ******************************************************************************/
package com.carmatech.cassandra;

import java.util.Date;

import org.joda.time.DateTime;

import com.eaio.uuid.UUID;
import com.eaio.uuid.UUIDGen;

public class CorbaTimeUUID {
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
	private static long LAST_TIMESTAMP = Long.MIN_VALUE;

	/**
	 * WARNING: Use only for testing purposes, as it may lead to duplicate UUIDs. Re-initialize the value of the last timestamp seen.
	 */
	public static synchronized void reset() {
		LAST_TIMESTAMP = Long.MIN_VALUE;
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
		return new com.eaio.uuid.UUID(toUUIDTime(uniqueTimestampIn100Ns), UUIDGen.getClockSeqAndNode());
	}

	private static long to100Ns(final long timestampInMs) {
		return (timestampInMs * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
	}

	private static synchronized long makeUnique(final long timestamp) {
		if (timestamp > LAST_TIMESTAMP) {
			LAST_TIMESTAMP = timestamp;
			return timestamp;
		} else {
			return ++LAST_TIMESTAMP;
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
		return new com.eaio.uuid.UUID(toUUIDTime(timestampIn100Ns), UUIDGen.getClockSeqAndNode());
	}

	/**
	 * Extract the "time" component of the provided UUID.
	 * 
	 * @param uuid
	 *            UUID to extract timestamp from.
	 * @return Timestamp in milliseconds.
	 */
	public static long toMillis(final UUID uuid) {
		return from100Ns(fromUUIDTime(uuid.getTime()));
	}

	private static long from100Ns(long timestampIn100Ns) {
		return (timestampIn100Ns - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

	private static long fromUUIDTime(final long uuidTime) {
		// Example:
		// Lowest 16 bits: 89AB CDEF 4567 1123 -> 0000 0000 89AB CDEF
		// Middle 32 bits: 89AB CDEF 4567 1123 -> 0000 0000 4567 0000 -> 0000 4567 0000 0000 -> 0000 4567 89AB CDEF
		// Highest 16 bits and version 1: 89AB CDEF 4567 1123 -> 0000 0000 0000 0123 -> 0123 0000 0000 0000 -> 0123 4567 89AB CDEF

		long timestampIn100Ns = uuidTime >>> 32;
		timestampIn100Ns |= (uuidTime & 0x00000000FFFF0000L) << 16;
		timestampIn100Ns |= (uuidTime & 0x0000000000000FFFL) << 48;
		return timestampIn100Ns;
	}
}
