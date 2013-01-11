/*******************************************************************************
 * Copyright (c) 2013 Marc CARRE <carre.marc@gmail.com>
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

public class TimeUUID {
	private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	/**
	 * Generate a new, unique UUID based on current timestamp.
	 */
	public static UUID createUUID() {
		return new UUID(); // Calls UUIDGen.createTime(System.currentTimeMillis()) and UUIDGen.getClockSeqAndNode()
	}

	/**
	 * Generate a new, unique UUID based on the provided timestamp.
	 * 
	 * @param timestamp
	 *            timestamp used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final long timestamp) {
		return new com.eaio.uuid.UUID(UUIDGen.createTime(timestamp), UUIDGen.getClockSeqAndNode());
	}

	/**
	 * Generate a new, unique UUID based on the provided date-time.
	 * 
	 * @param dateTime
	 *            date-time used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final DateTime dateTime) {
		return new com.eaio.uuid.UUID(UUIDGen.createTime(dateTime.getMillis()), UUIDGen.getClockSeqAndNode());
	}

	/**
	 * Generate a new, unique UUID based on the provided date.
	 * 
	 * @param javaDate
	 *            date used for the "time" component of the UUID.
	 */
	public static UUID createUUID(final Date javaDate) {
		return new com.eaio.uuid.UUID(UUIDGen.createTime(javaDate.getTime()), UUIDGen.getClockSeqAndNode());
	}

	/**
	 * WARNING: returned UUID is not unique. Get the UUID corresponding to the provided timestamp and the clock sequence, which depends on the IP and MAC
	 * addresses of the current machine, and a random component per process/JVM.
	 * 
	 * @param timestamp
	 *            timestamp used for the "time" component of the UUID.
	 */
	public static UUID toUUID(final long timestamp) {
		return new com.eaio.uuid.UUID(toUUIDTime(timestamp), UUIDGen.getClockSeqAndNode());
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

	private static long toUUIDTime(final long timestampInMs) {
		final long timestampIn100Ns = to100Ns(timestampInMs);

		// Time's lowest 16 bits:
		// 0123 4567 89AB CDEF -> 89AB CDEF 0000 0000
		long uuidTime = timestampIn100Ns << 32;

		// Time's middle 32 bits:
		// 0123 4567 89AB CDEF -> 0000 4567 0000 0000 -> 0000 0000 4567 0000 -> 89AB CDEF 4567 0000
		uuidTime |= (timestampIn100Ns & 0x0000FFFF00000000L) >> 16;

		// Time's highest 16 bits and version 1:
		// 0123 4567 89AB CDEF -> 0000 0000 0000 0123 -> 0000 0000 0000 0123 -> 0000 0000 0000 1123 -> 89AB CDEF 4567 1123
		// or, depending on the sign bit (even if end result is identical):
		// 1123 4567 89AB CDEF -> 1111 1111 1111 1123 -> 0000 0000 0000 0123 -> 0000 0000 0000 1123 -> 89AB CDEF 4567 1123
		uuidTime |= 0x1000 | ((timestampIn100Ns >> 48) & 0x0FFF);
		return uuidTime;
	}

	private static long to100Ns(final long timestampInMs) {
		return (timestampInMs * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
	}

	/**
	 * Extract the "time" component of the provided UUID.
	 * 
	 * @param uuid
	 *            UUID to extract timestamp from.
	 * @return Timestamp in milliseconds.
	 */
	public static long toMillis(final UUID uuid) {
		return fromUUIDTime(uuid.getTime());
	}

	private static long fromUUIDTime(final long uuidTime) {
		// Time's highest 16 bits and version 1:
		// 89AB CDEF 4567 1123 -> 0000 0000 0000 0123 -> 0123 0000 0000 0000
		long timestampIn100Ns = (uuidTime & 0x0000000000000FFFL) << 48;

		// Time's middle 32 bits:
		// 89AB CDEF 4567 1123 -> 1111 89AB CDEF 4567 -> 0000 0000 0000 4567 -> 0123 4567 0000 0000
		// or, depending on the sign bit (even if end result is identical):
		// 79AB CDEF 4567 1123 -> 0000 79AB CDEF 4567 -> 0000 0000 0000 4567 -> 0123 4567 0000 0000
		timestampIn100Ns |= ((uuidTime >> 16) & 0xFFFFL) << 32;

		// Time's lowest 16 bits:
		// 89AB CDEF 4567 1123 -> 0000 0000 89AB CDEF -> 0123 4567 89AB CDEF
		timestampIn100Ns |= uuidTime >>> 32;
		return from100Ns(timestampIn100Ns);
	}

	private static long from100Ns(long timestampIn100Ns) {
		return (timestampIn100Ns - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}
}
