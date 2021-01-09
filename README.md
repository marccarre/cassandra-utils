# cassandra-utils

Library of utilities to assist with developing applications based on Apache Cassandra.

Features:
- Sharding frequency for row keys:
  - Calculation of the sharding frequency based on query patterns.
  - Automated generation of row keys from "from" and "to" timestamps.
- Utilities to generate UUIDs and perform conversions from/to timestamps for:
  - Johann Burkard's UUIDs (See also: http://johannburkard.de/blog/programming/java/Java-UUID-generators-compared.html)
  - JDK's UUIDs