package s2.types;

public enum TimestampingMode {
  UNKNOWN,
  UNSPECIFIED,
  /// Prefer client-specified timestamp if present otherwise use arrival time.
  CLIENT_PREFER,
  /// Require a client-specified timestamp and reject the append if it is missing.
  CLIENT_REQUIRE,
  /// Use the arrival time and ignore any client-specified timestamp.
  ARRIVAL
}
