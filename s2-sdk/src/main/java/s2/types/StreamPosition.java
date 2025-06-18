package s2.types;

public class StreamPosition {
  public final long seqNum;
  public final long timestamp;

  public StreamPosition(long seqNum, long timestamp) {
    if (seqNum < 0) {
      throw new IllegalArgumentException("seqNum must be non-negative, got: " + seqNum);
    }
    if (timestamp < 0) {
      throw new IllegalArgumentException("timestamp must be non-negative, got: " + timestamp);
    }
    this.seqNum = seqNum;
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return String.format("StreamPosition[seqNum=%s, timestamp=%s]", seqNum, timestamp);
  }
}
