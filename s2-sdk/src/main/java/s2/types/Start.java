package s2.types;

import java.time.Instant;

public abstract class Start {
  public static SeqNum seqNum(long seqNum) {
    return new SeqNum(seqNum);
  }

  public static Timestamp timestamp(Instant instant) {
    return new Timestamp(instant.toEpochMilli());
  }

  public static TailOffset tailOffset(long tailOffset) {
    return new TailOffset(tailOffset);
  }

  public static final class SeqNum extends Start {
    public final long value;

    private SeqNum(long value) {
      this.value = value;
    }
  }

  public static final class Timestamp extends Start {
    public final long value;

    private Timestamp(long value) {
      this.value = value;
    }

    public Instant toInstant() {
      return Instant.ofEpochMilli(value);
    }
  }

  public static final class TailOffset extends Start {
    public final long value;

    private TailOffset(long value) {
      this.value = value;
    }
  }
}
