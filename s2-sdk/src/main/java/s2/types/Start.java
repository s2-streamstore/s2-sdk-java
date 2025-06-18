package s2.types;

public abstract class Start {
  public static SeqNum seqNum(long seqNum) {
    return new SeqNum(seqNum);
  }

  public static Timestamp timestamp(long timestamp) {
    return new Timestamp(timestamp);
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
  }

  public static final class TailOffset extends Start {
    public final long value;

    private TailOffset(long value) {
      this.value = value;
    }
  }
}
