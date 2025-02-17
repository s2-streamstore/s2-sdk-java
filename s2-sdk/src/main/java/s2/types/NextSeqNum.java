package s2.types;

public final class NextSeqNum implements ReadOutput {
  public final long value;

  public NextSeqNum(long value) {
    this.value = value;
  }
}
