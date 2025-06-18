package s2.types;

public class AppendOutput {
  public final StreamPosition start;
  public final StreamPosition end;
  public final StreamPosition tail;

  AppendOutput(StreamPosition start, StreamPosition end, StreamPosition tail) {
    this.start = start;
    this.end = end;
    this.tail = tail;
  }

  public static AppendOutput fromProto(s2.v1alpha.AppendOutput appendOutput) {
    return new AppendOutput(
        new StreamPosition(appendOutput.getStartSeqNum(), appendOutput.getStartTimestamp()),
        new StreamPosition(appendOutput.getEndSeqNum(), appendOutput.getEndTimestamp()),
        new StreamPosition(appendOutput.getNextSeqNum(), appendOutput.getLastTimestamp()));
  }

  @Override
  public String toString() {
    return String.format("AppendOutput[start=%s, end=%s, tail=%s]", start, end, tail);
  }
}
