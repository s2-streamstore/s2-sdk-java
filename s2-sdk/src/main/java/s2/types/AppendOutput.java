package s2.types;

public class AppendOutput {
  public final long startSeqNum;
  public final long endSeqNum;
  public final long nextSeqNum;

  AppendOutput(long startSeqNum, long endSeqNum, long nextSeqNum) {
    this.startSeqNum = startSeqNum;
    this.endSeqNum = endSeqNum;
    this.nextSeqNum = nextSeqNum;
  }

  public static AppendOutput fromProto(s2.v1alpha.AppendOutput appendOutput) {
    return new AppendOutput(
        appendOutput.getStartSeqNum(), appendOutput.getEndSeqNum(), appendOutput.getNextSeqNum());
  }

  @Override
  public String toString() {
    return String.format(
        "AppendOutput[startSeqNum=%s, endSeqNum=%s, nextSeqNum=%s]",
        startSeqNum, endSeqNum, nextSeqNum);
  }
}
