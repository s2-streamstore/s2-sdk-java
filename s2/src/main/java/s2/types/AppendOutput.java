package s2.types;

public record AppendOutput(long startSeqNum, long endSeqNum, long nextSeqNum) {
  public static AppendOutput fromProto(s2.v1alpha.AppendOutput appendOutput) {
    return new AppendOutput(
        appendOutput.getStartSeqNum(), appendOutput.getEndSeqNum(), appendOutput.getNextSeqNum());
  }
}
