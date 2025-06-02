package s2.types;

public interface ReadOutput {
  static ReadOutput fromProto(s2.v1alpha.ReadOutput readOutput) {
    switch (readOutput.getOutputCase()) {
      case BATCH:
        return new Batch(SequencedRecordBatch.fromProto(readOutput.getBatch()));
      case NEXT_SEQ_NUM:
        return new NextSeqNum(readOutput.getNextSeqNum());
    }
    throw new IllegalStateException("Unrecognized readOutput case: " + readOutput);
  }
}
