package s2.types;

public sealed interface ReadOutput permits Batch, FirstSeqNum, NextSeqNum {
  static ReadOutput fromProto(s2.v1alpha.ReadOutput readOutput) {
    switch (readOutput.getOutputCase()) {
      case BATCH -> {
        return new Batch(SequencedRecordBatch.fromProto(readOutput.getBatch()));
      }
      case FIRST_SEQ_NUM -> {
        return new FirstSeqNum(readOutput.getFirstSeqNum());
      }
      case NEXT_SEQ_NUM -> {
        return new NextSeqNum(readOutput.getNextSeqNum());
      }
    }
    throw new IllegalStateException("Unrecognized readOutput case: " + readOutput.getOutputCase());
  }
}
