package s2.types;

import java.util.Optional;

public final class Batch implements ReadOutput, MeteredBytes {

  public final SequencedRecordBatch sequencedRecordBatch;

  Batch(SequencedRecordBatch sequencedRecordBatch) {
    this.sequencedRecordBatch = sequencedRecordBatch;
  }

  public Optional<StreamPosition> firstPosition() {
    return this.sequencedRecordBatch.records.stream()
        .findFirst()
        .map(sr -> new StreamPosition(sr.seqNum, sr.timestamp));
  }

  public Optional<StreamPosition> lastPosition() {
    if (!this.sequencedRecordBatch.records.isEmpty()) {
      var lastRecord =
          this.sequencedRecordBatch.records.get(this.sequencedRecordBatch.records.size() - 1);
      return Optional.of(new StreamPosition(lastRecord.seqNum, lastRecord.timestamp));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public long meteredBytes() {
    return this.sequencedRecordBatch.meteredBytes();
  }
}
