package emulator;

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import s2.v1alpha.AppendOutput;
import s2.v1alpha.AppendRecord;
import s2.v1alpha.AppendRequest;
import s2.v1alpha.AppendResponse;
import s2.v1alpha.ReadOutput;
import s2.v1alpha.ReadSessionRequest;
import s2.v1alpha.ReadSessionResponse;
import s2.v1alpha.SequencedRecord;
import s2.v1alpha.SequencedRecordBatch;
import s2.v1alpha.StreamServiceGrpc;

public class EmulatedStreamService extends StreamServiceGrpc.StreamServiceImplBase {

  final AtomicInteger calls = new AtomicInteger(0);

  private final HashMap<String, List<SequencedRecord>> storage = new HashMap<>();

  private synchronized AppendOutput internalAppend(
      String streamName, List<AppendRecord> appendRecords) {
    storage.putIfAbsent(streamName, new ArrayList<>());
    var l = storage.get(streamName);

    final var startSeqNum = l.size();
    var seqNum = startSeqNum;
    for (AppendRecord appendRecord : appendRecords) {
      var sequenced =
          SequencedRecord.newBuilder().setSeqNum(seqNum).setBody(appendRecord.getBody()).build();
      l.add(sequenced);
      seqNum++;
    }
    var builder = AppendOutput.newBuilder();
    builder.setStartSeqNum(startSeqNum);
    builder.setEndSeqNum(seqNum);
    builder.setNextSeqNum(seqNum);
    return builder.build();
  }

  private synchronized ReadOutput internalRead(String streamName, Long startSeqNum, Long count) {
    var collected = storage.get(streamName).stream().skip(startSeqNum).limit(count);
    var output = ReadOutput.newBuilder();
    var batch = SequencedRecordBatch.newBuilder();

    collected.forEach(batch::addRecords);
    output.setBatch(batch.build());
    return output.build();
  }

  @Override
  public void append(AppendRequest request, StreamObserver<AppendResponse> responseObserver) {
    calls.incrementAndGet();
    var records = request.getInput().getRecordsList();
    var resp = internalAppend(request.getInput().getStream(), records);
    var output = AppendResponse.newBuilder().setOutput(resp).build();
    responseObserver.onNext(output);
    responseObserver.onCompleted();
  }

  @Override
  public void readSession(
      ReadSessionRequest request, StreamObserver<ReadSessionResponse> responseObserver) {
    calls.incrementAndGet();

    var output =
        internalRead(request.getStream(), request.getStartSeqNum(), request.getLimit().getCount());

    responseObserver.onNext(ReadSessionResponse.newBuilder().setOutput(output).build());
    responseObserver.onCompleted();
  }
}
