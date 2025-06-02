package s2.v1alpha.StreamService;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import s2.v1alpha.ReadOutput;
import s2.v1alpha.ReadSessionRequest;
import s2.v1alpha.ReadSessionResponse;
import s2.v1alpha.SequencedRecord;
import s2.v1alpha.SequencedRecordBatch;
import s2.v1alpha.StreamServiceGrpc.StreamServiceImplBase;

public class MockReadSessionStreamService extends StreamServiceImplBase {
  final AtomicInteger calls = new AtomicInteger(0);

  @Override
  public void readSession(
      ReadSessionRequest request, StreamObserver<ReadSessionResponse> responseObserver) {
    System.out.println("MockStreamService.readSession req " + request);

    long startSeqNum = 0;
    switch (request.getStartCase()) {
      case SEQ_NUM:
        startSeqNum = request.getSeqNum();
        break;
      case TIMESTAMP:
      case TAIL_OFFSET:
      case START_NOT_SET:
        startSeqNum = 0;
        break;
    }

    var limit = request.getLimit().getCount();
    if (!(limit > 0)) {
      throw new RuntimeException("count must be set");
    }
    for (var seqNum = startSeqNum; seqNum < startSeqNum + limit; seqNum++) {
      if (calls.getAndIncrement() % 10 == 0) {
        responseObserver.onError(new RuntimeException("Response observer failed"));
        return;
      } else {
        var batch =
            ReadOutput.newBuilder()
                .setBatch(
                    SequencedRecordBatch.newBuilder()
                        .addRecords(
                            SequencedRecord.newBuilder()
                                .setSeqNum(seqNum)
                                .setTimestamp(System.currentTimeMillis())
                                .setBody(ByteString.copyFromUtf8(String.format("fake %s", seqNum)))
                                .build())
                        .build())
                .build();
        responseObserver.onNext(ReadSessionResponse.newBuilder().setOutput(batch).build());
      }
    }

    responseObserver.onCompleted();
  }
}
