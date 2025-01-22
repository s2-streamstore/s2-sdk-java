package s2.v1alpha;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;

public class MockStreamService extends StreamServiceGrpc.StreamServiceImplBase {
  final AtomicInteger calls = new AtomicInteger(0);

  @Override
  public void readSession(
      ReadSessionRequest request, StreamObserver<ReadSessionResponse> responseObserver) {
    System.out.println("MockStreamService.readSession req " + request);

    var startSeqNum = request.getStartSeqNum();
    var limit = request.getLimit().getCount();
    if (!(limit > 0)) {
      throw new RuntimeException("count must be set");
    }
    for (var seqNum = startSeqNum; seqNum < startSeqNum + limit; seqNum++) {
      if (calls.getAndIncrement() % 10 == 0) {
        responseObserver.onError(new RuntimeException("I messed up!"));
        return;
      } else {
        var batch =
            ReadOutput.newBuilder()
                .setBatch(
                    SequencedRecordBatch.newBuilder()
                        .addRecords(
                            SequencedRecord.newBuilder()
                                .setSeqNum(seqNum)
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
