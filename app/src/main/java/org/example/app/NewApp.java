package org.example.app;

import com.google.protobuf.ByteString;
import java.time.temporal.ChronoUnit;
import java.util.List;
import s2.client.AccountClient;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.AppendInput;
import s2.types.AppendRecord;
import s2.types.ListBasinsRequest;
import s2.types.ReadLimit;
import s2.types.ReadRequest;

public class NewApp {
  public static void main(String[] args) throws Exception {
    System.setProperty("logback.debug", "true");
    var endpoint = Endpoints.fromEnvironment();
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(endpoint)
            .withMaxRetries(3)
            .withRequestTimeout(10000, ChronoUnit.MILLIS)
            .build();
    try (var client = new AccountClient(config)) {
      var basins = client.listBasins(ListBasinsRequest.newBuilder().build()).get();
      System.out.println("async: " + basins);

      //      var createBasin =
      //          client
      //
      // .createBasin(CreateBasinRequest.newBuilder().withBasin("hello-test-1").build())
      //              .get();

      var basinClient = client.basinClient("java-test");
      var resp = basinClient.listStreams("").get();
      System.out.println("streams: " + resp);

      var streamClient = basinClient.streamClient("t4");
      var resp2 = streamClient.checkTail().get();
      System.out.println("tail: " + resp2);

      var read1 =
          streamClient
              .read(ReadRequest.newBuilder().withReadLimit(ReadLimit.countOrBytes(5, 3000)).build())
              .get();

//      switch (read1) {
//        case Batch batch -> {
//          System.out.println("batch: " + batch.sequencedRecordBatch());
//        }
//        default -> throw new IllegalStateException("Unexpected value: " + read1);
//      }

      System.out.println("read unary new: " + read1);

//      var rs1 =
//          streamClient.readSession(
//              ReadSessionRequest.newBuilder().withStartSeqNum(10).build(),
//              r -> {
//                switch (r) {
//                  case Batch batch -> {
//                    System.out.println("batch: " + batch.sequencedRecordBatch());
//                  }
//                  case NextSeqNum nextSeqNum -> {}
//
//                  case FirstSeqNum firstSeqNum -> {}
//                }
//              },
//              error -> {
//                System.out.println("error1: " + error);
//              });
//
//      Thread.sleep(1500);
//      System.out.println("attempting to cancel");
//      rs1.close().get();
//      System.out.println("cancelled");
//
      System.out.println("finished with readsession");

      var z =
          AppendRecord.newBuilder()
              .withBytes(ByteString.copyFromUtf8("hi from unary append"))
              .build();
      var zz = AppendInput.newBuilder().withRecords(List.of(z)).build();
      var appendResp = streamClient.append(zz).get();
      System.out.println("append: " + appendResp);

      System.out.println("all done");

      var appendSession =
          streamClient.appendSession(
              out -> {
                System.out.println("session_out: " + out);
              },
              sessionErr -> {
                System.out.println("session_err: " + sessionErr);
              });

      for (var i = 0; i < 10; i++) {
        appendSession.submit(
            AppendInput.newBuilder()
                .withRecords(
                    List.of(
                        AppendRecord.newBuilder()
                            .withBytes(ByteString.copyFromUtf8("session " + i))
                            .build()))
                .build());
        Thread.sleep(1000);
      }
      System.out.println("all done w sending");
      Thread.sleep(5000);
      appendSession.close().get();

      //
      //      var sessionUpstream =
      //          streamClient.appendSession(
      //              resp3 -> {
      //                System.out.println("resp3: " + resp3);
      //              },
      //              e -> {
      //                System.out.println("error3: " + e);
      //              });
      //
      //      for (var i = 0; i < 10; i++) {
      //        sessionUpstream
      //            .nextRequest()
      //            .accept(
      //                AppendSessionRequest.newBuilder()
      //                    .setInput(
      //                        AppendInput.newBuilder()
      //                            .setStream("t3")
      //                            .addRecords(
      //                                AppendRecord.newBuilder()
      //                                    .setBody(ByteString.copyFrom("hello world " + i,
      // "utf-8"))
      //                                    .build())
      //                            .build())
      //                    .build());
      //        Thread.sleep(1000);
      //      }
      //
      //      sessionUpstream.closeUpstream();
      //
      //      sessionUpstream.downstreamFinished().get();

      // System.out.println("Hello World! basins = " + basins);
    }
  }
}
