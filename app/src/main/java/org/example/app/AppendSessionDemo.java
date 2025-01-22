package org.example.app;

import com.google.protobuf.ByteString;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.AccountClient;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.AppendInput;
import s2.types.AppendRecord;

public class AppendSessionDemo {

  private static final Logger logger = LoggerFactory.getLogger(AppendSessionDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var endpoint = Endpoints.fromEnvironment();
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(endpoint)
            .withMaxRetries(3)
            .withRequestTimeout(10000, ChronoUnit.MILLIS)
            .withAppendRetryPolicy(AppendRetryPolicy.NO_SIDE_EFFECTS)
            .build();

    try (var client = new AccountClient(config)) {

      var streamClient = client.basinClient("java-test").streamClient("t4");

      var appendSession =
          streamClient.appendSession(
              out -> logger.info("Received output={}", out),
              err -> logger.error("Received error.", err));

      for (var i = 0; i < 1000090; i++) {
        logger.info("Sending append session={}", i);
        appendSession.submit(
            AppendInput.newBuilder()
                .withRecords(
                    List.of(
                        AppendRecord.newBuilder()
                            .withBytes(ByteString.copyFromUtf8("session " + i))
                            .build()))
                .build());
      }

      logger.info("Finished sending. Closing session.");
      appendSession.close().get();
      logger.info("Session closed.");
    }
  }
}
