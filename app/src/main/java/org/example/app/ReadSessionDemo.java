package org.example.app;

import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.AccountClient;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.Batch;
import s2.types.FirstSeqNum;
import s2.types.NextSeqNum;
import s2.types.ReadSessionRequest;

public class ReadSessionDemo {

  private static final Logger logger = LoggerFactory.getLogger(ReadSessionDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var endpoint = Endpoints.fromEnvironment();
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(endpoint)
            .withMaxRetries(3)
            .withRequestTimeout(10000, ChronoUnit.MILLIS)
            .build();
    try (var client = new AccountClient(config)) {

      var streamClient = client.basinClient("java-test").streamClient("t4");

      var readSession =
          streamClient.readSession(
              ReadSessionRequest.newBuilder().withStartSeqNum(10).build(),
              output -> {
                if (output instanceof Batch batch) {
                  logger.info("Batch={}", batch);
                } else if (output instanceof NextSeqNum nextSeqNum) {
                  logger.info("NextSeqNum={}", nextSeqNum);
                } else if (output instanceof FirstSeqNum firstSeqNum) {
                  logger.info("FirstSeqNum={}", firstSeqNum);
                }
              },
              err -> logger.error("Error.", err));

      logger.info("Awaiting completion.");
      readSession.awaitCompletion().get();
      logger.info("Finished.");
    }
  }
}
