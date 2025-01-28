package org.example.app;

import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.AccountClient;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.Batch;
import s2.types.ReadLimit;
import s2.types.ReadSessionRequest;

public class BufferedReadSessionDemo {

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

      var streamClient = client.basinClient("java-test").streamClient("t10");

      var buffered =
          streamClient.bufferedReadSession(
              ReadSessionRequest.newBuilder().withReadLimit(ReadLimit.count(400000)).build());

      while (!buffered.isClosed()) {
        var resp = buffered.get();
        resp.ifPresentOrElse(
            r -> {
              if (r instanceof Batch batch) {

                logger.info("batch {}", batch.meteredBytes());
              } else {
                logger.info("non batch {}", r);
              }
            },
            () -> {
              return;
            });
        Thread.sleep(1);
      }
      logger.info("finished");
    }
  }
}
