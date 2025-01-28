package org.example.app;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.Client;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.Batch;
import s2.types.ReadLimit;
import s2.types.ReadSessionRequest;

public class ManagedReadSessionDemo {

  private static final Logger logger = LoggerFactory.getLogger(ReadSessionDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var endpoint = Endpoints.fromEnvironment();
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(endpoint)
            .withMaxRetries(10)
            .withRetryDelay(Duration.ofSeconds(1))
            .build();
    try (var client = new Client(config)) {

      var streamClient = client.basinClient("java-test").streamClient("t9");

      var buffered =
          streamClient.managedReadSession(
              ReadSessionRequest.newBuilder().withReadLimit(ReadLimit.count(400000)).build(),
              1024 * 1024 * 2000);

      logger.info("starting");
      AtomicLong received = new AtomicLong();
      while (!buffered.isClosed()) {
        var resp = buffered.get(Duration.ofSeconds(60));
        resp.ifPresentOrElse(
            r -> {
              if (r instanceof Batch batch) {
                var size = batch.meteredBytes();
                logger.info("batch {}, {}..={}", size, batch.firstSeqNum(), batch.lastSeqNum());
                received.addAndGet(size);
              } else {
                logger.info("non batch {}", r);
              }
            },
            () -> {
              logger.info("no batch");
            });
      }
      logger.info("finished, received {} bytes", received.get());
    }
  }
}
