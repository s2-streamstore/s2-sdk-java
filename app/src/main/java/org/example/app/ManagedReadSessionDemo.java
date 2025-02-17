package org.example.app;

import java.time.Duration;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.channel.ManagedChannelFactory;
import s2.client.StreamClient;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.Batch;
import s2.types.ReadLimit;
import s2.types.ReadSessionRequest;

public class ManagedReadSessionDemo {

  private static final Logger logger =
      LoggerFactory.getLogger(ManagedReadSessionDemo.class.getName());

  public static void main(String[] args) throws Exception {

    final var authToken = System.getenv("S2_AUTH_TOKEN");
    final var basinName = System.getenv("S2_BASIN");
    final var streamName = System.getenv("S2_STREAM");
    if (authToken == null) {
      throw new IllegalStateException("S2_AUTH_TOKEN not set");
    }
    if (basinName == null) {
      throw new IllegalStateException("S2_BASIN not set");
    }
    if (streamName == null) {
      throw new IllegalStateException("S2_STREAM not set");
    }

    var config =
        Config.newBuilder(authToken)
            .withEndpoints(Endpoints.fromEnvironment())
            .withCompression(true)
            .build();

    try (final var executor = new ScheduledThreadPoolExecutor(12);
        final var channel = ManagedChannelFactory.forBasinOrStreamService(config, basinName)) {

      final var streamClient =
          StreamClient.newBuilder(config, basinName, streamName)
              .withExecutor(executor)
              .withChannel(channel)
              .build();

      try (final var managedSession =
          streamClient.managedReadSession(
              ReadSessionRequest.newBuilder().withReadLimit(ReadLimit.count(100_000)).build(),
              1024 * 1024 * 1024 * 5)) {

        AtomicLong receivedBytes = new AtomicLong();
        while (!managedSession.isClosed()) {
          // Poll for up to 1 minute.
          var resp = managedSession.get(Duration.ofSeconds(60));
          resp.ifPresentOrElse(
              elem -> {
                if (elem instanceof Batch batch) {
                  var size = batch.meteredBytes();
                  logger.info(
                      "batch of {} bytes, seqnums {}..={}",
                      size,
                      batch.firstSeqNum(),
                      batch.lastSeqNum());
                  receivedBytes.addAndGet(size);
                } else {
                  logger.info("non batch received: {}", elem);
                }
              },
              () -> {
                logger.info("no batch");
              });
        }
        logger.info("finished, received {} bytes in total", receivedBytes.get());
      }
    }
  }
}
