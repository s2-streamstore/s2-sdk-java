package org.example.app;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.Client;
import s2.config.AppendRetryPolicy;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.AppendInput;
import s2.types.AppendOutput;
import s2.types.AppendRecord;

public class ManagedAppendSessionDemo {

  static class RandomASCIIStringGenerator {
    private static final String ASCII_PRINTABLE_CHARACTERS =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";

    private static final Random RANDOM = new Random();

    public static String generateRandomASCIIString(String prefix, int length) {
      if (length < 0) {
        throw new IllegalArgumentException("Length cannot be negative.");
      }

      StringBuilder sb = new StringBuilder(length);
      sb.append(prefix);
      for (int i = 0; i < length - prefix.length(); i++) {
        int index = RANDOM.nextInt(ASCII_PRINTABLE_CHARACTERS.length());
        sb.append(ASCII_PRINTABLE_CHARACTERS.charAt(index));
      }
      return sb.toString();
    }
  }

  private static final Logger logger =
      LoggerFactory.getLogger(ManagedAppendSessionDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var endpoint = Endpoints.fromEnvironment();
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(endpoint)
            .withMaxRetries(10)
            .withRequestTimeout(10000, ChronoUnit.MILLIS)
            .withMaxAppendInflightBytes(1024 * 1024 * 5)
            .withAppendRetryPolicy(AppendRetryPolicy.ALL)
            .build();

    LinkedBlockingQueue<ListenableFuture<AppendOutput>> futs = new LinkedBlockingQueue<>();

    var executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
    var thatsIt =
        Futures.catchingAsync(
            executor.submit(
                () -> {
                  try {
                    while (true) {
                      var output = futs.take().get();
                      if (output == null) {
                        logger.info("consumer closing");
                        break;
                      }
                      logger.info("consumer got: {}", output);
                    }
                  } catch (Exception e) {
                    logger.error("consumer failed", e);
                  }
                }),
            Throwable.class,
            t -> {
              logger.error("it all ended!", t);
              return null;
            },
            executor);

    try (var client = new Client(config)) {

      var streamClient = client.basinClient("java-test").streamClient("t9");

      var futureAppendSession = streamClient.managedAppendSession();

      for (var i = 0; i < 10000; i++) {
        try {
          var payload = RandomASCIIStringGenerator.generateRandomASCIIString(i + " - ", 1024);
          var myFut =
              futureAppendSession.submit(
                  AppendInput.newBuilder()
                      .withRecords(
                          List.of(
                              AppendRecord.newBuilder()
                                  .withBytes(payload.getBytes(StandardCharsets.UTF_8))
                                  .build()))
                      .build(),
                  Duration.ofMillis(60000));

          logger.debug("adding fut for {}", i);
          futs.add(myFut);
        } catch (RuntimeException e) {
          logger.error("producer fatal" + e);
          futs.add(Futures.immediateFailedFuture(e));
          break;
        }
      }

      futs.add(Futures.immediateFuture(null));

      logger.info("starting close");
      try {
        futureAppendSession.closeGracefully().get();
      } catch (Exception e) {
        logger.info("caught exception", e);
      }
      logger.info("finished closing");
    }

    thatsIt.get();
    executor.shutdown();
  }
}
