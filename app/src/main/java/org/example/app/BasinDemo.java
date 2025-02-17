package org.example.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.BasinClient;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.CreateStreamRequest;
import s2.types.ListStreamsRequest;
import s2.types.StreamConfig;

public class BasinDemo {

  private static final Logger logger = LoggerFactory.getLogger(BasinDemo.class.getName());

  public static void main(String[] args) throws Exception {
    final var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(Endpoints.fromEnvironment())
            .build();

    try (final var basinClient =
        BasinClient.newBuilder(config, System.getenv("S2_BASIN")).build()) {
      final var streams = basinClient.listStreams(ListStreamsRequest.newBuilder().build()).get();
      streams
          .elems()
          .forEach(
              stream -> {
                logger.info("stream={}", stream);
              });

      var newStream =
          basinClient
              .createStream(
                  CreateStreamRequest.newBuilder()
                      .withStreamName("test/1")
                      .withStreamConfig(StreamConfig.newBuilder().build())
                      .build())
              .get();
      logger.info("newStream={}", newStream);
    }
  }
}
