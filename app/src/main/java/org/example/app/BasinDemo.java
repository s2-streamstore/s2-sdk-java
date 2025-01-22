package org.example.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.AccountClient;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.CreateStreamRequest;
import s2.types.ListStreamsRequest;
import s2.types.StreamConfig;

public class BasinDemo {

  private static final Logger logger = LoggerFactory.getLogger(BasinDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(Endpoints.fromEnvironment())
            .build();

    try (var client = new AccountClient(config)) {

      var basinClient = client.basinClient("java-test");
      var streams = basinClient.listStreams(ListStreamsRequest.newBuilder().build()).get();
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
                      .withStreamName("test-stream1")
                      .withStreamConfig(StreamConfig.newBuilder().build())
                      .build())
              .get();
      logger.info("newStream={}", newStream);
    }
  }
}
