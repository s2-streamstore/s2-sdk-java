package org.example.app;

import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.Client;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.Age;
import s2.types.BasinConfig;
import s2.types.CreateBasinRequest;
import s2.types.ListBasinsRequest;
import s2.types.StorageClass;
import s2.types.StreamConfig;
import s2.types.Timestamping;
import s2.types.TimestampingMode;

public class AccountDemo {

  private static final Logger logger = LoggerFactory.getLogger(AccountDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var config =
        Config.newBuilder(System.getenv("S2_ACCESS_TOKEN"))
            .withEndpoints(Endpoints.fromEnvironment())
            .build();

    try (var client = Client.newBuilder(config).build()) {

      var basins = client.listBasins(ListBasinsRequest.newBuilder().build()).get();
      basins.elems.forEach(basin -> logger.info("basin={}", basin));

      var newBasin =
          client
              .createBasin(
                  CreateBasinRequest.newBuilder()
                      .withBasin(UUID.randomUUID().toString())
                      .withBasinConfig(
                          new BasinConfig(
                              StreamConfig.newBuilder()
                                  .withRetentionPolicy(new Age(Duration.ofDays(7)))
                                  .withTimestamping(
                                      new Timestamping(TimestampingMode.CLIENT_REQUIRE, true))
                                  .withStorageClass(StorageClass.STANDARD)
                                  .build(),
                              false,
                              false))
                      .build())
              .get();
      logger.info("newBasin={}", newBasin);
    }
  }
}
