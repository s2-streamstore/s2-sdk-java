package org.example.app;

import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.Client;
import s2.config.Config;
import s2.config.Endpoints;
import s2.types.Age;
import s2.types.CreateBasinRequest;
import s2.types.ListBasinsRequest;
import s2.types.StorageClass;
import s2.types.StreamConfig;

public class AccountDemo {

  private static final Logger logger = LoggerFactory.getLogger(AccountDemo.class.getName());

  public static void main(String[] args) throws Exception {
    var config =
        Config.newBuilder(System.getenv("S2_AUTH_TOKEN"))
            .withEndpoints(Endpoints.fromEnvironment())
            .build();

    try (var client = new Client(config)) {

      var basins = client.listBasins(ListBasinsRequest.newBuilder().build()).get();
      basins.elems().forEach(basin -> logger.info("basin={}", basin));

      var newBasin =
          client
              .createBasin(
                  CreateBasinRequest.newBuilder()
                      .withBasin(UUID.randomUUID().toString())
                      .withDefaultStreamConfig(
                          StreamConfig.newBuilder()
                              .withRetentionPolicy(new Age(Duration.ofDays(7)))
                              .withStorageClass(StorageClass.STANDARD)
                              .build())
                      .build())
              .get();
      logger.info("newBasin={}", newBasin);

      client.deleteBasin(newBasin.name()).get();
      logger.info("deleted basin {}", newBasin);
    }
  }
}
