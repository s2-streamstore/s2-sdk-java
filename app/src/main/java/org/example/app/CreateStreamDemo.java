package org.example.app;

import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import s2.client.AccountClient;
import s2.config.Config;
import s2.config.Endpoints;

public class CreateStreamDemo {

  private static final Logger logger = LoggerFactory.getLogger(CreateStreamDemo.class.getName());

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
    }
  }
}
