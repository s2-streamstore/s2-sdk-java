package s2.channel;

import io.grpc.ManagedChannelBuilder;
import s2.config.Config;

public class ManagedChannelFactory {
  public static Account forAccountService(Config config) {
    return new Account(
        ManagedChannelBuilder.forAddress(
                config.endpoints.account.host, config.endpoints.account.port)
            .build());
  }

  public static Basin forBasinOrStreamService(Config config, String basinName) {
    return new Basin(
        ManagedChannelBuilder.forTarget(config.endpoints.basin.toTarget(basinName)).build());
  }

  public static Combined forCombinedChannel(Config config, String basinName) {
    // TODO verify
    return new Combined(
        ManagedChannelBuilder.forAddress(
                config.endpoints.account.host, config.endpoints.account.port)
            .build());
  }
}
