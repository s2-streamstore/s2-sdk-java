package s2.channel;

import io.grpc.ManagedChannelBuilder;
import s2.config.Config;

public class ManagedChannelFactory {
  public static AccountChannel forAccountService(Config config) {
    return new AccountChannel(
        ManagedChannelBuilder.forAddress(
                config.endpoints.account.host, config.endpoints.account.port)
            .build());
  }

  public static BasinChannel forBasinOrStreamService(Config config, String basinName) {
    return new BasinChannel(
        ManagedChannelBuilder.forTarget(config.endpoints.basin.toTarget(basinName)).build());
  }

  public static CombinedChannel forCombinedChannel(Config config) {
    if (!config.endpoints.singleEndpoint()) {
      throw new IllegalArgumentException(
          "Combined channel cannot be used when account and basin endpoints differ.");
    }
    return new CombinedChannel(
        ManagedChannelBuilder.forAddress(
                config.endpoints.account.host, config.endpoints.account.port)
            .build());
  }
}
